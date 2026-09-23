package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// SessionOrgKey is the PostgreSQL session variable row-level security policies
// read. Changing it means changing every policy, so it lives here as the one
// definition (ADR-0022).
const SessionOrgKey = "app.current_org"

// InTx runs fn inside a transaction, committing on success and rolling back on
// error or panic.
//
// The rollback is deferred rather than written at each return path: a missed
// rollback leaks a connection and holds locks until the idle-in-transaction
// timeout fires, which is the kind of bug that only shows up under load.
func InTx(ctx context.Context, pool *Pool, fn func(pgx.Tx) error) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	committed := false
	defer func() {
		if committed {
			return
		}
		// context.WithoutCancel so the rollback still runs when the request
		// context is already cancelled — otherwise a cancelled request leaves the
		// transaction open until the server times it out.
		if rbErr := tx.Rollback(context.WithoutCancel(ctx)); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			// Nothing useful can be returned from a deferred function here; the
			// caller already has the original error, which matters more.
			_ = rbErr
		}
	}()

	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	committed = true
	return nil
}

// InTxForOrg runs fn inside a transaction scoped to one tenant.
//
// It sets the session variable that row-level security reads, so a query inside
// fn that forgets its org_id filter returns an empty result instead of another
// tenant's rows. This is the second of the two enforcement layers described in
// ADR-0022; application-level scoping is still required.
func InTxForOrg(ctx context.Context, pool *Pool, orgID uuid.UUID, fn func(pgx.Tx) error) error {
	if orgID == uuid.Nil {
		return errors.New("refusing to open a tenant transaction with a nil organization id")
	}
	return InTx(ctx, pool, func(tx pgx.Tx) error {
		if err := SetSessionOrg(ctx, tx, orgID); err != nil {
			return err
		}
		return fn(tx)
	})
}

// SetSessionOrg sets the tenant for the current transaction.
//
// set_config with is_local=true is used rather than SET LOCAL because SET does
// not accept bind parameters, and building the statement by string concatenation
// would be an injection point at the centre of the tenant-isolation mechanism.
func SetSessionOrg(ctx context.Context, q Querier, orgID uuid.UUID) error {
	_, err := q.Exec(ctx, `SELECT set_config($1, $2, true)`, SessionOrgKey, orgID.String())
	if err != nil {
		return fmt.Errorf("setting %s: %w", SessionOrgKey, err)
	}
	return nil
}

// CurrentSessionOrg reads back the tenant of the current transaction. Used by
// tests and by diagnostics; application code should not need it.
func CurrentSessionOrg(ctx context.Context, q Querier) (string, error) {
	var v string
	if err := q.QueryRow(ctx, `SELECT coalesce(current_setting($1, true), '')`, SessionOrgKey).Scan(&v); err != nil {
		return "", fmt.Errorf("reading %s: %w", SessionOrgKey, err)
	}
	return v, nil
}

// NewID mints a UUIDv7 for a new row.
//
// Identifiers are generated in the application, not the database: the ID then
// exists before the insert, so it can be logged and traced without a round trip,
// and multi-table inserts need no RETURNING clauses (ADR-0023).
func NewID() (uuid.UUID, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.Nil, fmt.Errorf("generating identifier: %w", err)
	}
	return id, nil
}

// MustNewID is NewID for contexts that cannot return an error, such as test
// fixtures. It panics only if the system entropy source fails.
func MustNewID() uuid.UUID {
	id, err := NewID()
	if err != nil {
		panic(err)
	}
	return id
}
