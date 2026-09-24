// Package store holds the control plane's PostgreSQL access.
//
// Queries are hand-written against pgx rather than generated. ADR-0019 commits to
// sqlc, and that remains the intent; the generator could not be installed in the
// Phase 2 build environment, so the queries here are written in the shape sqlc
// produces — one method per query, explicit column lists, typed parameters — so
// the switch is mechanical. TODO(NEB-130) tracks it.
//
// Two rules hold throughout:
//
//   - Every method takes a Querier, so it composes into a caller's transaction.
//     Multi-statement operations that must be atomic are therefore the caller's
//     transaction, not a hidden one.
//   - Every tenant-scoped query filters on org_id even though row-level security
//     would also filter it. Belt and braces: the application filter gives a useful
//     error, and RLS turns a missed filter into an empty result instead of a leak
//     (ADR-0022).
package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/adityasatwar321/nebula/packages/db"
)

// Querier is the read/write surface shared by a pool, a connection and a
// transaction.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Store groups the repositories. It holds the pool so callers can open
// transactions through it, and exposes each repository as a field so handler code
// reads as store.Models.Create(...) rather than a flat namespace of 40 methods.
type Store struct {
	pool *db.Pool

	Organizations *OrganizationRepo
	Users         *UserRepo
	APIKeys       *APIKeyRepo
	RatePolicies  *RateLimitPolicyRepo
	Models        *ModelRepo
	Versions      *ModelVersionRepo
	Deployments   *DeploymentRepo
	Audit         *AuditRepo
}

// New builds a Store over a pool.
func New(pool *db.Pool) *Store {
	return &Store{
		pool:          pool,
		Organizations: &OrganizationRepo{},
		Users:         &UserRepo{},
		APIKeys:       &APIKeyRepo{},
		RatePolicies:  &RateLimitPolicyRepo{},
		Models:        &ModelRepo{},
		Versions:      &ModelVersionRepo{},
		Deployments:   &DeploymentRepo{},
		Audit:         &AuditRepo{},
	}
}

// Pool exposes the underlying pool for health checks and for read paths that do
// not need a transaction.
func (s *Store) Pool() *db.Pool { return s.pool }

// InTx runs fn in a transaction with no tenant scope. For system-level work such
// as creating an organization, where no tenant exists yet.
func (s *Store) InTx(ctx context.Context, fn func(pgx.Tx) error) error {
	return db.InTx(ctx, s.pool, fn)
}

// InTxForOrg runs fn in a transaction scoped to one tenant, with the actor
// recorded so database triggers can attribute the change.
//
// This is the entry point for essentially every request handler. It sets three
// session variables: the tenant that row-level security reads, and the actor that
// the deployment state-transition trigger stamps onto history.
func (s *Store) InTxForOrg(ctx context.Context, orgID uuid.UUID, actor Actor, fn func(pgx.Tx) error) error {
	if orgID == uuid.Nil {
		return errors.New("refusing to open a tenant transaction with a nil organization id")
	}
	return db.InTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := db.SetSessionOrg(ctx, tx, orgID); err != nil {
			return err
		}
		if err := setSessionActor(ctx, tx, actor); err != nil {
			return err
		}
		return fn(tx)
	})
}

// Actor identifies who is making a change, for trigger-written history.
type Actor struct {
	Type string // user | api_key | system
	ID   uuid.UUID
}

// SystemActor is the actor for changes with no human or credential behind them,
// such as the controller reconciling.
var SystemActor = Actor{Type: "system"}

// setSessionActor records the actor for the current transaction.
//
// set_config with is_local=true rather than SET LOCAL, because SET does not accept
// bind parameters and building the statement by concatenation would be an
// injection point.
func setSessionActor(ctx context.Context, q Querier, a Actor) error {
	t := a.Type
	if t == "" {
		t = "system"
	}
	if _, err := q.Exec(ctx, `SELECT set_config('app.current_actor_type', $1, true)`, t); err != nil {
		return fmt.Errorf("setting app.current_actor_type: %w", err)
	}
	id := ""
	if a.ID != uuid.Nil {
		id = a.ID.String()
	}
	if _, err := q.Exec(ctx, `SELECT set_config('app.current_actor', $1, true)`, id); err != nil {
		return fmt.Errorf("setting app.current_actor: %w", err)
	}
	return nil
}

// Sentinel errors. Handlers map these to HTTP statuses, so the mapping lives in
// one place and a repository never needs to know about HTTP.
var (
	// ErrNotFound means no row matched. Also returned for a row in another tenant,
	// so existence in another organization is never disclosed.
	ErrNotFound = errors.New("not found")
	// ErrConflict means a uniqueness or state precondition failed.
	ErrConflict = errors.New("conflict")
	// ErrImmutable means the row may not be changed.
	ErrImmutable = errors.New("immutable")
	// ErrInUse means the row is referenced by something that must be removed first.
	ErrInUse = errors.New("in use")
)

// PostgreSQL error codes this package interprets.
const (
	pgUniqueViolation     = "23505"
	pgForeignKeyViolation = "23503"
	pgCheckViolation      = "23514"
	pgRestrictViolation   = "23001"
)

// classify converts a PostgreSQL error into a sentinel where the mapping is
// unambiguous, preserving the original as the wrapped cause.
//
// The constraint NAME is carried through deliberately: "deployment name already
// exists" is a useful message, and it comes from knowing which unique index fired.
func classify(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return err
	}
	switch pgErr.Code {
	case pgUniqueViolation:
		return fmt.Errorf("%w: %s", ErrConflict, constraintDescription(pgErr.ConstraintName))
	case pgForeignKeyViolation:
		return fmt.Errorf("%w: %s", ErrConflict, constraintDescription(pgErr.ConstraintName))
	case pgRestrictViolation:
		// Raised by the immutability triggers, which put a usable sentence in the
		// message; surfacing it is better than inventing one.
		return fmt.Errorf("%w: %s", ErrImmutable, pgErr.Message)
	case pgCheckViolation:
		return fmt.Errorf("%w: %s", ErrConflict, constraintDescription(pgErr.ConstraintName))
	}
	return err
}

// constraintDescription turns a constraint name into something a caller can act
// on. Unknown constraints fall back to the name, which is still more useful than
// a generic message and is greppable in the schema.
func constraintDescription(name string) string {
	switch name {
	case "uq_organizations__slug":
		return "an organization with that slug already exists"
	case "uq_users__email":
		return "a user with that email already exists"
	case "uq_api_keys__prefix":
		return "generated key prefix collided; retry"
	case "uq_models__org_name":
		return "a model with that name already exists in this organization"
	case "uq_model_versions__model_version":
		return "that version already exists for this model"
	case "uq_deployments__org_name":
		return "a deployment with that name already exists in this organization"
	case "uq_deployment_revisions__dep_rev":
		return "that revision number already exists for this deployment"
	case "ck_deployments__replica_bounds":
		return "replica counts must satisfy min <= desired <= max"
	case "ck_deployments__delete_only_when_resting":
		return "a deployment must be stopped before it can be deleted"
	case "ck_model_versions__ready_fields":
		return "a ready model version must record when it became ready"
	case "":
		return "constraint violation"
	default:
		return name
	}
}
