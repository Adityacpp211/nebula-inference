package db

import (
	"context"
	"errors"
	"fmt"
)

// SchemaMismatchError reports that the database schema is not the version this
// binary was built against.
type SchemaMismatchError struct {
	Expected int64
	Actual   int64
}

func (e *SchemaMismatchError) Error() string {
	switch {
	case e.Actual == 0:
		return fmt.Sprintf("database has no migrations applied, this build expects schema version %d: run nebula-migrate up", e.Expected)
	case e.Actual < e.Expected:
		return fmt.Sprintf("database schema version %d is older than this build expects (%d): run nebula-migrate up before starting this version", e.Actual, e.Expected)
	default:
		return fmt.Sprintf("database schema version %d is newer than this build expects (%d): this binary is older than the database, roll it forward", e.Actual, e.Expected)
	}
}

// ErrNoSchemaTable reports that schema_migrations does not exist, which means the
// database has never been migrated.
var ErrNoSchemaTable = errors.New("schema_migrations table does not exist")

// SchemaVersion returns the highest applied migration version, or 0 when none
// have been applied.
func SchemaVersion(ctx context.Context, q Querier) (int64, error) {
	var exists bool
	err := q.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM information_schema.tables
		                 WHERE table_schema = current_schema() AND table_name = 'schema_migrations')`).Scan(&exists)
	if err != nil {
		return 0, fmt.Errorf("checking for schema_migrations: %w", err)
	}
	if !exists {
		return 0, ErrNoSchemaTable
	}

	var v *int64
	if err := q.QueryRow(ctx, `SELECT max(version) FROM schema_migrations`).Scan(&v); err != nil {
		return 0, fmt.Errorf("reading schema version: %w", err)
	}
	if v == nil {
		return 0, nil
	}
	return *v, nil
}

// AssertSchemaVersion refuses to continue unless the database is at exactly the
// expected version.
//
// Exactly, not "at least": a newer database means this binary predates the schema
// and may write columns that no longer mean what it thinks, which is worse than
// refusing to start. This is what makes a partial upgrade fail loudly instead of
// corrupting data (docs/data-model.md §10).
func AssertSchemaVersion(ctx context.Context, q Querier, expected int64) error {
	actual, err := SchemaVersion(ctx, q)
	if err != nil {
		if errors.Is(err, ErrNoSchemaTable) {
			return &SchemaMismatchError{Expected: expected, Actual: 0}
		}
		return err
	}
	if actual != expected {
		return &SchemaMismatchError{Expected: expected, Actual: actual}
	}
	return nil
}
