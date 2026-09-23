// Package migrate applies embedded SQL migrations to PostgreSQL.
//
// Why this is in-repo rather than a migration library: see
// docs/architecture-decisions/0025-own-the-migration-runner.md. In short, we need
// one database and one source, plus two behaviours the common libraries do not
// offer — checksum drift detection on already-applied migrations, and an explicit
// per-file opt out of the wrapping transaction for statements PostgreSQL forbids
// inside one.
package migrate

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// advisoryLockKey serialises migration runs across processes. Any fixed value
// works as long as every NEBULA binary uses the same one.
const advisoryLockKey int64 = 4216729318547921

// noTxDirective opts a migration out of the wrapping transaction. A file carrying
// it must contain exactly one statement, because PostgreSQL wraps a multi-statement
// simple query in an implicit transaction regardless of what we do.
const noTxDirective = "-- nebula:no-transaction"

var fileRE = regexp.MustCompile(`^(\d{6})_([a-z0-9_]+)\.(up|down)\.sql$`)

// Migration is one versioned schema change.
type Migration struct {
	Version int64
	Name    string
	Up      string
	Down    string
	// UpChecksum is the SHA-256 of Up, recorded when applied and re-verified on
	// every subsequent run so an edited migration is caught rather than ignored.
	UpChecksum []byte
	// NoTransaction is true when Up carries the no-transaction directive.
	NoTransaction bool
}

// ErrDirty reports that a previous run failed partway. The operator must decide
// what happened; guessing would risk applying half a migration twice.
var ErrDirty = errors.New("migration state is dirty")

// ErrChecksumMismatch reports that an applied migration's file has changed.
var ErrChecksumMismatch = errors.New("applied migration has been modified")

// Load reads migrations from a filesystem, typically an embed.FS.
//
// Every version must have both an .up.sql and a .down.sql. Requiring a down
// migration is a deliberate constraint: it forces the author to think about
// reversibility, and it lets local development rewind (docs/data-model.md §10).
func Load(fsys fs.FS) ([]Migration, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("reading migrations directory: %w", err)
	}

	type pair struct {
		name string
		up   string
		down string
		hasUp,
		hasDown bool
	}
	byVersion := map[int64]*pair{}

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		m := fileRE.FindStringSubmatch(e.Name())
		if m == nil {
			return nil, fmt.Errorf("migration file %q does not match NNNNNN_name.(up|down).sql", e.Name())
		}
		v, err := strconv.ParseInt(m[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("migration file %q: bad version: %w", e.Name(), err)
		}
		if v == 0 {
			return nil, fmt.Errorf("migration file %q: version 0 is reserved", e.Name())
		}
		body, err := fs.ReadFile(fsys, e.Name())
		if err != nil {
			return nil, fmt.Errorf("reading %q: %w", e.Name(), err)
		}

		p := byVersion[v]
		if p == nil {
			p = &pair{name: m[2]}
			byVersion[v] = p
		}
		if p.name != m[2] {
			return nil, fmt.Errorf("version %06d has conflicting names %q and %q", v, p.name, m[2])
		}
		switch m[3] {
		case "up":
			if p.hasUp {
				return nil, fmt.Errorf("version %06d has more than one up migration", v)
			}
			p.up, p.hasUp = string(body), true
		case "down":
			if p.hasDown {
				return nil, fmt.Errorf("version %06d has more than one down migration", v)
			}
			p.down, p.hasDown = string(body), true
		}
	}

	out := make([]Migration, 0, len(byVersion))
	for v, p := range byVersion {
		if !p.hasUp {
			return nil, fmt.Errorf("version %06d (%s) has no up migration", v, p.name)
		}
		if !p.hasDown {
			return nil, fmt.Errorf("version %06d (%s) has no down migration", v, p.name)
		}
		sum := sha256.Sum256([]byte(p.up))
		out = append(out, Migration{
			Version:       v,
			Name:          p.name,
			Up:            p.up,
			Down:          p.down,
			UpChecksum:    sum[:],
			NoTransaction: strings.Contains(p.up, noTxDirective),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out, nil
}

// Runner applies migrations.
type Runner struct {
	pool       *pgxpool.Pool
	migrations []Migration
	logger     *slog.Logger
}

// New creates a Runner over an ordered migration set.
func New(pool *pgxpool.Pool, migrations []Migration, logger *slog.Logger) *Runner {
	return &Runner{pool: pool, migrations: migrations, logger: logger}
}

// Target is the highest embedded version: the schema version this binary expects.
func (r *Runner) Target() int64 {
	if len(r.migrations) == 0 {
		return 0
	}
	return r.migrations[len(r.migrations)-1].Version
}

// AppliedRecord is one row of schema_migrations.
type AppliedRecord struct {
	Version     int64
	Name        string
	Checksum    []byte
	AppliedAt   time.Time
	ExecutionMS int32
}

// Status is the per-migration view for `nebula-migrate status`.
type Status struct {
	Version   int64
	Name      string
	Applied   bool
	AppliedAt time.Time
	Drifted   bool
}

const createTableSQL = `
CREATE TABLE IF NOT EXISTS schema_migrations (
    version      bigint      PRIMARY KEY,
    name         text        NOT NULL,
    checksum     bytea       NOT NULL,
    applied_at   timestamptz NOT NULL DEFAULT now(),
    execution_ms integer     NOT NULL
)`

// ensureTable creates the bookkeeping table if absent.
func ensureTable(ctx context.Context, q execer) error {
	if _, err := q.Exec(ctx, createTableSQL); err != nil {
		return fmt.Errorf("creating schema_migrations: %w", err)
	}
	return nil
}

// execer is the subset of pgx used here, so a pool, a connection and a
// transaction are interchangeable.
type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func applied(ctx context.Context, q execer) (map[int64]AppliedRecord, error) {
	rows, err := q.Query(ctx, `SELECT version, name, checksum, applied_at, execution_ms
	                             FROM schema_migrations ORDER BY version`)
	if err != nil {
		return nil, fmt.Errorf("reading schema_migrations: %w", err)
	}
	defer rows.Close()

	out := map[int64]AppliedRecord{}
	for rows.Next() {
		var a AppliedRecord
		if err := rows.Scan(&a.Version, &a.Name, &a.Checksum, &a.AppliedAt, &a.ExecutionMS); err != nil {
			return nil, fmt.Errorf("scanning schema_migrations: %w", err)
		}
		out[a.Version] = a
	}
	return out, rows.Err()
}

// withLock runs fn while holding the migration advisory lock on a single
// connection, so two processes starting at once cannot both migrate.
func (r *Runner) withLock(ctx context.Context, fn func(context.Context, *pgxpool.Conn) error) error {
	conn, err := r.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquiring connection: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, advisoryLockKey); err != nil {
		return fmt.Errorf("acquiring migration lock: %w", err)
	}
	defer func() {
		// Best effort: the lock is released with the session in any case.
		if _, err := conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, advisoryLockKey); err != nil && r.logger != nil {
			r.logger.Warn("releasing migration lock", slog.String("error", err.Error()))
		}
	}()

	return fn(ctx, conn)
}

// Up applies every pending migration in version order and returns the versions
// applied.
func (r *Runner) Up(ctx context.Context) ([]int64, error) {
	var appliedNow []int64
	err := r.withLock(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		if err := ensureTable(ctx, conn); err != nil {
			return err
		}
		have, err := applied(ctx, conn)
		if err != nil {
			return err
		}
		if err := r.verify(have); err != nil {
			return err
		}

		for _, m := range r.migrations {
			if _, ok := have[m.Version]; ok {
				continue
			}
			start := time.Now()
			if err := r.applyOne(ctx, conn, m); err != nil {
				return fmt.Errorf("applying %06d_%s: %w", m.Version, m.Name, err)
			}
			took := time.Since(start)
			appliedNow = append(appliedNow, m.Version)
			if r.logger != nil {
				// Deliberately NOT "version"/"name": the service logger already
				// binds "version" to the build version, and a duplicate JSON key
				// is resolved unpredictably by log processors.
				r.logger.Info("migration applied",
					slog.Int64("migration_version", m.Version),
					slog.String("migration_name", m.Name),
					slog.Int64("duration_ms", took.Milliseconds()))
			}
		}
		return nil
	})
	return appliedNow, err
}

// applyOne runs a single up migration and records it.
func (r *Runner) applyOne(ctx context.Context, conn *pgxpool.Conn, m Migration) error {
	start := time.Now()

	if m.NoTransaction {
		// Outside a transaction: for statements PostgreSQL refuses inside one,
		// such as CREATE INDEX CONCURRENTLY. The bookkeeping insert is a separate
		// statement, so a crash between the two leaves the migration applied but
		// unrecorded — which the checksum check will surface on the next run as a
		// pending migration that fails loudly rather than silently half-applying.
		if _, err := conn.Exec(ctx, m.Up); err != nil {
			return err
		}
		return record(ctx, conn, m, time.Since(start))
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after a successful commit

	if _, err := tx.Exec(ctx, m.Up); err != nil {
		return err
	}
	if err := record(ctx, tx, m, time.Since(start)); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

func record(ctx context.Context, q execer, m Migration, took time.Duration) error {
	_, err := q.Exec(ctx,
		`INSERT INTO schema_migrations (version, name, checksum, execution_ms) VALUES ($1, $2, $3, $4)`,
		m.Version, m.Name, m.UpChecksum, int32(took.Milliseconds()))
	if err != nil {
		return fmt.Errorf("recording migration: %w", err)
	}
	return nil
}

// Down reverts the last steps applied migrations, newest first.
func (r *Runner) Down(ctx context.Context, steps int) ([]int64, error) {
	if steps <= 0 {
		return nil, errors.New("steps must be at least 1")
	}
	var reverted []int64
	err := r.withLock(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		if err := ensureTable(ctx, conn); err != nil {
			return err
		}
		have, err := applied(ctx, conn)
		if err != nil {
			return err
		}

		// Walk embedded migrations newest-first, reverting those recorded as applied.
		for i := len(r.migrations) - 1; i >= 0 && len(reverted) < steps; i-- {
			m := r.migrations[i]
			if _, ok := have[m.Version]; !ok {
				continue
			}
			tx, err := conn.Begin(ctx)
			if err != nil {
				return fmt.Errorf("begin: %w", err)
			}
			if _, err := tx.Exec(ctx, m.Down); err != nil {
				_ = tx.Rollback(ctx)
				return fmt.Errorf("reverting %06d_%s: %w", m.Version, m.Name, err)
			}
			if _, err := tx.Exec(ctx, `DELETE FROM schema_migrations WHERE version = $1`, m.Version); err != nil {
				_ = tx.Rollback(ctx)
				return fmt.Errorf("deleting migration record %d: %w", m.Version, err)
			}
			if err := tx.Commit(ctx); err != nil {
				return fmt.Errorf("commit: %w", err)
			}
			reverted = append(reverted, m.Version)
			if r.logger != nil {
				r.logger.Info("migration reverted",
					slog.Int64("migration_version", m.Version), slog.String("migration_name", m.Name))
			}
		}
		return nil
	})
	return reverted, err
}

// Version returns the highest applied version, and whether any migration is
// applied at all.
func (r *Runner) Version(ctx context.Context) (int64, bool, error) {
	if err := ensureTable(ctx, r.pool); err != nil {
		return 0, false, err
	}
	var v *int64
	if err := r.pool.QueryRow(ctx, `SELECT max(version) FROM schema_migrations`).Scan(&v); err != nil {
		return 0, false, fmt.Errorf("reading current version: %w", err)
	}
	if v == nil {
		return 0, false, nil
	}
	return *v, true, nil
}

// Status reports every embedded migration and whether it is applied or drifted.
func (r *Runner) Status(ctx context.Context) ([]Status, error) {
	if err := ensureTable(ctx, r.pool); err != nil {
		return nil, err
	}
	have, err := applied(ctx, r.pool)
	if err != nil {
		return nil, err
	}
	out := make([]Status, 0, len(r.migrations))
	for _, m := range r.migrations {
		s := Status{Version: m.Version, Name: m.Name}
		if a, ok := have[m.Version]; ok {
			s.Applied = true
			s.AppliedAt = a.AppliedAt
			s.Drifted = string(a.Checksum) != string(m.UpChecksum)
		}
		out = append(out, s)
	}
	return out, nil
}

// Validate checks the database against the embedded migration set without
// changing anything: every applied migration must still exist with the same
// content, and no applied version may be unknown to this binary.
func (r *Runner) Validate(ctx context.Context) error {
	if err := ensureTable(ctx, r.pool); err != nil {
		return err
	}
	have, err := applied(ctx, r.pool)
	if err != nil {
		return err
	}
	return r.verify(have)
}

// verify compares recorded checksums against the embedded files.
//
// This catches the mistake that silently corrupts environments: editing a
// migration that has already run. The edited file applies to fresh databases but
// not to existing ones, so two environments diverge with no error anywhere.
func (r *Runner) verify(have map[int64]AppliedRecord) error {
	known := map[int64]Migration{}
	for _, m := range r.migrations {
		known[m.Version] = m
	}
	var problems []string

	for v, a := range have {
		m, ok := known[v]
		if !ok {
			problems = append(problems, fmt.Sprintf(
				"version %06d (%s) is applied in the database but unknown to this binary: "+
					"the database is newer than this build", v, a.Name))
			continue
		}
		if string(a.Checksum) != string(m.UpChecksum) {
			problems = append(problems, fmt.Sprintf(
				"version %06d (%s) was modified after it was applied: edit a new migration instead",
				v, m.Name))
		}
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		return fmt.Errorf("%w:\n  - %s", ErrChecksumMismatch, strings.Join(problems, "\n  - "))
	}
	return nil
}
