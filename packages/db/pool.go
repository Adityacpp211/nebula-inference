// Package db owns PostgreSQL access: the connection pool, transaction helpers,
// the row-level-security session variable, and the schema-version assertion that
// stops a binary serving against a schema it was not built for.
//
// It is the only package permitted to import pgx
// (docs/repository-structure.md §4).
package db

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adityasatwar321/nebula/packages/config"
)

// Pool is the application's handle on PostgreSQL.
type Pool = pgxpool.Pool

// Querier is the read/write surface shared by a pool, a connection and a
// transaction, so helpers work with any of them.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Open creates and verifies a connection pool.
//
// Session-level settings are applied through RuntimeParams rather than a
// per-checkout hook: they then travel with every connection the pool opens,
// including ones created later to grow the pool, with no chance of a connection
// escaping without them.
func Open(ctx context.Context, cfg config.DatabaseConfig, appName string, logger *slog.Logger) (*Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.URL.Reveal())
	if err != nil {
		// The error from ParseConfig can quote the DSN, which contains a password,
		// so it is deliberately not wrapped in.
		return nil, fmt.Errorf("parsing database URL: invalid connection string")
	}

	poolCfg.MaxConns = cfg.MaxConns
	poolCfg.MinConns = cfg.MinConns
	poolCfg.MaxConnLifetime = cfg.MaxConnLifetime.Duration()
	poolCfg.MaxConnIdleTime = cfg.MaxConnIdleTime.Duration()
	// Jitter avoids every connection in the pool expiring at the same instant and
	// causing a reconnect storm.
	poolCfg.MaxConnLifetimeJitter = cfg.MaxConnLifetime.Duration() / 10
	poolCfg.ConnConfig.ConnectTimeout = cfg.ConnectTimeout.Duration()

	if poolCfg.ConnConfig.RuntimeParams == nil {
		poolCfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	// application_name makes pg_stat_activity readable during an incident.
	poolCfg.ConnConfig.RuntimeParams["application_name"] = appName
	if cfg.StatementTimeout.Duration() > 0 {
		ms := strconv.FormatInt(cfg.StatementTimeout.Duration().Milliseconds(), 10)
		poolCfg.ConnConfig.RuntimeParams["statement_timeout"] = ms
		// A transaction left open by a bug holds locks and blocks vacuum; bound it
		// at twice the statement timeout.
		poolCfg.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] =
			strconv.FormatInt(cfg.StatementTimeout.Duration().Milliseconds()*2, 10)
	}

	if cfg.Role != "" {
		// A startup parameter rather than a SET after connecting: it applies before
		// the first statement, on every connection the pool ever opens.
		poolCfg.ConnConfig.RuntimeParams["role"] = cfg.Role
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("creating connection pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, cfg.ConnectTimeout.Duration())
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connecting to database: %w", err)
	}

	if logger != nil {
		logger.InfoContext(ctx, "database pool ready",
			slog.Int("max_conns", int(cfg.MaxConns)),
			slog.Int("min_conns", int(cfg.MinConns)),
			slog.Duration("statement_timeout", cfg.StatementTimeout.Duration()),
			slog.String("application_name", appName))
	}
	return pool, nil
}

// Stats is a snapshot of pool utilisation, for /healthz and later for metrics.
type Stats struct {
	AcquiredConns    int32         `json:"acquired_conns"`
	IdleConns        int32         `json:"idle_conns"`
	TotalConns       int32         `json:"total_conns"`
	MaxConns         int32         `json:"max_conns"`
	AcquireCount     int64         `json:"acquire_count"`
	AcquireDuration  time.Duration `json:"acquire_duration"`
	EmptyAcquires    int64         `json:"empty_acquire_count"`
	CanceledAcquires int64         `json:"canceled_acquire_count"`
}

// PoolStats snapshots the pool.
func PoolStats(p *Pool) Stats {
	s := p.Stat()
	return Stats{
		AcquiredConns:    s.AcquiredConns(),
		IdleConns:        s.IdleConns(),
		TotalConns:       s.TotalConns(),
		MaxConns:         s.MaxConns(),
		AcquireCount:     s.AcquireCount(),
		AcquireDuration:  s.AcquireDuration(),
		EmptyAcquires:    s.EmptyAcquireCount(),
		CanceledAcquires: s.CanceledAcquireCount(),
	}
}
