// Package dbtest provides the PostgreSQL harness NEBULA's integration tests share.
//
// It is an ordinary package rather than test-only files because more than one test
// package needs it: the schema tests live in tests/integration, and the control
// plane's own integration tests must live under services/controlplane so they can
// import its internal packages. Duplicating the harness in both places would mean
// two subtly different notions of "a migrated database", which is exactly the kind
// of divergence that makes one suite pass while the other fails.
//
// Nothing outside a test imports this package, and it links only the standard
// library, pgx and NEBULA's own packages.
package dbtest

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adityasatwar321/nebula/migrations"
	"github.com/adityasatwar321/nebula/packages/config"
	"github.com/adityasatwar321/nebula/packages/db"
	"github.com/adityasatwar321/nebula/packages/db/migrate"
	"github.com/adityasatwar321/nebula/packages/telemetry"
)

// dbSeq numbers the databases this process creates.
var dbSeq atomic.Int64

// EnvDatabaseURL names the environment variable holding the test database URL.
//
// Absent, integration tests skip rather than fail: `go test ./...` on a laptop with
// no database must still pass, or nobody runs it.
const EnvDatabaseURL = "NEBULA_TEST_DATABASE_URL"

// BaseURL returns the configured administrative connection string, or "".
func BaseURL() string { return os.Getenv(EnvDatabaseURL) }

// Require skips the test when no database is configured.
func Require(t *testing.T) string {
	t.Helper()
	url := BaseURL()
	if url == "" {
		t.Skipf("%s is not set; skipping integration test", EnvDatabaseURL)
	}
	return url
}

// New creates an empty, uniquely named database and returns a pool connected to it.
// The database is dropped when the test finishes.
//
// A database per test rather than a shared one with cleanup between cases: shared
// state makes a failure in one test show up as a failure in another, and the
// migrations themselves are part of what is under test.
func New(t *testing.T) (pool *pgxpool.Pool, url string) {
	t.Helper()
	baseURL := Require(t)

	ctx := context.Background()
	admin, err := pgxpool.New(ctx, baseURL)
	if err != nil {
		t.Fatalf("connecting to the administrative database: %v", err)
	}
	defer admin.Close()

	// A per-process counter, not the clock, makes the name unique: parallel tests
	// on a platform with a coarse timer (Windows) read the same nanosecond value
	// and collided on CREATE DATABASE. The pid keeps concurrent test binaries apart.
	name := fmt.Sprintf("nebula_it_%d_%d", os.Getpid(), dbSeq.Add(1))
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE DATABASE %q`, name)); err != nil {
		t.Fatalf("creating test database %s: %v", name, err)
	}

	url, err = ReplaceDBName(baseURL, name)
	if err != nil {
		t.Fatalf("building test database URL: %v", err)
	}

	opened, err := db.Open(ctx, Config(url), "nebula-integration-test", telemetry.Discard())
	if err != nil {
		t.Fatalf("connecting to test database %s: %v", name, err)
	}
	pool = opened

	t.Cleanup(func() {
		pool.Close()
		cleanup, err := pgxpool.New(context.Background(), baseURL)
		if err != nil {
			t.Logf("could not reconnect to drop %s: %v", name, err)
			return
		}
		defer cleanup.Close()
		if _, err := cleanup.Exec(context.Background(),
			fmt.Sprintf(`DROP DATABASE IF EXISTS %q WITH (FORCE)`, name)); err != nil {
			t.Logf("could not drop test database %s: %v", name, err)
		}
	})

	return pool, url
}

// Migrated returns a pool over a database with every migration applied.
func Migrated(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, _ := New(t)
	if _, err := Runner(t, pool).Up(context.Background()); err != nil {
		t.Fatalf("applying migrations: %v", err)
	}
	return pool
}

// Runner builds a migration runner over the embedded migrations.
func Runner(t *testing.T, pool *pgxpool.Pool) *migrate.Runner {
	t.Helper()
	loaded, err := migrate.Load(migrations.FS)
	if err != nil {
		t.Fatalf("loading embedded migrations: %v", err)
	}
	return migrate.New(pool, loaded, telemetry.Discard())
}

// Config is the pool configuration integration tests use.
func Config(url string) config.DatabaseConfig {
	return config.DatabaseConfig{
		URL:              config.Secret(url),
		MaxConns:         6,
		MinConns:         1,
		MaxConnLifetime:  config.Duration(time.Hour),
		MaxConnIdleTime:  config.Duration(10 * time.Minute),
		ConnectTimeout:   config.Duration(10 * time.Second),
		StatementTimeout: config.Duration(30 * time.Second),
	}
}

// ReplaceDBName rewrites the database name in a postgres:// URL.
func ReplaceDBName(url, name string) (string, error) {
	i := strings.LastIndex(url, "/")
	if i < 0 {
		return "", fmt.Errorf("cannot find the database name in the connection URL")
	}
	rest := ""
	if q := strings.Index(url[i:], "?"); q >= 0 {
		rest = url[i+q:]
	}
	return url[:i+1] + name + rest, nil
}

// Context returns a test context with a bounded lifetime, so a hung query fails the
// test instead of the whole run.
func Context(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}
