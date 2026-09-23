// Package integration_test exercises NEBULA against a real PostgreSQL instance.
//
// These tests are skipped unless NEBULA_TEST_DATABASE_URL is set, so `go test
// ./...` on a laptop with no database still passes. CI supplies a PostgreSQL
// service container (.github/workflows/ci.yml).
//
// What is verified here cannot be verified any other way: a constraint, a
// trigger or a row-level-security policy that has never run against a real
// PostgreSQL is an intention, not a guarantee.
package integration_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adityasatwar321/nebula/migrations"
	"github.com/adityasatwar321/nebula/packages/config"
	"github.com/adityasatwar321/nebula/packages/db"
	"github.com/adityasatwar321/nebula/packages/db/migrate"
	"github.com/adityasatwar321/nebula/packages/telemetry"
)

// EnvDatabaseURL names the environment variable holding the test database URL.
const EnvDatabaseURL = "NEBULA_TEST_DATABASE_URL"

// baseURL is the connection string for the administrative database, used to
// create and drop per-test databases.
var baseURL string

func TestMain(m *testing.M) {
	baseURL = os.Getenv(EnvDatabaseURL)
	os.Exit(m.Run())
}

// requireDB skips the test when no database is configured.
func requireDB(t *testing.T) {
	t.Helper()
	if baseURL == "" {
		t.Skipf("%s is not set; skipping integration test", EnvDatabaseURL)
	}
}

// replaceDBName rewrites the database name in a postgres:// URL.
func replaceDBName(url, name string) (string, error) {
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

// newDatabase creates an empty, uniquely named database and returns a pool
// connected to it. The database is dropped when the test finishes.
func newDatabase(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	requireDB(t)

	ctx := context.Background()
	admin, err := pgxpool.New(ctx, baseURL)
	if err != nil {
		t.Fatalf("connecting to the administrative database: %v", err)
	}
	defer admin.Close()

	name := fmt.Sprintf("nebula_it_%d_%d", os.Getpid(), time.Now().UnixNano()%1_000_000)
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE DATABASE %q`, name)); err != nil {
		t.Fatalf("creating test database %s: %v", name, err)
	}

	url, err := replaceDBName(baseURL, name)
	if err != nil {
		t.Fatalf("building test database URL: %v", err)
	}

	pool, err := db.Open(ctx, testDBConfig(url), "nebula-integration-test", telemetry.Discard())
	if err != nil {
		t.Fatalf("connecting to test database %s: %v", name, err)
	}

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

func testDBConfig(url string) config.DatabaseConfig {
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

// migratedDatabase returns a pool over a database with every migration applied.
func migratedDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, _ := newDatabase(t)
	runner := newRunner(t, pool)
	if _, err := runner.Up(context.Background()); err != nil {
		t.Fatalf("applying migrations: %v", err)
	}
	return pool
}

func newRunner(t *testing.T, pool *pgxpool.Pool) *migrate.Runner {
	t.Helper()
	loaded, err := migrate.Load(migrations.FS)
	if err != nil {
		t.Fatalf("loading embedded migrations: %v", err)
	}
	return migrate.New(pool, loaded, telemetry.Discard())
}

func ctxT(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}
