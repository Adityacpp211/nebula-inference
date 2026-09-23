// Package integration_test exercises NEBULA's schema against a real PostgreSQL.
//
// These tests are skipped unless NEBULA_TEST_DATABASE_URL is set, so `go test
// ./...` on a laptop with no database still passes. CI supplies a PostgreSQL
// service container (.github/workflows/ci.yml).
//
// What is verified here cannot be verified any other way: a constraint, a trigger or
// a row-level-security policy that has never run against a real PostgreSQL is an
// intention, not a guarantee.
//
// The control plane's own integration tests live in services/controlplane/tests,
// because Go's internal-package rule puts its handlers out of reach from here. Both
// suites share the harness in packages/testsupport/dbtest.
package integration_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adityasatwar321/nebula/packages/config"
	"github.com/adityasatwar321/nebula/packages/db/migrate"
	"github.com/adityasatwar321/nebula/packages/testsupport/dbtest"
)

// EnvDatabaseURL names the environment variable holding the test database URL.
const EnvDatabaseURL = dbtest.EnvDatabaseURL

// baseURL is the connection string for the administrative database.
var baseURL string

func TestMain(m *testing.M) {
	baseURL = dbtest.BaseURL()
	os.Exit(m.Run())
}

// The helpers below delegate to dbtest, keeping the existing tests in this package
// readable while there is only one implementation of the harness.

func newDatabase(t *testing.T) (pool *pgxpool.Pool, url string) {
	t.Helper()
	return dbtest.New(t)
}

func migratedDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	return dbtest.Migrated(t)
}

func newRunner(t *testing.T, pool *pgxpool.Pool) *migrate.Runner {
	t.Helper()
	return dbtest.Runner(t, pool)
}

func ctxT(t *testing.T) context.Context {
	t.Helper()
	return dbtest.Context(t)
}

func testDBConfig(url string) config.DatabaseConfig { return dbtest.Config(url) }
