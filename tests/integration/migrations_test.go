package integration_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adityasatwar321/nebula/migrations"
	"github.com/adityasatwar321/nebula/packages/db"
	"github.com/adityasatwar321/nebula/packages/db/migrate"
)

// The Phase 1 gate: up from empty, down to empty, then up again. A down
// migration that has never run is not a down migration.
func TestMigrateUpDownUp(t *testing.T) {
	pool, _ := newDatabase(t)
	ctx := ctxT(t)
	runner := newRunner(t, pool)

	applied, err := runner.Up(ctx)
	if err != nil {
		t.Fatalf("first up: %v", err)
	}
	if len(applied) == 0 {
		t.Fatal("first up applied no migrations")
	}
	target := runner.Target()

	got, err := db.SchemaVersion(ctx, pool)
	if err != nil {
		t.Fatalf("reading schema version: %v", err)
	}
	if got != target {
		t.Fatalf("schema version after up = %d, want %d", got, target)
	}

	// Applying again must be a no-op, not an error: the migration Job reruns on
	// every Helm upgrade.
	again, err := runner.Up(ctx)
	if err != nil {
		t.Fatalf("second up: %v", err)
	}
	if len(again) != 0 {
		t.Errorf("second up applied %d migrations, want 0", len(again))
	}

	reverted, err := runner.Down(ctx, len(applied))
	if err != nil {
		t.Fatalf("down: %v", err)
	}
	if len(reverted) != len(applied) {
		t.Fatalf("down reverted %d migrations, want %d", len(reverted), len(applied))
	}

	v, stillApplied, err := runner.Version(ctx)
	if err != nil {
		t.Fatalf("reading version after down: %v", err)
	}
	if stillApplied || v != 0 {
		t.Errorf("after a full down the schema version is %d (applied=%v), want 0", v, stillApplied)
	}

	// Every table must be gone, otherwise a down migration left debris that the
	// next up would collide with.
	var tables int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM information_schema.tables
		 WHERE table_schema = 'public' AND table_name <> 'schema_migrations'`).Scan(&tables); err != nil {
		t.Fatalf("counting leftover tables: %v", err)
	}
	if tables != 0 {
		t.Errorf("%d table(s) survived the down migrations", tables)
	}

	if _, err := runner.Up(ctx); err != nil {
		t.Fatalf("second up after down: %v", err)
	}
	got, err = db.SchemaVersion(ctx, pool)
	if err != nil {
		t.Fatalf("reading schema version after re-up: %v", err)
	}
	if got != target {
		t.Errorf("schema version after re-up = %d, want %d", got, target)
	}
}

// Editing a migration that has already run applies to fresh databases but not to
// existing ones, so two environments diverge with no error anywhere. The runner
// detects it instead.
func TestChecksumDriftIsDetected(t *testing.T) {
	pool, _ := newDatabase(t)
	ctx := ctxT(t)

	if _, err := newRunner(t, pool).Up(ctx); err != nil {
		t.Fatalf("up: %v", err)
	}

	// Simulate an edited migration by corrupting the recorded checksum.
	if _, err := pool.Exec(ctx,
		`UPDATE schema_migrations SET checksum = decode(repeat('00', 32), 'hex') WHERE version = 1`); err != nil {
		t.Fatalf("corrupting the recorded checksum: %v", err)
	}

	runner := newRunner(t, pool)
	err := runner.Validate(ctx)
	if err == nil {
		t.Fatal("Validate() passed despite a modified migration")
	}
	if !errors.Is(err, migrate.ErrChecksumMismatch) {
		t.Errorf("error = %v, want it to wrap ErrChecksumMismatch", err)
	}
	if !strings.Contains(err.Error(), "modified after it was applied") {
		t.Errorf("error message is not actionable: %v", err)
	}

	// Up must refuse to run rather than layering changes on a database whose
	// history no longer matches this binary.
	if _, err := runner.Up(ctx); err == nil {
		t.Error("Up() proceeded despite checksum drift")
	}

	st, err := runner.Status(ctx)
	if err != nil {
		t.Fatalf("Status(): %v", err)
	}
	if !st[0].Drifted {
		t.Error("Status() does not report the drifted migration")
	}
}

// A database migrated by a NEWER binary must make an older binary refuse to
// serve, rather than writing columns that no longer mean what it thinks.
func TestSchemaVersionAssertion(t *testing.T) {
	pool := migratedDatabase(t)
	ctx := ctxT(t)

	loaded, err := migrate.Load(migrations.FS)
	if err != nil {
		t.Fatalf("loading migrations: %v", err)
	}
	target := loaded[len(loaded)-1].Version

	if err := db.AssertSchemaVersion(ctx, pool, target); err != nil {
		t.Fatalf("AssertSchemaVersion at the correct version failed: %v", err)
	}

	var mismatch *db.SchemaMismatchError

	err = db.AssertSchemaVersion(ctx, pool, target+1)
	if !errors.As(err, &mismatch) {
		t.Fatalf("expected a SchemaMismatchError when the binary is newer, got %v", err)
	}
	if !strings.Contains(err.Error(), "nebula-migrate up") {
		t.Errorf("error should tell the operator what to do: %v", err)
	}

	err = db.AssertSchemaVersion(ctx, pool, target-1)
	if !errors.As(err, &mismatch) {
		t.Fatalf("expected a SchemaMismatchError when the database is newer, got %v", err)
	}
	if !strings.Contains(err.Error(), "newer") {
		t.Errorf("error should say the database is newer: %v", err)
	}
}

func TestSchemaVersionOnUnmigratedDatabase(t *testing.T) {
	pool, _ := newDatabase(t)
	ctx := ctxT(t)

	_, err := db.SchemaVersion(ctx, pool)
	if !errors.Is(err, db.ErrNoSchemaTable) {
		t.Fatalf("SchemaVersion on an empty database = %v, want ErrNoSchemaTable", err)
	}

	var mismatch *db.SchemaMismatchError
	if err := db.AssertSchemaVersion(ctx, pool, 8); !errors.As(err, &mismatch) {
		t.Fatalf("AssertSchemaVersion on an empty database = %v, want SchemaMismatchError", err)
	}
}

// The seeded rows are the ones the system cannot function without.
func TestSeedData(t *testing.T) {
	pool := migratedDatabase(t)
	ctx := ctxT(t)

	t.Run("default pricing prices everything at zero", func(t *testing.T) {
		var cpu, mem, storage, egress int64
		var name string
		err := pool.QueryRow(ctx, `
			SELECT name, cpu_core_hour_micros, memory_gib_hour_micros,
			       storage_gib_month_micros, egress_gib_micros
			  FROM pricing_profiles WHERE org_id IS NULL AND version = 1`).
			Scan(&name, &cpu, &mem, &storage, &egress)
		if err != nil {
			t.Fatalf("reading the default pricing profile: %v", err)
		}
		if name != "default" {
			t.Errorf("profile name = %q, want default", name)
		}
		if cpu != 0 || mem != 0 || storage != 0 || egress != 0 {
			t.Errorf("the seeded profile must price everything at zero so no cost is ever "+
				"fabricated, got cpu=%d mem=%d storage=%d egress=%d", cpu, mem, storage, egress)
		}
	})

	t.Run("built-in routing policies", func(t *testing.T) {
		rows, err := pool.Query(ctx, `SELECT strategy, is_default FROM routing_policies WHERE org_id IS NULL`)
		if err != nil {
			t.Fatalf("reading routing policies: %v", err)
		}
		defer rows.Close()

		got := map[string]bool{}
		defaults := 0
		for rows.Next() {
			var strategy string
			var isDefault bool
			if err := rows.Scan(&strategy, &isDefault); err != nil {
				t.Fatalf("scanning: %v", err)
			}
			got[strategy] = isDefault
			if isDefault {
				defaults++
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iterating: %v", err)
		}

		for _, want := range []string{
			"round_robin", "least_loaded", "latency_aware", "cost_aware", "capability_based", "failover",
		} {
			if _, ok := got[want]; !ok {
				t.Errorf("built-in routing policy %q is missing", want)
			}
		}
		if defaults != 1 {
			t.Errorf("%d policies are marked default, want exactly 1", defaults)
		}
		if !got["least_loaded"] {
			t.Error("least_loaded must be the default strategy")
		}
	})

	t.Run("default rate limit policy is finite", func(t *testing.T) {
		var rpm, tpm, conc, depth *int32
		err := pool.QueryRow(ctx, `
			SELECT requests_per_minute, tokens_per_minute, max_concurrency, max_queue_depth
			  FROM rate_limit_policies WHERE org_id IS NULL AND name = 'default'`).
			Scan(&rpm, &tpm, &conc, &depth)
		if err != nil {
			t.Fatalf("reading the default rate limit policy: %v", err)
		}
		// An unset limit is an unbounded one, which is how one client takes down a
		// shared deployment.
		for name, v := range map[string]*int32{
			"requests_per_minute": rpm, "tokens_per_minute": tpm,
			"max_concurrency": conc, "max_queue_depth": depth,
		} {
			if v == nil {
				t.Errorf("%s is unset in the default policy, which means unlimited", name)
			}
		}
	})
}

// Seed rows must survive a down/up cycle without duplicating.
func TestSeedIsIdempotent(t *testing.T) {
	pool := migratedDatabase(t)
	ctx := ctxT(t)
	runner := newRunner(t, pool)

	count := func() int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM routing_policies WHERE org_id IS NULL`).Scan(&n); err != nil {
			t.Fatalf("counting routing policies: %v", err)
		}
		return n
	}
	before := count()

	if _, err := runner.Down(ctx, 1); err != nil {
		t.Fatalf("down 1: %v", err)
	}
	if _, err := runner.Up(ctx); err != nil {
		t.Fatalf("up after down: %v", err)
	}
	if after := count(); after != before {
		t.Errorf("routing policies = %d after a down/up cycle, want %d", after, before)
	}
}

func TestPartitionsExist(t *testing.T) {
	pool := migratedDatabase(t)
	ctx := ctxT(t)

	for _, parent := range []string{"requests", "audit_logs"} {
		var n int
		err := pool.QueryRow(ctx, `
			SELECT count(*) FROM pg_inherits i
			  JOIN pg_class p ON p.oid = i.inhparent
			 WHERE p.relname = $1`, parent).Scan(&n)
		if err != nil {
			t.Fatalf("counting partitions of %s: %v", parent, err)
		}
		// The migration creates the current month plus the next two.
		if n < 3 {
			t.Errorf("%s has %d partitions, want at least 3", parent, n)
		}
	}
}

// A row must land in the partition covering its timestamp, and the helper must
// be idempotent so the Phase 13 maintenance job can call it on a schedule.
func TestPartitionHelperIsIdempotent(t *testing.T) {
	pool := migratedDatabase(t)
	ctx := ctxT(t)

	var first, second string
	if err := pool.QueryRow(ctx,
		`SELECT nebula_ensure_month_partition('requests'::regclass, date '2031-07-15')`).Scan(&first); err != nil {
		t.Fatalf("creating a partition: %v", err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT nebula_ensure_month_partition('requests'::regclass, date '2031-07-02')`).Scan(&second); err != nil {
		t.Fatalf("second call: %v", err)
	}
	if first != second {
		t.Errorf("second call returned %q, want the same partition %q", second, first)
	}
	if !strings.Contains(first, "203107") {
		t.Errorf("partition name %q does not encode the month", first)
	}
}

func TestMigrationStatusReporting(t *testing.T) {
	pool, _ := newDatabase(t)
	ctx := ctxT(t)
	runner := newRunner(t, pool)

	st, err := runner.Status(ctx)
	if err != nil {
		t.Fatalf("Status() on an empty database: %v", err)
	}
	for _, s := range st {
		if s.Applied {
			t.Errorf("migration %06d reported applied on an empty database", s.Version)
		}
	}

	if _, err := runner.Up(ctx); err != nil {
		t.Fatalf("up: %v", err)
	}
	st, err = runner.Status(ctx)
	if err != nil {
		t.Fatalf("Status() after up: %v", err)
	}
	for _, s := range st {
		if !s.Applied {
			t.Errorf("migration %06d reported pending after up", s.Version)
		}
		if s.Drifted {
			t.Errorf("migration %06d reported drifted immediately after being applied", s.Version)
		}
		if s.AppliedAt.IsZero() {
			t.Errorf("migration %06d has no applied_at timestamp", s.Version)
		}
	}
}

// Concurrent migration runs must serialise on the advisory lock rather than both
// trying to apply the same migration.
func TestConcurrentMigrationsSerialise(t *testing.T) {
	pool, url := newDatabase(t)
	ctx := ctxT(t)

	second, err := db.Open(ctx, testDBConfig(url), "nebula-integration-test-2", nil)
	if err != nil {
		t.Fatalf("opening a second pool: %v", err)
	}
	defer second.Close()

	type result struct {
		applied []int64
		err     error
	}
	results := make(chan result, 2)
	for _, p := range []*pgxpool.Pool{pool, second} {
		go func(p *pgxpool.Pool) {
			r := newRunner(t, p)
			applied, err := r.Up(context.Background())
			results <- result{applied: applied, err: err}
		}(p)
	}

	total := 0
	for range 2 {
		r := <-results
		if r.err != nil {
			t.Fatalf("concurrent Up() failed: %v", r.err)
		}
		total += len(r.applied)
	}

	loaded, _ := migrate.Load(migrations.FS)
	if total != len(loaded) {
		t.Errorf("the two runs applied %d migrations in total, want exactly %d: "+
			"each migration must be applied exactly once", total, len(loaded))
	}
}
