package store_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adityasatwar321/nebula/packages/db"
	"github.com/adityasatwar321/nebula/packages/telemetry"
	"github.com/adityasatwar321/nebula/packages/testsupport/dbtest"
	"github.com/adityasatwar321/nebula/services/controller/internal/store"
)

type usageFixture struct {
	admin    *pgxpool.Pool
	st       *store.Store
	org, dep uuid.UUID
	hour     time.Time
}

// newUsageFixture: an org on its own pricing profile (1 core-hour = 36 000 µ,
// 1 GiB-hour = 7 200 µ), a deployment of 1 core and 1 GiB with 4 parallel slots —
// 3 µ per request-second — and a store connected as nebula_controller.
func newUsageFixture(t *testing.T) *usageFixture {
	t.Helper()
	admin := dbtest.Migrated(t)
	ctx := dbtest.Context(t)
	f := &usageFixture{admin: admin, org: db.MustNewID(), dep: db.MustNewID(),
		hour: time.Now().UTC().Truncate(time.Hour)}
	model, version, profile := db.MustNewID(), db.MustNewID(), db.MustNewID()
	for _, stmt := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO organizations (id, slug, name, default_namespace) VALUES ($1, 'acme', 'Acme', 'nebula-acme')`, []any{f.org}},
		{`INSERT INTO pricing_profiles (id, org_id, name, version, cpu_core_hour_micros, memory_gib_hour_micros, effective_from)
		  VALUES ($1, $2, 'standard', 1, 36000, 7200, '2020-01-01')`, []any{profile, f.org}},
		{`UPDATE organizations SET pricing_profile_id = $2 WHERE id = $1`, []any{f.org, profile}},
		{`INSERT INTO models (id, org_id, name, task) VALUES ($1, $2, 'qwen', 'chat')`, []any{model, f.org}},
		{`INSERT INTO model_versions (id, model_id, version, format, runtime, size_bytes, checksum_sha256,
		     context_window, artifact_uri, hardware_profile, status, ready_at, runtime_config)
		  VALUES ($1, $2, 'v1', 'mock', 'mock', 10, decode(repeat('ab', 32), 'hex'), 512,
		     's3://b/sha256/' || repeat('ab', 32), '{}', 'ready', now(), '{"parallel_slots": 4}')`, []any{version, model}},
		{`INSERT INTO deployments (id, org_id, model_version_id, name, namespace, resources)
		  VALUES ($1, $2, $3, 'chat', 'nebula-acme', '{"cpu_milli":1000,"memory_mib":1024}')`, []any{f.dep, f.org, version}},
	} {
		if _, err := admin.Exec(ctx, stmt.sql, stmt.args...); err != nil {
			t.Fatalf("seeding: %v", err)
		}
	}
	cfg := dbtest.Config(admin.Config().ConnString())
	cfg.Role = "nebula_controller"
	pool, err := db.Open(ctx, cfg, "usage-test", telemetry.Discard())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	f.st = store.New(pool)
	return f
}

func (f *usageFixture) row(computeMS int64) store.UsageRow {
	dep := f.dep
	return store.UsageRow{ID: db.MustNewID(), OrgID: f.org, CreatedAt: f.hour.Add(10 * time.Minute),
		DeploymentID: &dep, ModelVersion: "qwen:v1", Endpoint: "chat.completions", Priority: "NORMAL",
		StatusCode: 200, Outcome: "completed", DurationMS: computeMS + 5, ComputeMS: computeMS, Attempts: 1,
		PromptTokens: 10, CompletionTokens: 20, SampleRate: 1}
}

type rollupRow struct {
	requests, errors, prompt, completion, cost int64
}

func (f *usageFixture) rollup(t *testing.T) rollupRow {
	t.Helper()
	var r rollupRow
	if err := f.admin.QueryRow(dbtest.Context(t), `
		SELECT request_count, error_count, prompt_tokens, completion_tokens, estimated_cost_micros
		  FROM usage_records WHERE org_id = $1 AND bucket_hour = $2 AND deployment_id = $3`,
		f.org, f.hour, f.dep).Scan(&r.requests, &r.errors, &r.prompt, &r.completion, &r.cost); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestIngestPricesStoresAndRollsUp(t *testing.T) {
	t.Parallel()
	f := newUsageFixture(t)
	ctx := dbtest.Context(t)
	rows := []store.UsageRow{f.row(1000), f.row(2000), f.row(500)}
	failed := f.row(1000)
	class := "upstream_error"
	failed.ErrorClass, failed.Outcome, failed.StatusCode = &class, "failed", 502
	rows = append(rows, failed)

	res, err := f.st.IngestUsage(ctx, rows)
	if err != nil {
		t.Fatal(err)
	}
	if res.Inserted != 4 || res.Buckets != 1 {
		t.Fatalf("result %+v", res)
	}
	// 3 µ per request-second: 3 + 6 + 1.5→2 + 3 = 14.
	got := f.rollup(t)
	if got != (rollupRow{requests: 4, errors: 1, prompt: 40, completion: 80, cost: 14}) {
		t.Fatalf("rollup %+v", got)
	}
	var mv *uuid.UUID
	_ = f.admin.QueryRow(ctx, `SELECT model_version_id FROM requests WHERE id = $1`, rows[0].ID).Scan(&mv)
	if mv == nil {
		t.Error("the model version was not resolved")
	}
}

// At-least-once delivery runs a batch twice; nothing changes the second time.
func TestIngestIsIdempotent(t *testing.T) {
	t.Parallel()
	f := newUsageFixture(t)
	ctx := dbtest.Context(t)
	rows := []store.UsageRow{f.row(1000), f.row(1000)}
	if _, err := f.st.IngestUsage(ctx, rows); err != nil {
		t.Fatal(err)
	}
	first := f.rollup(t)
	res, err := f.st.IngestUsage(ctx, rows)
	if err != nil || res.Inserted != 0 {
		t.Fatalf("redelivery: %+v %v", res, err)
	}
	if again := f.rollup(t); again != first {
		t.Fatalf("redelivery changed the rollup: %+v -> %+v", first, again)
	}
	var n int
	_ = f.admin.QueryRow(ctx, `SELECT count(*) FROM requests`).Scan(&n)
	if n != 2 {
		t.Fatalf("%d request rows, want 2", n)
	}
}

// A price change is a new version; costs already computed — and a rollup
// recomputed after the change — keep the price they were computed under.
func TestPriceChangeLeavesHistoryUnchanged(t *testing.T) {
	t.Parallel()
	f := newUsageFixture(t)
	ctx := dbtest.Context(t)
	if _, err := f.st.IngestUsage(ctx, []store.UsageRow{f.row(4000)}); err != nil {
		t.Fatal(err)
	}
	before := f.rollup(t)
	if before.cost != 12 {
		t.Fatalf("cost %d, want 12", before.cost)
	}
	// Prices triple from the next hour.
	if _, err := f.admin.Exec(ctx, `
		INSERT INTO pricing_profiles (id, org_id, name, version, cpu_core_hour_micros, memory_gib_hour_micros, effective_from)
		VALUES (gen_random_uuid(), $1, 'standard', 2, 108000, 21600, $2)`, f.org, f.hour.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	// More usage in the same, earlier hour: priced at the old version, and the
	// recomputed rollup keeps the first request's cost.
	if _, err := f.st.IngestUsage(ctx, []store.UsageRow{f.row(4000)}); err != nil {
		t.Fatal(err)
	}
	if got := f.rollup(t); got.cost != 24 {
		t.Fatalf("after a later price change: %d, want 24", got.cost)
	}
	// Usage in the new hour is priced at the new version: 9 µ per second.
	later := f.row(4000)
	later.CreatedAt = f.hour.Add(70 * time.Minute)
	if _, err := f.st.IngestUsage(ctx, []store.UsageRow{later}); err != nil {
		t.Fatal(err)
	}
	var cost int64
	_ = f.admin.QueryRow(ctx, `SELECT estimated_cost_micros FROM requests WHERE id = $1`, later.ID).Scan(&cost)
	if cost != 36 {
		t.Fatalf("new-hour cost %d, want 36", cost)
	}
}

// A version effective partway through an hour prices that hour's later requests;
// the earlier ones keep their price, and the bucket names the newest version used.
func TestMidHourPriceChange(t *testing.T) {
	t.Parallel()
	f := newUsageFixture(t)
	ctx := dbtest.Context(t)
	early := f.row(4000) // at hour+10m: 12 µ
	if _, err := f.st.IngestUsage(ctx, []store.UsageRow{early}); err != nil {
		t.Fatal(err)
	}
	v2 := db.MustNewID()
	if _, err := f.admin.Exec(ctx, `
		INSERT INTO pricing_profiles (id, org_id, name, version, cpu_core_hour_micros, memory_gib_hour_micros, effective_from)
		VALUES ($1, $2, 'standard', 2, 108000, 21600, $3)`, v2, f.org, f.hour.Add(30*time.Minute)); err != nil {
		t.Fatal(err)
	}
	late := f.row(4000)
	late.CreatedAt = f.hour.Add(40 * time.Minute) // 36 µ
	if _, err := f.st.IngestUsage(ctx, []store.UsageRow{late, early}); err != nil {
		t.Fatal(err)
	}
	if got := f.rollup(t); got.cost != 48 || got.requests != 2 {
		t.Fatalf("rollup %+v, want 2 requests costing 12 + 36", got)
	}
	var profile uuid.UUID
	_ = f.admin.QueryRow(ctx, `SELECT pricing_profile_id FROM usage_records WHERE org_id = $1 AND bucket_hour = $2`,
		f.org, f.hour).Scan(&profile)
	if profile != v2 {
		t.Errorf("the bucket names %s, want the newest version used", profile)
	}
}

// With no profile of its own, an organization is priced by the global default,
// which is zero until an operator sets real prices: no cost is invented.
func TestDefaultProfileCostsNothing(t *testing.T) {
	t.Parallel()
	f := newUsageFixture(t)
	ctx := dbtest.Context(t)
	if _, err := f.admin.Exec(ctx, `UPDATE organizations SET pricing_profile_id = NULL WHERE id = $1`, f.org); err != nil {
		t.Fatal(err)
	}
	if _, err := f.st.IngestUsage(ctx, []store.UsageRow{f.row(60_000)}); err != nil {
		t.Fatal(err)
	}
	if got := f.rollup(t); got.cost != 0 || got.requests != 1 {
		t.Fatalf("%+v", got)
	}
}

// A record from before the organization's profile took effect — a late record —
// is priced by the global default then, not left blocking the batch.
func TestRecordBeforeTheProfileUsesTheDefault(t *testing.T) {
	t.Parallel()
	f := newUsageFixture(t)
	ctx := dbtest.Context(t)
	if _, err := f.admin.Exec(ctx, `UPDATE pricing_profiles SET effective_from = $2 WHERE org_id = $1`,
		f.org, f.hour.Add(30*time.Minute)); err == nil {
		t.Fatal("pricing profiles are immutable; the trigger should refuse this")
	}
	// A new family whose only version starts later than the record.
	if _, err := f.admin.Exec(ctx, `
		WITH p AS (INSERT INTO pricing_profiles (id, org_id, name, version, cpu_core_hour_micros, memory_gib_hour_micros, effective_from)
		           VALUES (gen_random_uuid(), $1, 'later', 1, 36000, 7200, $2) RETURNING id)
		UPDATE organizations SET pricing_profile_id = (SELECT id FROM p) WHERE id = $1`, f.org, f.hour.Add(30*time.Minute)); err != nil {
		t.Fatal(err)
	}
	res, err := f.st.IngestUsage(ctx, []store.UsageRow{f.row(4000)}) // at hour+10m
	if err != nil || res.Inserted != 1 {
		t.Fatalf("%+v %v", res, err)
	}
	if got := f.rollup(t); got.cost != 0 {
		t.Fatalf("priced %d under a version not yet in force", got.cost)
	}
}
