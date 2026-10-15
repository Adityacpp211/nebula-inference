package store_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adityasatwar321/nebula/packages/autoscale"
	"github.com/adityasatwar321/nebula/packages/db"
	"github.com/adityasatwar321/nebula/packages/telemetry"
	"github.com/adityasatwar321/nebula/packages/testsupport/dbtest"
	"github.com/adityasatwar321/nebula/services/autoscaler/internal/store"
)

type fixture struct {
	admin *pgxpool.Pool
	st    *store.Store
	pool  *pgxpool.Pool // connected AS nebula_controller
	org   uuid.UUID
	ver   uuid.UUID
}

// newFixture: a migrated database with one org and one ready version, and a store
// connected as nebula_controller, so the grants are what these tests exercise.
func newFixture(t *testing.T) *fixture {
	t.Helper()
	admin := dbtest.Migrated(t)
	ctx := dbtest.Context(t)
	f := &fixture{admin: admin, org: db.MustNewID(), ver: db.MustNewID()}
	model := db.MustNewID()
	for _, stmt := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO organizations (id, slug, name, default_namespace) VALUES ($1, 'acme', 'Acme', 'nebula-acme')`, []any{f.org}},
		{`INSERT INTO models (id, org_id, name, task) VALUES ($1, $2, 'qwen', 'chat')`, []any{model, f.org}},
		{`INSERT INTO model_versions (id, model_id, version, format, runtime, size_bytes, checksum_sha256,
		     context_window, artifact_uri, hardware_profile, status, ready_at)
		  VALUES ($1, $2, 'v1', 'mock', 'mock', 10, decode(repeat('ab', 32), 'hex'), 512,
		     's3://b/sha256/' || repeat('ab', 32), '{}', 'ready', now())`, []any{f.ver, model}},
	} {
		if _, err := admin.Exec(ctx, stmt.sql, stmt.args...); err != nil {
			t.Fatalf("seeding: %v", err)
		}
	}
	cfg := dbtest.Config(admin.Config().ConnString())
	cfg.Role = "nebula_controller"
	pool, err := db.Open(ctx, cfg, "autoscaler-test", telemetry.Discard())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	f.pool, f.st = pool, store.New(pool)
	return f
}

// deployment inserts one deployment in a state, as an administrator would.
func (f *fixture) deployment(t *testing.T, name, state, autoscaling string, desired int) uuid.UUID {
	t.Helper()
	id := db.MustNewID()
	if _, err := f.admin.Exec(dbtest.Context(t), `
		INSERT INTO deployments (id, org_id, model_version_id, name, namespace, resources,
		                         desired_replicas, min_replicas, max_replicas, autoscaling)
		VALUES ($1, $2, $3, $4, 'nebula-acme', '{"cpu_milli":500,"memory_mib":256}', $5, 1, 6, $6)`,
		id, f.org, f.ver, name, desired, autoscaling); err != nil {
		t.Fatal(err)
	}
	// Walk the lifecycle to the state under test: the transition trigger refuses a
	// jump, and that rule is not this package's to bypass.
	path := map[string][]string{
		"pending":  nil,
		"ready":    {"provisioning", "starting", "ready"},
		"degraded": {"provisioning", "starting", "ready", "degraded"},
		"stopped":  {"provisioning", "starting", "ready", "stopping", "stopped"},
	}[state]
	ctx := dbtest.Context(t)
	tx, err := f.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT set_config('app.current_actor_type', 'system', true)`); err != nil {
		t.Fatal(err)
	}
	for _, to := range path {
		if _, err := tx.Exec(ctx, `UPDATE deployments SET state = $2, state_reason = $3 WHERE id = $1`,
			id, to, reasons[to]); err != nil {
			t.Fatalf("-> %s: %v", to, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return id
}

func (f *fixture) row(t *testing.T, id uuid.UUID) (desired int32, generation int64) {
	t.Helper()
	if err := f.admin.QueryRow(dbtest.Context(t), `SELECT desired_replicas, generation FROM deployments WHERE id = $1`,
		id).Scan(&desired, &generation); err != nil {
		t.Fatal(err)
	}
	return desired, generation
}

const on = `{"enabled": true}`

var reasons = map[string]string{
	"provisioning": "Claimed", "starting": "ObjectsApplied", "ready": "ReplicasReady",
	"degraded": "ReplicaLost", "stopping": "StopRequested", "stopped": "Drained",
}

func TestTargetsAreRunningAutoscaledDeployments(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	ctx := dbtest.Context(t)
	ready := f.deployment(t, "ready", "ready", on, 2)
	f.deployment(t, "off", "ready", `{"enabled": false}`, 1)
	f.deployment(t, "unset", "ready", `{}`, 1)
	f.deployment(t, "stopped", "stopped", on, 1)
	f.deployment(t, "pending", "pending", on, 1)
	canary := f.deployment(t, "canary", "degraded", on, 1)

	route := db.MustNewID()
	if _, err := f.admin.Exec(ctx, `INSERT INTO routes (id, org_id, model_name) VALUES ($1, $2, 'r')`,
		route, f.org); err != nil {
		t.Fatal(err)
	}
	if _, err := f.admin.Exec(ctx, `
		INSERT INTO canary_rollouts (id, org_id, route_id, baseline_deployment_id, canary_deployment_id, steps, analysis, state)
		VALUES (gen_random_uuid(), $2, $1, $3, $4, '[{"weight": 10}]', '{}', 'progressing')`,
		route, f.org, ready, canary); err != nil {
		t.Fatal(err)
	}

	got, err := f.st.Targets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]store.Target{}
	for _, tg := range got {
		names[tg.Name] = tg
	}
	if len(got) != 2 || names["ready"].ID != ready || names["canary"].ID != canary {
		t.Fatalf("targets: %+v", got)
	}
	if !names["ready"].RolloutActive || !names["canary"].RolloutActive {
		t.Error("both sides of a progressing rollout must carry the interlock")
	}
	if r := names["ready"]; r.Desired != 2 || r.Min != 1 || r.Max != 6 || r.OrgSlug != "acme" {
		t.Errorf("target: %+v", r)
	}
}

// A change and its explanation are one transaction, and the change advances
// generation so the controller reconciles it at once.
func TestRecordScalesAndExplains(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	ctx := dbtest.Context(t)
	id := f.deployment(t, "dep", "ready", on, 2)
	_, gen := f.row(t, id)
	tg := store.Target{ID: id, OrgID: f.org, Name: "dep", Desired: 2, Min: 1, Max: 6}

	sigs := []autoscale.Signal{{Name: "concurrency_per_replica", Observed: 8, Target: 2, Ratio: 4, Selected: true}}
	up := autoscale.Decision{From: 2, To: 4, Direction: autoscale.Up, Decision: autoscale.Scaled,
		Reason: "concurrency_per_replica 8 vs target 2: 2 -> 4", Signals: sigs, Record: true}
	if err := f.st.Record(ctx, tg, up); err != nil {
		t.Fatal(err)
	}
	desired, after := f.row(t, id)
	if desired != 4 || after != gen+1 {
		t.Fatalf("desired %d generation %d, want 4 and %d", desired, after, gen+1)
	}

	var (
		from, to         int32
		dir, dec, reason string
		signals          json.RawMessage
	)
	if err := f.admin.QueryRow(ctx, `SELECT from_replicas, to_replicas, direction, decision, reason, signals
		FROM autoscaling_events WHERE deployment_id = $1`, id).Scan(&from, &to, &dir, &dec, &reason, &signals); err != nil {
		t.Fatal(err)
	}
	var stored []autoscale.Signal
	_ = json.Unmarshal(signals, &stored)
	if from != 2 || to != 4 || dir != "up" || dec != "scaled" || !strings.Contains(reason, "2 -> 4") ||
		len(stored) != 1 || stored[0].Ratio != 4 || !stored[0].Selected {
		t.Errorf("event: %d %d %s %s %q %s", from, to, dir, dec, reason, signals)
	}

	// A suppression is recorded without touching the deployment.
	held := autoscale.Decision{From: 4, To: 4, Direction: autoscale.Hold, Decision: autoscale.SuppressedNoSignal,
		Reason: "no usable signal", Record: true}
	tg.Desired = 4
	if err := f.st.Record(ctx, tg, held); err != nil {
		t.Fatal(err)
	}
	if _, g := f.row(t, id); g != gen+1 {
		t.Errorf("a suppression moved generation to %d", g)
	}
}

// The update is a compare-and-set: a user's scale the autoscaler has not seen
// wins, and nothing — not even the explanation — is written.
func TestRecordNeverOverwritesAnUnseenScale(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	ctx := dbtest.Context(t)
	id := f.deployment(t, "dep", "ready", on, 2)
	if _, err := f.admin.Exec(ctx, `UPDATE deployments SET desired_replicas = 5, generation = generation + 1 WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	// A control-plane write that already advanced generation is not counted twice.
	if _, gen := f.row(t, id); gen != 2 {
		t.Fatalf("generation %d after one spec write, want 2", gen)
	}
	_, gen := f.row(t, id)
	tg := store.Target{ID: id, OrgID: f.org, Desired: 2, Min: 1, Max: 6}
	err := f.st.Record(ctx, tg, autoscale.Decision{From: 2, To: 3, Direction: autoscale.Up,
		Decision: autoscale.Scaled, Reason: "x", Record: true})
	if !errors.Is(err, store.ErrConflict) {
		t.Fatalf("want ErrConflict, got %v", err)
	}
	if d, g := f.row(t, id); d != 5 || g != gen {
		t.Errorf("desired %d generation %d after a refused write", d, g)
	}
	var n int
	_ = f.admin.QueryRow(ctx, `SELECT count(*) FROM autoscaling_events WHERE deployment_id = $1`, id).Scan(&n)
	if n != 0 {
		t.Errorf("%d events for a change that did not happen", n)
	}
	// Out of bounds is refused by the same predicate.
	tg.Desired = 5
	if err := f.st.Record(ctx, tg, autoscale.Decision{From: 5, To: 9, Direction: autoscale.Up,
		Decision: autoscale.Scaled, Reason: "x", Record: true}); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("beyond max: %v", err)
	}
}

// The autoscaler's role owns desired_replicas and nothing else on deployments.
func TestAutoscalerRoleCannotWriteTheSpec(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	ctx := dbtest.Context(t)
	id := f.deployment(t, "dep", "ready", on, 2)
	for _, sql := range []string{
		`UPDATE deployments SET generation = generation + 1 WHERE id = $1`,
		`UPDATE deployments SET max_replicas = 50 WHERE id = $1`,
		`UPDATE deployments SET autoscaling = '{}' WHERE id = $1`,
		`UPDATE autoscaling_events SET reason = 'rewritten' WHERE deployment_id = $1`,
	} {
		_, err := f.pool.Exec(ctx, sql, id)
		if err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Errorf("%s: want permission denied, got %v", sql, err)
		}
	}
}
