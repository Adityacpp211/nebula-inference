package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adityasatwar321/nebula/packages/db"
	"github.com/adityasatwar321/nebula/packages/db/models"
	"github.com/adityasatwar321/nebula/packages/telemetry"
	"github.com/adityasatwar321/nebula/packages/testsupport/dbtest"
	"github.com/adityasatwar321/nebula/services/controller/internal/store"
)

type rolloutFixture struct {
	admin              *pgxpool.Pool
	st                 *store.Store
	org, route, rollID uuid.UUID
	baseline, canary   uuid.UUID
}

// rolloutFixture: an org, two deployments, a route at 100% baseline / 0% canary,
// and a pending rollout — with the store connected as nebula_controller.
func newRolloutFixture(t *testing.T) *rolloutFixture {
	t.Helper()
	admin := dbtest.Migrated(t)
	ctx := dbtest.Context(t)
	f := &rolloutFixture{admin: admin, org: db.MustNewID(), route: db.MustNewID(), rollID: db.MustNewID(),
		baseline: db.MustNewID(), canary: db.MustNewID()}
	model, version := db.MustNewID(), db.MustNewID()
	for _, stmt := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO organizations (id, slug, name, default_namespace) VALUES ($1, 'acme', 'Acme', 'nebula-acme')`, []any{f.org}},
		{`INSERT INTO models (id, org_id, name, task) VALUES ($1, $2, 'qwen', 'chat')`, []any{model, f.org}},
		{`INSERT INTO model_versions (id, model_id, version, format, runtime, size_bytes, checksum_sha256,
		     context_window, artifact_uri, hardware_profile, status, ready_at)
		  VALUES ($1, $2, 'v1', 'mock', 'mock', 10, decode(repeat('ab', 32), 'hex'), 512,
		     's3://b/sha256/' || repeat('ab', 32), '{}', 'ready', now())`, []any{version, model}},
		{`INSERT INTO deployments (id, org_id, model_version_id, name, namespace, resources)
		  VALUES ($1, $2, $3, 'stable', 'nebula-acme', '{}')`, []any{f.baseline, f.org, version}},
		{`INSERT INTO deployments (id, org_id, model_version_id, name, namespace, resources)
		  VALUES ($1, $2, $3, 'next', 'nebula-acme', '{}')`, []any{f.canary, f.org, version}},
		{`INSERT INTO routes (id, org_id, model_name) VALUES ($1, $2, 'chat')`, []any{f.route, f.org}},
		{`INSERT INTO route_targets (id, route_id, deployment_id, weight, is_baseline, label)
		  VALUES (gen_random_uuid(), $1, $2, 100, true, 'baseline'), (gen_random_uuid(), $1, $3, 0, false, 'canary')`,
			[]any{f.route, f.baseline, f.canary}},
		{`INSERT INTO canary_rollouts (id, org_id, route_id, baseline_deployment_id, canary_deployment_id, steps, analysis)
		  VALUES ($1, $2, $3, $4, $5, '[10, 50, 100]', '{"hold_s": 30}')`,
			[]any{f.rollID, f.org, f.route, f.baseline, f.canary}},
	} {
		if _, err := admin.Exec(ctx, stmt.sql, stmt.args...); err != nil {
			t.Fatalf("seeding: %v", err)
		}
	}
	cfg := dbtest.Config(admin.Config().ConnString())
	cfg.Role = "nebula_controller"
	pool, err := db.Open(ctx, cfg, "controller-rollout-test", telemetry.Discard())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	f.st = store.New(pool)
	return f
}

func (f *rolloutFixture) weights(t *testing.T) (baseline, canary int, canaryIsBaseline bool) {
	t.Helper()
	ctx := dbtest.Context(t)
	if err := f.admin.QueryRow(ctx, `SELECT weight FROM route_targets WHERE deployment_id = $1`, f.baseline).Scan(&baseline); err != nil {
		t.Fatal(err)
	}
	if err := f.admin.QueryRow(ctx, `SELECT weight, is_baseline FROM route_targets WHERE deployment_id = $1`,
		f.canary).Scan(&canary, &canaryIsBaseline); err != nil {
		t.Fatal(err)
	}
	return baseline, canary, canaryIsBaseline
}

func (f *rolloutFixture) only(t *testing.T) store.Rollout {
	t.Helper()
	rs, err := f.st.ActiveRollouts(dbtest.Context(t))
	if err != nil || len(rs) != 1 {
		t.Fatalf("active rollouts: %v %v", rs, err)
	}
	return rs[0]
}

func TestRolloutMovesWeightsStateAndAuditTogether(t *testing.T) {
	t.Parallel()
	f := newRolloutFixture(t)
	ctx := dbtest.Context(t)
	r := f.only(t)
	if r.Route != "chat" || r.Baseline != "stable" || r.Canary != "next" || r.State != models.RolloutPending {
		t.Fatalf("rollout: %+v", r)
	}

	if err := f.st.ApplyRollout(ctx, r, store.RolloutChange{State: models.RolloutProgressing, Step: 0,
		CanaryWeight: 10, Start: true, EnterStep: true, Action: "rollout.started"}); err != nil {
		t.Fatal(err)
	}
	if b, c, _ := f.weights(t); b != 90 || c != 10 {
		t.Fatalf("weights %d/%d", b, c)
	}

	// The read state is stale now: the compare-and-set refuses, and nothing moves.
	if err := f.st.ApplyRollout(ctx, r, store.RolloutChange{State: models.RolloutProgressing, Step: 1,
		CanaryWeight: 50}); !errors.Is(err, store.ErrRolloutChanged) {
		t.Fatalf("stale apply: %v", err)
	}
	if b, c, _ := f.weights(t); b != 90 || c != 10 {
		t.Fatalf("a refused change moved the weights: %d/%d", b, c)
	}

	// Promotion: canary at 100%, and it becomes the route's baseline.
	r = f.only(t)
	analysis := json.RawMessage(`{"outcome":"pass"}`)
	if err := f.st.ApplyRollout(ctx, r, store.RolloutChange{State: models.RolloutPromoted, Step: 2, CanaryWeight: 100,
		Promote: true, Finish: true, LastAnalysis: analysis, Action: "rollout.promoted"}); err != nil {
		t.Fatal(err)
	}
	if b, c, isBase := f.weights(t); b != 0 || c != 100 || !isBase {
		t.Fatalf("promoted: %d/%d canary-baseline=%v", b, c, isBase)
	}
	if rs, _ := f.st.ActiveRollouts(ctx); len(rs) != 0 {
		t.Fatalf("a promoted rollout is not active: %+v", rs)
	}
	var n int
	_ = f.admin.QueryRow(ctx, `SELECT count(*) FROM audit_logs WHERE resource_id = $1 AND actor_type = 'system'`,
		f.rollID).Scan(&n)
	if n != 2 {
		t.Errorf("%d audit rows, want 2", n)
	}
	var finished bool
	_ = f.admin.QueryRow(ctx, `SELECT finished_at IS NOT NULL FROM canary_rollouts WHERE id = $1`, f.rollID).Scan(&finished)
	if !finished {
		t.Error("finished_at not stamped")
	}
}

// An abort restores the baseline and records why; the row carries the reason the
// database requires of a rolled-back rollout.
func TestRolloutRollsBackWithItsReason(t *testing.T) {
	t.Parallel()
	f := newRolloutFixture(t)
	ctx := dbtest.Context(t)
	r := f.only(t)
	_ = f.st.ApplyRollout(ctx, r, store.RolloutChange{State: models.RolloutProgressing, CanaryWeight: 10,
		Start: true, EnterStep: true})
	r = f.only(t)
	if err := f.st.ApplyRollout(ctx, r, store.RolloutChange{State: models.RolloutRolledBack, Step: r.CurrentStep,
		CanaryWeight: 0, Finish: true, AbortReason: "canary error rate 30.00% exceeds …",
		Action: "rollout.rolled_back"}); err != nil {
		t.Fatal(err)
	}
	if b, c, isBase := f.weights(t); b != 100 || c != 0 || isBase {
		t.Fatalf("rolled back: %d/%d canary-baseline=%v", b, c, isBase)
	}
	var reason string
	_ = f.admin.QueryRow(context.Background(), `SELECT abort_reason FROM canary_rollouts WHERE id = $1`, f.rollID).Scan(&reason)
	if reason == "" {
		t.Error("no abort reason")
	}
}
