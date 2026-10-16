package controlplane_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/adityasatwar321/nebula/packages/db/models"
	"github.com/adityasatwar321/nebula/packages/testsupport/dbtest"
)

type rolloutBody struct {
	ID                   uuid.UUID `json:"id"`
	RouteID              uuid.UUID `json:"route_id"`
	BaselineDeploymentID uuid.UUID `json:"baseline_deployment_id"`
	CanaryDeploymentID   uuid.UUID `json:"canary_deployment_id"`
	Steps                []int     `json:"steps"`
	CanaryWeight         int       `json:"canary_weight"`
	State                string    `json:"state"`
	AbortReason          *string   `json:"abort_reason"`
	Analysis             struct {
		HoldS              int    `json:"hold_s"`
		MinRequests        int    `json:"min_requests"`
		OnInsufficientData string `json:"on_insufficient_data"`
	} `json:"analysis"`
}

// serving walks a deployment to ready through the admin transition endpoint.
func (f *fixture) serving(t *testing.T, id uuid.UUID) {
	t.Helper()
	for _, s := range []struct {
		from, to models.DeploymentState
		reason   string
	}{
		{"pending", "provisioning", "Claimed"}, {"provisioning", "starting", "ObjectsApplied"},
		{"starting", "ready", "ReplicasReady"},
	} {
		if code := f.transition(t, id, s.from, s.to, s.reason); code != http.StatusOK {
			t.Fatalf("%s -> %s: %d", s.from, s.to, code)
		}
	}
}

func (f *fixture) weightOf(t *testing.T, route, dep uuid.UUID) (weight int, baseline bool) {
	t.Helper()
	if err := f.Pool.QueryRow(dbtest.Context(t), `SELECT weight, is_baseline FROM route_targets
		WHERE route_id = $1 AND deployment_id = $2`, route, dep).Scan(&weight, &baseline); err != nil {
		t.Fatal(err)
	}
	return weight, baseline
}

func TestRolloutLifecycleThroughTheAPI(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	_, v := f.createReadyVersion(t, "canaried", "v1")
	stable := f.createDeployment(t, "stable", v)
	next := f.createDeployment(t, "next", v)
	f.serving(t, stable)
	if code := f.do(t, "POST", "/v1/routes", map[string]any{"model_name": "chat",
		"targets": []any{target("stable", 100, "")}}, nil); code != 201 {
		t.Fatalf("route: %d", code)
	}
	body := map[string]any{"route": "chat", "canary_deployment": "next", "steps": []int{20, 100},
		"analysis": map[string]any{"hold_s": 30, "on_insufficient_data": "pause"}}

	// The canary must be serving before traffic shifts to it.
	if env := f.expectError(t, http.StatusConflict, "POST", "/v1/rollouts", body); env.Error.Code != "canary_not_ready" {
		t.Fatalf("code %q", env.Error.Code)
	}
	f.serving(t, next)

	var ro rolloutBody
	if code := f.do(t, "POST", "/v1/rollouts", body, &ro); code != http.StatusCreated {
		t.Fatalf("create: %d", code)
	}
	if ro.State != "pending" || ro.CanaryWeight != 0 || ro.BaselineDeploymentID != stable || ro.CanaryDeploymentID != next ||
		ro.Analysis.HoldS != 30 || ro.Analysis.MinRequests != 100 || ro.Analysis.OnInsufficientData != "pause" {
		t.Fatalf("created: %+v", ro)
	}
	if w, base := f.weightOf(t, ro.RouteID, stable); w != 100 || !base {
		t.Errorf("baseline target: %d %v", w, base)
	}
	if w, base := f.weightOf(t, ro.RouteID, next); w != 0 || base {
		t.Errorf("canary target: %d %v", w, base)
	}

	// One active rollout per route, by the database.
	if env := f.expectError(t, http.StatusConflict, "POST", "/v1/rollouts", body); env.Error.Code != "rollout_active" {
		t.Fatalf("second rollout: %q", env.Error.Code)
	}

	path := "/v1/rollouts/" + ro.ID.String()
	if code := f.do(t, "POST", path+"/pause", nil, &ro); code != 200 || ro.State != "paused" {
		t.Fatalf("pause: %d %s", code, ro.State)
	}
	if code := f.do(t, "POST", path+"/resume", nil, &ro); code != 200 || ro.State != "progressing" {
		t.Fatalf("resume: %d %s", code, ro.State)
	}
	if code := f.do(t, "POST", path+"/promote", nil, &ro); code != 200 || ro.State != "promoted" || ro.CanaryWeight != 100 {
		t.Fatalf("promote: %d %+v", code, ro)
	}
	if w, base := f.weightOf(t, ro.RouteID, next); w != 100 || !base {
		t.Errorf("a promoted canary is the route's baseline at 100: %d %v", w, base)
	}
	if w, base := f.weightOf(t, ro.RouteID, stable); w != 0 || base {
		t.Errorf("the old baseline: %d %v", w, base)
	}
	if env := f.expectError(t, http.StatusConflict, "POST", path+"/pause", nil); env.Error.Code != "invalid_state" {
		t.Errorf("pausing a finished rollout: %q", env.Error.Code)
	}

	// The route is free again: roll the old deployment back in, then abort.
	var back rolloutBody
	if code := f.do(t, "POST", "/v1/rollouts", map[string]any{"route": "chat", "canary_deployment": "stable"},
		&back); code != http.StatusCreated {
		t.Fatalf("second rollout after promotion: %d", code)
	}
	if code := f.do(t, "POST", "/v1/rollouts/"+back.ID.String()+"/abort", map[string]any{"reason": "changed my mind"},
		&back); code != 200 || back.State != "aborted" || back.CanaryWeight != 0 || back.AbortReason == nil ||
		!strings.Contains(*back.AbortReason, "changed my mind") {
		t.Fatalf("abort: %d %+v", code, back)
	}

	var list struct {
		Data []rolloutBody `json:"data"`
	}
	if code := f.do(t, "GET", "/v1/rollouts?state=promoted", nil, &list); code != 200 || len(list.Data) != 1 {
		t.Fatalf("list: %d %d", code, len(list.Data))
	}
	var n int
	_ = f.Pool.QueryRow(dbtest.Context(t), `SELECT count(*) FROM audit_logs WHERE action LIKE 'rollout.%'`).Scan(&n)
	if n != 6 {
		t.Errorf("%d rollout audit rows, want 6 (create, pause, resume, promote, create, abort)", n)
	}
}

func TestRolloutRefusals(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	_, v := f.createReadyVersion(t, "refused", "v1")
	a := f.createDeployment(t, "a", v)
	b := f.createDeployment(t, "b", v)
	f.serving(t, a)
	f.serving(t, b)
	if code := f.do(t, "POST", "/v1/routes", map[string]any{"model_name": "split",
		"targets": []any{target("a", 50, ""), target("b", 50, "")}}, nil); code != 201 {
		t.Fatalf("route: %d", code)
	}
	if code := f.do(t, "POST", "/v1/routes", map[string]any{"model_name": "single",
		"targets": []any{target("a", 100, "")}}, nil); code != 201 {
		t.Fatalf("route: %d", code)
	}
	for name, tc := range map[string]struct {
		body   map[string]any
		status int
		code   string
	}{
		"split route":     {map[string]any{"route": "split", "canary_deployment": "b"}, 409, "route_split"},
		"canary=baseline": {map[string]any{"route": "single", "canary_deployment": "a"}, 400, "canary_is_baseline"},
		"no route":        {map[string]any{"route": "nope", "canary_deployment": "b"}, 422, "route_not_found"},
		"no canary":       {map[string]any{"route": "single", "canary_deployment": "nope"}, 422, "deployment_not_found"},
		"bad steps": {map[string]any{"route": "single", "canary_deployment": "b", "steps": []int{50, 10, 100}},
			400, "invalid_steps"},
		"no 100": {map[string]any{"route": "single", "canary_deployment": "b", "steps": []int{10, 50}}, 400, "invalid_steps"},
		"bad analysis": {map[string]any{"route": "single", "canary_deployment": "b",
			"analysis": map[string]any{"max_p95_ratio": 0.5}}, 400, "invalid_analysis"},
	} {
		var env apiError
		if got := f.do(t, "POST", "/v1/rollouts", tc.body, &env); got != tc.status || env.Error.Code != tc.code {
			t.Errorf("%s: %d %q, want %d %q (%s)", name, got, env.Error.Code, tc.status, tc.code, env.Error.Message)
		}
	}
}
