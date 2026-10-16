package controlplane_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/adityasatwar321/nebula/packages/testsupport/dbtest"
)

type experimentResults struct {
	Arms []struct {
		Label      string  `json:"label"`
		Deployment string  `json:"deployment"`
		Requests   int64   `json:"requests"`
		ErrorRate  float64 `json:"error_rate"`
		P50MS      float64 `json:"p50_ms"`
	} `json:"arms"`
	Comparison []struct {
		Baseline string `json:"baseline"`
		Variant  string `json:"variant"`
		P50MS    *struct {
			Delta float64    `json:"delta"`
			CI95  [2]float64 `json:"ci95"`
			Note  string     `json:"note"`
		} `json:"p50_ms"`
	} `json:"comparison"`
	Significance struct {
		Claimed bool   `json:"claimed"`
		Reason  string `json:"reason"`
	} `json:"significance"`
}

func TestExperimentComparesArms(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	_, v := f.createReadyVersion(t, "ab", "v1")
	a := f.createDeployment(t, "arm-a", v)
	b := f.createDeployment(t, "arm-b", v)
	var route struct {
		ID uuid.UUID `json:"id"`
	}
	if code := f.do(t, "POST", "/v1/routes", map[string]any{"model_name": "ab-route",
		"targets": []any{map[string]any{"deployment": "arm-a", "weight": 50, "label": "baseline", "is_baseline": true},
			target("arm-b", 50, "variant")}}, &route); code != 201 {
		t.Fatalf("route: %d", code)
	}
	var exp struct {
		ID    uuid.UUID `json:"id"`
		State string    `json:"state"`
	}
	if code := f.do(t, "POST", "/v1/experiments", map[string]any{"name": "faster", "route": "ab-route",
		"hypothesis": "arm-b is no slower"}, &exp); code != http.StatusCreated || exp.State != "running" {
		t.Fatalf("create: %d %+v", code, exp)
	}
	if env := f.expectError(t, http.StatusConflict, "POST", "/v1/experiments",
		map[string]any{"name": "second", "route": "ab-route"}); env.Error.Code != "experiment_conflict" {
		t.Fatalf("second running experiment: %q", env.Error.Code)
	}

	// 400 requests per arm: arm-b is 200 ms slower at the median and fails 5%.
	ctx := dbtest.Context(t)
	at := time.Now().UTC()
	for i := range 400 {
		for _, arm := range []struct {
			dep    uuid.UUID
			ms     int
			status int
		}{{a, 380 + i%40, 200}, {b, 580 + i%40, map[bool]int{true: 502, false: 200}[i%20 == 0]}} {
			if _, err := f.Pool.Exec(ctx, `
				INSERT INTO requests (id, org_id, created_at, route_id, deployment_id, status_code, duration_ms,
				                      compute_ms, completion_tokens, outcome)
				VALUES (gen_random_uuid(), $1, $2, $3, $4, $5::smallint, $6, $6, 20,
				        CASE WHEN $5::int >= 500 THEN 'failed' ELSE 'completed' END)`,
				f.OrgID, at, route.ID, arm.dep, arm.status, arm.ms); err != nil {
				t.Fatal(err)
			}
		}
	}

	var res experimentResults
	if code := f.do(t, "GET", "/v1/experiments/"+exp.ID.String()+"/results", nil, &res); code != 200 {
		t.Fatalf("results: %d", code)
	}
	if len(res.Arms) != 2 || res.Arms[0].Label != "baseline" || res.Arms[1].Label != "variant" ||
		res.Arms[0].Requests != 400 || res.Arms[1].ErrorRate != 0.05 {
		t.Fatalf("arms: %+v", res.Arms)
	}
	c := res.Comparison[0]
	if c.P50MS == nil || c.P50MS.Delta < 190 || c.P50MS.Delta > 210 || c.P50MS.CI95[0] <= 0 || c.P50MS.Note != "" {
		t.Fatalf("comparison: %+v", c.P50MS)
	}
	if res.Significance.Claimed || !strings.Contains(res.Significance.Reason, "must not be read") {
		t.Fatalf("significance: %+v", res.Significance)
	}
	// Reproducible: the same window gives the same intervals.
	var again experimentResults
	f.do(t, "GET", "/v1/experiments/"+exp.ID.String()+"/results", nil, &again)
	if again.Comparison[0].P50MS.CI95 != c.P50MS.CI95 {
		t.Fatalf("intervals moved: %v vs %v", c.P50MS.CI95, again.Comparison[0].P50MS.CI95)
	}

	var stopped struct {
		State      string  `json:"state"`
		Conclusion *string `json:"conclusion"`
	}
	if code := f.do(t, "POST", "/v1/experiments/"+exp.ID.String()+"/stop",
		map[string]any{"conclusion": "arm-b is slower; keep arm-a"}, &stopped); code != 200 || stopped.State != "stopped" ||
		stopped.Conclusion == nil {
		t.Fatalf("stop: %d %+v", code, stopped)
	}
	if env := f.expectError(t, http.StatusConflict, "POST", "/v1/experiments/"+exp.ID.String()+"/stop", nil); env.Error.Code != "invalid_state" {
		t.Errorf("stopping twice: %q", env.Error.Code)
	}
	if code := f.do(t, "POST", "/v1/experiments", map[string]any{"name": "after", "route": "ab-route"}, nil); code != 201 {
		t.Errorf("a new experiment after the first stopped: %d", code)
	}
}
