package rollout_test

import (
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/adityasatwar321/nebula/packages/rollout"
)

func TestSteps(t *testing.T) {
	t.Parallel()
	if s, err := rollout.Steps(nil); err != nil || len(s) != 4 || s[3] != 100 {
		t.Fatalf("default: %v %v", s, err)
	}
	if s, err := rollout.Steps(json.RawMessage(`[5, 100]`)); err != nil || len(s) != 2 {
		t.Fatalf("%v %v", s, err)
	}
	for _, bad := range []string{`[]`, `[10, 10, 100]`, `[50, 25, 100]`, `[10, 50]`, `[0, 100]`, `[10, 150]`, `"x"`} {
		if _, err := rollout.Steps(json.RawMessage(bad)); err == nil {
			t.Errorf("%s accepted", bad)
		}
	}
}

func TestParseAnalysis(t *testing.T) {
	t.Parallel()
	a, err := rollout.ParseAnalysis(json.RawMessage(`{"hold_s": 30, "on_insufficient_data": "pause"}`))
	if err != nil || a.HoldS != 30 || a.OnInsufficientData != rollout.Pause || a.MinRequests != rollout.DefaultMinRequests ||
		a.MaxP95Ratio != rollout.DefaultMaxP95Ratio || a.MaxErrorRateIncrease != rollout.DefaultMaxErrorRateIncrease {
		t.Fatalf("%+v %v", a, err)
	}
	for _, bad := range []string{`{"hold_s": 5}`, `{"max_p95_ratio": 0.5}`, `{"on_insufficient_data": "panic"}`,
		`{"max_error_rate_increase": 2}`, `{"typo": 1}`} {
		if _, err := rollout.ParseAnalysis(json.RawMessage(bad)); err == nil {
			t.Errorf("%s accepted", bad)
		}
	}
}

var criteria, _ = rollout.ParseAnalysis(json.RawMessage(`{"min_requests": 50}`))

func TestHealthyCanaryPasses(t *testing.T) {
	t.Parallel()
	v := rollout.Evaluate(criteria, rollout.Arm{Requests: 900, Errors: 2, P95MS: 400},
		rollout.Arm{Requests: 100, Errors: 0, P95MS: 430})
	if v.Outcome != rollout.Pass || len(v.Checks) != 3 {
		t.Fatalf("%+v", v)
	}
	for _, c := range v.Checks {
		if !c.Passed {
			t.Errorf("check %s failed: %+v", c.Name, c)
		}
	}
}

func TestErrorRateBreachAborts(t *testing.T) {
	t.Parallel()
	v := rollout.Evaluate(criteria, rollout.Arm{Requests: 900, Errors: 1, P95MS: 400},
		rollout.Arm{Requests: 100, Errors: 30, P95MS: 400})
	if v.Outcome != rollout.Breach || !strings.Contains(v.Reason, "error rate 30.00%") {
		t.Fatalf("%+v", v)
	}
}

// A canary failing everything is aborted at once, before it has served the
// requests a pass would need.
func TestBreachIsJudgedBeforeSufficiency(t *testing.T) {
	t.Parallel()
	v := rollout.Evaluate(criteria, rollout.Arm{Requests: 900, P95MS: 400}, rollout.Arm{Requests: 8, Errors: 8, P95MS: 400})
	if v.Outcome != rollout.Breach {
		t.Fatalf("%+v", v)
	}
}

func TestLatencyBreachAborts(t *testing.T) {
	t.Parallel()
	v := rollout.Evaluate(criteria, rollout.Arm{Requests: 900, P95MS: 400}, rollout.Arm{Requests: 100, P95MS: 2400})
	if v.Outcome != rollout.Breach || !strings.Contains(v.Reason, "p95 2400 ms") {
		t.Fatalf("%+v", v)
	}
	// Within the ratio passes.
	if v := rollout.Evaluate(criteria, rollout.Arm{Requests: 900, P95MS: 400}, rollout.Arm{Requests: 100, P95MS: 470}); v.Outcome != rollout.Pass {
		t.Fatalf("%+v", v)
	}
}

func TestTooFewRequestsIsInsufficient(t *testing.T) {
	t.Parallel()
	v := rollout.Evaluate(criteria, rollout.Arm{Requests: 900, P95MS: 400}, rollout.Arm{Requests: 12, P95MS: 400})
	if v.Outcome != rollout.Insufficient || !strings.Contains(v.Reason, "12 requests") {
		t.Fatalf("%+v", v)
	}
}

// An unknown p95 (no requests in a histogram window) is not a breach.
func TestUnknownLatencyIsNotABreach(t *testing.T) {
	t.Parallel()
	v := rollout.Evaluate(criteria, rollout.Arm{Requests: 900, P95MS: math.NaN()}, rollout.Arm{Requests: 100, P95MS: 400})
	if v.Outcome != rollout.Pass {
		t.Fatalf("%+v", v)
	}
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("a verdict must serialise for last_analysis: %v", err)
	}
	if !strings.Contains(string(b), `"p95_ms":null`) {
		t.Errorf("an unknown p95 must be null: %s", b)
	}
}
