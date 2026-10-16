package rolloutctrl_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/adityasatwar321/nebula/packages/db/models"
	"github.com/adityasatwar321/nebula/packages/promquery"
	"github.com/adityasatwar321/nebula/packages/rollout"
	"github.com/adityasatwar321/nebula/packages/telemetry"
	"github.com/adityasatwar321/nebula/services/controller/internal/rolloutctrl"
	"github.com/adityasatwar321/nebula/services/controller/internal/store"
)

// fakeStore applies changes like the real one, compare-and-set included.
type fakeStore struct {
	mu      sync.Mutex
	r       store.Rollout
	weights []int // canary weight after each change that moved it
	actions []string
	reason  string
	now     func() time.Time
}

func (f *fakeStore) ActiveRollouts(context.Context) ([]store.Rollout, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch f.r.State {
	case models.RolloutPending, models.RolloutProgressing, models.RolloutPaused:
		return []store.Rollout{f.r}, nil
	}
	return nil, nil
}

func (f *fakeStore) ApplyRollout(_ context.Context, r store.Rollout, ch store.RolloutChange) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.State != f.r.State || r.CurrentStep != f.r.CurrentStep {
		return store.ErrRolloutChanged
	}
	f.r.State, f.r.CurrentStep = ch.State, ch.Step
	if len(ch.LastAnalysis) > 0 {
		f.r.LastAnalysis = ch.LastAnalysis
	}
	if ch.EnterStep {
		t := f.now()
		f.r.StepEnteredAt = &t
	}
	if ch.CanaryWeight >= 0 {
		f.weights = append(f.weights, ch.CanaryWeight)
	}
	if ch.Action != "" {
		f.actions = append(f.actions, ch.Action)
	}
	if ch.AbortReason != "" {
		f.reason = ch.AbortReason
	}
	return nil
}

// fakeArms reports per-window observations the test sets.
type fakeArms struct {
	base, canary rollout.Arm
	err          error
}

func (f *fakeArms) Arms(context.Context, uuid.UUID, uuid.UUID, time.Duration) (base, can rollout.Arm, err error) {
	return f.base, f.canary, f.err
}

type harness struct {
	st   *fakeStore
	arms *fakeArms
	rec  *rolloutctrl.Reconciler
	now  time.Time
}

func newHarness(analysis string) *harness {
	h := &harness{now: time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)}
	h.st = &fakeStore{r: store.Rollout{ID: uuid.New(), Route: "chat", Canary: "next",
		BaselineID: uuid.New(), CanaryID: uuid.New(), Steps: json.RawMessage(`[10, 25, 50, 100]`),
		Analysis: json.RawMessage(analysis), State: models.RolloutPending}, now: func() time.Time { return h.now }}
	h.arms = &fakeArms{base: rollout.Arm{Requests: 900, Errors: 1, P95MS: 400},
		canary: rollout.Arm{Requests: 100, Errors: 0, P95MS: 420}}
	h.rec = &rolloutctrl.Reconciler{Store: h.st, Analyzer: h.arms, Logger: telemetry.Discard(),
		Now: func() time.Time { return h.now }}
	return h
}

func (h *harness) run(t *testing.T, d time.Duration) {
	t.Helper()
	for end := h.now.Add(d); h.now.Before(end); h.now = h.now.Add(5 * time.Second) {
		if err := h.rec.Tick(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

const hold30 = `{"hold_s": 30, "min_requests": 50}`

// Healthy metrics walk 10 → 25 → 50 → 100, holding each step before advancing.
func TestHealthyCanaryWalksToPromotion(t *testing.T) {
	t.Parallel()
	h := newHarness(hold30)
	h.run(t, 20*time.Second)
	if h.st.r.State != models.RolloutProgressing || len(h.st.weights) != 1 || h.st.weights[0] != 10 {
		t.Fatalf("after starting: %s %v", h.st.r.State, h.st.weights)
	}
	h.run(t, 3*time.Minute)
	if h.st.r.State != models.RolloutPromoted {
		t.Fatalf("state %s, weights %v", h.st.r.State, h.st.weights)
	}
	if got := h.st.weights; len(got) != 4 || got[0] != 10 || got[1] != 25 || got[2] != 50 || got[3] != 100 {
		t.Fatalf("weights %v", got)
	}
	want := "rollout.started,rollout.advanced,rollout.advanced,rollout.promoted"
	if got := strings.Join(h.st.actions, ","); got != want {
		t.Errorf("audit %s", got)
	}
}

// Nothing moves before the hold window has passed.
func TestEachStepIsHeld(t *testing.T) {
	t.Parallel()
	h := newHarness(hold30)
	h.run(t, 25*time.Second) // started, and 20 s into the first step
	if len(h.st.weights) != 1 {
		t.Fatalf("advanced inside the hold window: %v", h.st.weights)
	}
}

func TestErrorRateBreachRollsBack(t *testing.T) {
	t.Parallel()
	h := newHarness(hold30)
	h.run(t, 5*time.Second)
	h.arms.canary = rollout.Arm{Requests: 100, Errors: 40, P95MS: 400}
	h.run(t, time.Minute)
	if h.st.r.State != models.RolloutRolledBack || h.st.weights[len(h.st.weights)-1] != 0 {
		t.Fatalf("state %s weights %v", h.st.r.State, h.st.weights)
	}
	if !strings.Contains(h.st.reason, "error rate 40.00%") {
		t.Errorf("reason %q", h.st.reason)
	}
}

func TestLatencyBreachRollsBack(t *testing.T) {
	t.Parallel()
	h := newHarness(hold30)
	h.run(t, 5*time.Second)
	h.arms.canary = rollout.Arm{Requests: 100, P95MS: 3000}
	h.run(t, time.Minute)
	if h.st.r.State != models.RolloutRolledBack || !strings.Contains(h.st.reason, "p95 3000 ms") {
		t.Fatalf("state %s reason %q", h.st.r.State, h.st.reason)
	}
}

// on_insufficient_data: extend waits further windows, then pauses; pause and fail
// act at the first short window.
func TestInsufficientData(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		choice string
		want   models.RolloutState
		within time.Duration
	}{
		{"extend", models.RolloutPaused, 3 * time.Minute},
		{"pause", models.RolloutPaused, time.Minute},
		{"fail", models.RolloutFailed, time.Minute},
	} {
		h := newHarness(`{"hold_s": 30, "min_requests": 50, "max_extensions": 2, "on_insufficient_data": "` + tc.choice + `"}`)
		h.arms.canary = rollout.Arm{Requests: 5, P95MS: 400}
		h.run(t, 5*time.Second)
		if tc.choice == "extend" {
			h.run(t, 80*time.Second) // two windows: still extending
			if h.st.r.State != models.RolloutProgressing {
				t.Fatalf("extend paused after only two windows: %s", h.st.r.State)
			}
		}
		h.run(t, tc.within)
		if h.st.r.State != tc.want {
			t.Errorf("%s: state %s", tc.choice, h.st.r.State)
		}
		if tc.want == models.RolloutFailed && h.st.weights[len(h.st.weights)-1] != 0 {
			t.Errorf("a failed rollout must restore the baseline: %v", h.st.weights)
		}
		if tc.want == models.RolloutPaused && h.st.weights[len(h.st.weights)-1] != 10 {
			t.Errorf("a paused rollout keeps its weights: %v", h.st.weights)
		}
	}
}

// Metrics that cannot be read never pass a step.
func TestBlindIsNeverHealthy(t *testing.T) {
	t.Parallel()
	h := newHarness(`{"hold_s": 30, "on_insufficient_data": "pause"}`)
	h.arms.err = errors.New("prometheus down")
	h.run(t, 2*time.Minute)
	if h.st.r.State != models.RolloutPaused || len(h.st.weights) != 1 {
		t.Fatalf("state %s weights %v", h.st.r.State, h.st.weights)
	}
	var last struct {
		Reason string `json:"reason"`
	}
	_ = json.Unmarshal(h.st.r.LastAnalysis, &last)
	if !strings.Contains(last.Reason, "metrics unavailable") {
		t.Errorf("last analysis %s", h.st.r.LastAnalysis)
	}
}

// An operator's change between the read and the write wins.
func TestOperatorWins(t *testing.T) {
	t.Parallel()
	h := newHarness(hold30)
	h.run(t, 5*time.Second)
	stale := h.st.r
	h.st.r.State = models.RolloutPaused // an operator paused it through the control plane
	if err := h.st.ApplyRollout(context.Background(), stale, store.RolloutChange{State: models.RolloutProgressing,
		Step: 1, CanaryWeight: 25}); !errors.Is(err, store.ErrRolloutChanged) {
		t.Fatalf("a stale write must be refused: %v", err)
	}
	h.run(t, 3*time.Minute)
	if h.st.r.State != models.RolloutPaused || len(h.st.weights) != 1 {
		t.Fatalf("a paused rollout moved: %s %v", h.st.r.State, h.st.weights)
	}
}

func TestQueriesSelectBothArms(t *testing.T) {
	t.Parallel()
	b, c := uuid.New(), uuid.New()
	q := (&rolloutctrl.Prometheus{Client: &promquery.Client{}}).Queries(b, c, 10*time.Second)
	for name, query := range q {
		if !strings.Contains(query, b.String()) || !strings.Contains(query, c.String()) || !strings.Contains(query, "[30s]") {
			t.Errorf("%s: %s", name, query)
		}
	}
}

// The gauges follow the rollout and go away when it finishes.
func TestRolloutGauges(t *testing.T) {
	t.Parallel()
	h := newHarness(hold30)
	m := telemetry.NewMetrics()
	h.rec.Metrics = m
	h.run(t, 10*time.Second)
	if v := testutil.ToFloat64(m.Gauge("nebula_rollout_step").WithLabelValues("chat", h.st.r.ID.String())); v != 10 {
		t.Fatalf("step gauge %v", v)
	}
	h.run(t, 3*time.Minute) // promoted
	h.run(t, 10*time.Second)
	if n := testutil.CollectAndCount(m.Gauge("nebula_rollout_step")); n != 0 {
		t.Errorf("%d step series after the rollout finished", n)
	}
	if n := testutil.CollectAndCount(m.Gauge("nebula_rollout_state")); n != 0 {
		t.Errorf("%d state series after the rollout finished", n)
	}
}
