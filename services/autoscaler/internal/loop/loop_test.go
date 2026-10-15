package loop_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/adityasatwar321/nebula/packages/autoscale"
	"github.com/adityasatwar321/nebula/packages/telemetry"
	"github.com/adityasatwar321/nebula/services/autoscaler/internal/loop"
	"github.com/adityasatwar321/nebula/services/autoscaler/internal/signals"
	"github.com/adityasatwar321/nebula/services/autoscaler/internal/store"
)

var dep = uuid.MustParse("33333333-3333-4333-8333-333333333333")

// fakeStore applies recorded changes, like the real one, so the loop converges.
type fakeStore struct {
	targets  []store.Target
	recorded []autoscale.Decision
	listErr  error
	conflict bool
}

func (f *fakeStore) Targets(context.Context) ([]store.Target, error) {
	return append([]store.Target(nil), f.targets...), f.listErr
}

func (f *fakeStore) Record(_ context.Context, t store.Target, d autoscale.Decision) error {
	if d.Changed() && f.conflict {
		return store.ErrConflict
	}
	f.recorded = append(f.recorded, d)
	for i := range f.targets {
		if f.targets[i].ID == t.ID && d.Changed() {
			f.targets[i].Desired = d.To
		}
	}
	return nil
}

// fakeSignals reports total load spread over the store's current replicas.
type fakeSignals struct {
	load float64
	err  error
}

func (f *fakeSignals) Read(context.Context) signals.Reading {
	if f.err != nil {
		return signals.Reading{Err: f.err}
	}
	return signals.Reading{
		WorkerLoad:    map[uuid.UUID]float64{dep: f.load},
		GatewayQueued: map[uuid.UUID]float64{},
		Slots:         map[uuid.UUID]float64{dep: 2},
		QueueWaitMS:   map[uuid.UUID]float64{},
	}
}

type harness struct {
	l   *loop.Loop
	st  *fakeStore
	sig *fakeSignals
	now time.Time
	m   *telemetry.Metrics
}

func newHarness() *harness {
	h := &harness{
		st: &fakeStore{targets: []store.Target{{ID: dep, OrgSlug: "acme", Name: "chat", Desired: 1, Min: 1, Max: 6,
			Autoscaling: json.RawMessage(`{"enabled": true}`)}}},
		sig: &fakeSignals{},
		now: time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC),
		m:   telemetry.NewMetrics(),
	}
	h.l = &loop.Loop{Store: h.st, Signals: h.sig, Metrics: h.m, Logger: telemetry.Discard(),
		Now: func() time.Time { return h.now }}
	return h
}

func (h *harness) run(t *testing.T, d time.Duration) {
	t.Helper()
	for end := h.now.Add(d); h.now.Before(end); h.now = h.now.Add(15 * time.Second) {
		if err := h.l.Tick(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

// Load raises replicas, idle lowers them after the window, and every change is
// a recorded, explained row.
func TestLoadRaisesAndIdleLowers(t *testing.T) {
	t.Parallel()
	h := newHarness()
	h.sig.load = 12 // 6 replicas' worth at 2 slots each
	h.run(t, 5*time.Minute)
	if got := h.st.targets[0].Desired; got != 6 {
		t.Fatalf("under load: %d replicas, want 6", got)
	}
	changes := 0
	for _, d := range h.st.recorded {
		if d.Changed() {
			changes++
			if !strings.Contains(d.Reason, "concurrency_per_replica") {
				t.Errorf("unexplained change: %q", d.Reason)
			}
		}
	}
	if changes < 2 {
		t.Errorf("1 -> 6 in %d steps: the step limit allows at most doubling", changes)
	}
	if v := testutil.ToFloat64(h.m.Counter("nebula_autoscale_decisions_total").WithLabelValues("chat", "up", "scaled")); v < 2 {
		t.Errorf("decisions_total{up,scaled} = %v", v)
	}

	h.sig.load = 0
	h.run(t, 4*time.Minute)
	if got := h.st.targets[0].Desired; got != 6 {
		t.Fatalf("scaled down to %d inside the 300s window", got)
	}
	h.run(t, 10*time.Minute)
	if got := h.st.targets[0].Desired; got != 1 {
		t.Fatalf("idle: %d replicas, want 1", got)
	}
}

func TestPrometheusDownHoldsAndRecordsOnce(t *testing.T) {
	t.Parallel()
	h := newHarness()
	h.st.targets[0].Desired = 4
	h.sig.err = errors.New("connection refused")
	h.run(t, 10*time.Minute)
	if h.st.targets[0].Desired != 4 {
		t.Fatalf("moved to %d with no signals", h.st.targets[0].Desired)
	}
	if len(h.st.recorded) != 1 || h.st.recorded[0].Decision != autoscale.SuppressedNoSignal {
		t.Fatalf("recorded: %+v", h.st.recorded)
	}
	// No ratio series for a missing signal: absence, not "idle".
	if n := testutil.CollectAndCount(h.m.Gauge("nebula_autoscale_signal_ratio")); n != 0 {
		t.Errorf("%d ratio series while every signal is missing", n)
	}
}

func TestConflictIsNotFatal(t *testing.T) {
	t.Parallel()
	h := newHarness()
	h.st.conflict = true
	h.sig.load = 12
	h.run(t, 2*time.Minute)
	if h.st.targets[0].Desired != 1 {
		t.Fatal("a conflicting write must not apply")
	}
	h.st.conflict = false
	h.run(t, 2*time.Minute)
	if h.st.targets[0].Desired == 1 {
		t.Fatal("the loop must keep deciding after a conflict")
	}
}

// A tick cut short by shutdown decides nothing: its failed queries are not
// missing signals, and recording them as such would be a false row.
func TestCancelledTickRecordsNothing(t *testing.T) {
	t.Parallel()
	h := newHarness()
	h.sig.err = context.Canceled
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := h.l.Tick(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if len(h.st.recorded) != 0 {
		t.Fatalf("recorded %+v during shutdown", h.st.recorded)
	}
}

func TestListFailureDecidesNothing(t *testing.T) {
	t.Parallel()
	h := newHarness()
	h.st.listErr = errors.New("database down")
	if err := h.l.Tick(context.Background()); err == nil {
		t.Fatal("want the list error")
	}
	if len(h.st.recorded) != 0 {
		t.Fatal("decided without a deployment list")
	}
}

// A deployment that stops being a target is forgotten, metrics included, and
// observes its windows afresh if it returns.
func TestForgottenDeploymentsStartAfresh(t *testing.T) {
	t.Parallel()
	h := newHarness()
	h.sig.load = 12
	h.run(t, time.Minute)
	if n := testutil.CollectAndCount(h.m.Gauge("nebula_autoscale_signal_ratio")); n != 1 {
		t.Fatalf("%d ratio series, want 1", n)
	}
	saved := h.st.targets
	h.st.targets = nil
	h.run(t, 15*time.Second)
	if n := testutil.CollectAndCount(h.m.Gauge("nebula_autoscale_signal_ratio")); n != 0 {
		t.Errorf("%d ratio series for a deployment no longer autoscaled", n)
	}
	h.st.targets = saved
	before := h.st.targets[0].Desired
	h.run(t, 15*time.Second)
	if h.st.targets[0].Desired != before {
		t.Error("a returning deployment acted before observing its window")
	}
}

func TestInvalidSettingsHold(t *testing.T) {
	t.Parallel()
	h := newHarness()
	h.st.targets[0].Autoscaling = json.RawMessage(`{"enabled": true, "tolerance": 7}`)
	h.sig.load = 12
	h.run(t, 5*time.Minute)
	if h.st.targets[0].Desired != 1 || len(h.st.recorded) != 0 {
		t.Fatalf("acted on settings it cannot parse: %d %+v", h.st.targets[0].Desired, h.st.recorded)
	}
}
