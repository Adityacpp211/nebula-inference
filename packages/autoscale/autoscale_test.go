package autoscale

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

var t0 = time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)

const tick = 15 * time.Second

// sim drives one deployment through a load curve on a simulated clock. Load is
// total requests in flight or queued; the observed signal is load per replica
// against a target of 2, so the loop must actually converge. Each scaled decision
// is applied before the next tick, as the controller would.
type sim struct {
	h        History
	settings Settings
	current  int32
	min, max int32
	now      time.Time
	rollout  bool
	missing  bool

	decisions []Decision
	changes   int
}

func newSim(current, lo, hi int32) *sim {
	s, _ := ParseSettings(json.RawMessage(`{"enabled":true}`))
	return &sim{settings: s, current: current, min: lo, max: hi, now: t0}
}

func (s *sim) run(d time.Duration, load func(time.Time) float64) {
	end := s.now.Add(d)
	for ; s.now.Before(end); s.now = s.now.Add(tick) {
		perReplica := load(s.now) / float64(maxInt32(s.current, 1))
		sig := Signal{Name: "concurrency_per_replica", Observed: perReplica, Target: 2, Source: "prometheus", Stale: s.missing}
		dec := s.h.Decide(Input{Now: s.now, Current: s.current, Min: s.min, Max: s.max, Settings: s.settings,
			Signals: []Signal{sig}, RolloutActive: s.rollout})
		s.decisions = append(s.decisions, dec)
		if dec.Changed() {
			s.changes++
			s.current = dec.To
		}
	}
}

func constant(v float64) func(time.Time) float64 { return func(time.Time) float64 { return v } }

func (s *sim) recorded(decision string) int {
	n := 0
	for _, d := range s.decisions {
		if d.Record && d.Decision == decision {
			n++
		}
	}
	return n
}

func TestRampUpConvergesWithinStepsAndWindows(t *testing.T) {
	t.Parallel()
	s := newSim(1, 1, 8)
	s.run(10*time.Minute, constant(16))
	if s.current != 8 {
		t.Fatalf("converged to %d, want 8", s.current)
	}
	var prev int32 = 1
	for _, d := range s.decisions {
		if !d.Changed() {
			continue
		}
		if d.Direction != Up || d.To > prev*2 {
			t.Errorf("step %d -> %d exceeds doubling", prev, d.To)
		}
		if d.To != prev {
			prev = d.To
		}
		if len(d.Signals) != 1 || !d.Signals[0].Selected || d.Signals[0].Ratio <= 1 {
			t.Errorf("a scale-up must carry the signal that caused it: %+v", d.Signals)
		}
	}
	// Nothing happens before the up-window has been observed.
	for _, d := range s.decisions[:2] {
		if d.Changed() {
			t.Fatal("scaled before the stabilization window had been observed")
		}
	}
}

func TestPlateauIsQuiet(t *testing.T) {
	t.Parallel()
	s := newSim(8, 1, 8)
	s.run(30*time.Minute, constant(16)) // exactly 2 per replica: on target
	if s.changes != 0 {
		t.Fatalf("%d changes on a plateau", s.changes)
	}
	for _, d := range s.decisions {
		if d.Record {
			t.Fatalf("steady state wrote a row: %+v", d)
		}
	}
}

func TestRampDownWaitsForTheWindowThenStepsDown(t *testing.T) {
	t.Parallel()
	s := newSim(8, 1, 8)
	s.run(20*time.Minute, constant(0))
	if s.current != 1 {
		t.Fatalf("idle settled at %d, want the minimum of 1", s.current)
	}
	first := -1
	for i, d := range s.decisions {
		if d.Changed() {
			if first < 0 {
				first = i
			}
			if d.From-d.To != 1 {
				t.Errorf("a scale-down of 8 or fewer moves one replica at a time: %d -> %d", d.From, d.To)
			}
		}
	}
	if waited := time.Duration(first) * tick; waited < DefaultScaleDownStabilizationS*time.Second {
		t.Fatalf("scaled down after %s, before the %ds window", waited, DefaultScaleDownStabilizationS)
	}
}

func TestOneTickSpikeDoesNotScale(t *testing.T) {
	t.Parallel()
	s := newSim(2, 1, 8)
	s.run(2*time.Minute, constant(4))
	spike := s.now
	s.run(5*time.Minute, func(at time.Time) float64 {
		if at.Equal(spike) {
			return 40
		}
		return 4
	})
	if s.changes != 0 {
		t.Fatalf("a one-tick spike changed replicas %d times", s.changes)
	}
}

// Flapping input must not oscillate replicas: load alternating every minute for a
// synthetic hour produces a handful of changes, not one per flip.
func TestFlappingInputDoesNotOscillate(t *testing.T) {
	t.Parallel()
	s := newSim(1, 1, 8)
	s.run(time.Hour, func(at time.Time) float64 {
		if (at.Sub(t0)/time.Minute)%2 == 0 {
			return 16
		}
		return 2
	})
	if s.changes > 4 {
		t.Fatalf("%d replica changes in an hour of flapping input", s.changes)
	}
	downs := 0
	for _, d := range s.decisions {
		if d.Changed() && d.Direction == Down {
			downs++
		}
	}
	if downs != 0 {
		t.Fatalf("scaled down %d times while load kept returning inside the window", downs)
	}
}

// All signals missing holds — it never falls back to the minimum — and the
// suppression is recorded once, not every tick.
func TestMissingSignalsHold(t *testing.T) {
	t.Parallel()
	s := newSim(6, 1, 8)
	s.missing = true
	s.run(30*time.Minute, constant(0))
	if s.current != 6 || s.changes != 0 {
		t.Fatalf("moved to %d with no signal", s.current)
	}
	if n := s.recorded(SuppressedNoSignal); n != 1 {
		t.Fatalf("suppressed_no_signal recorded %d times, want once", n)
	}
	for _, d := range s.decisions {
		if !d.Signals[0].Stale {
			t.Fatal("the missing signal must be marked stale in the vector")
		}
	}
}

func TestClampedAtMaxAndMin(t *testing.T) {
	t.Parallel()
	s := newSim(1, 1, 3)
	s.run(10*time.Minute, constant(40))
	if s.current != 3 {
		t.Fatalf("settled at %d, want the maximum 3", s.current)
	}
	if s.recorded(ClampedMax) != 1 {
		t.Fatalf("clamped_max recorded %d times", s.recorded(ClampedMax))
	}

	low := newSim(2, 2, 8)
	low.run(20*time.Minute, constant(0))
	if low.current != 2 || low.recorded(ClampedMin) != 1 {
		t.Fatalf("min clamp: at %d, recorded %d", low.current, low.recorded(ClampedMin))
	}
}

func TestNoScalingDuringRollout(t *testing.T) {
	t.Parallel()
	s := newSim(1, 1, 8)
	s.rollout = true
	s.run(10*time.Minute, constant(16))
	if s.changes != 0 || s.recorded(SuppressedRollout) != 1 {
		t.Fatalf("changes %d, suppressed_rollout rows %d", s.changes, s.recorded(SuppressedRollout))
	}
	s.rollout = false
	s.run(5*time.Minute, constant(16))
	if s.current == 1 {
		t.Fatal("scaling did not resume after the rollout")
	}
}

func TestToleranceAndDisabled(t *testing.T) {
	t.Parallel()
	s := newSim(4, 1, 8)
	s.run(10*time.Minute, constant(8.6)) // 2.15 per replica: within 10%
	if s.changes != 0 {
		t.Fatal("a ratio inside the tolerance changed replicas")
	}
	off := newSim(1, 1, 8)
	off.settings.Enabled = false
	off.run(10*time.Minute, constant(40))
	if off.changes != 0 || off.decisions[0].Decision != Disabled || off.decisions[0].Record {
		t.Fatalf("disabled: %+v", off.decisions[0])
	}
}

func TestCooldownSeparatesChanges(t *testing.T) {
	t.Parallel()
	s := newSim(1, 1, 64)
	s.run(15*time.Minute, constant(200))
	var last time.Time
	for i, d := range s.decisions {
		if !d.Changed() {
			continue
		}
		at := t0.Add(time.Duration(i) * tick)
		if !last.IsZero() && at.Sub(last) < DefaultCooldownS*time.Second {
			t.Fatalf("changes %s apart, cooldown is %ds", at.Sub(last), DefaultCooldownS)
		}
		last = at
	}
	if s.recorded(SuppressedCooldown) == 0 {
		t.Fatal("expected cooldown suppressions while load kept rising")
	}
}

func TestReasonsExplainTheDecision(t *testing.T) {
	t.Parallel()
	s := newSim(1, 1, 8)
	s.run(2*time.Minute, constant(16))
	for _, d := range s.decisions {
		if d.Changed() {
			if !strings.Contains(d.Reason, "concurrency_per_replica") || !strings.Contains(d.Reason, "->") {
				t.Fatalf("reason does not explain the change: %q", d.Reason)
			}
			return
		}
	}
	t.Fatal("no change to inspect")
}

func TestParseSettings(t *testing.T) {
	t.Parallel()
	s, err := ParseSettings(json.RawMessage(`{"enabled":true,"target_concurrency":3,"cooldown_s":10}`))
	if err != nil || !s.Enabled || s.TargetConcurrency != 3 || s.CooldownS != 10 ||
		s.ScaleDownStabilizationS != DefaultScaleDownStabilizationS || s.TargetQueueWaitMS != DefaultTargetQueueWaitMS {
		t.Fatalf("%+v %v", s, err)
	}
	if s, err := ParseSettings(nil); err != nil || s.Enabled {
		t.Fatalf("empty: %+v %v", s, err)
	}
	for _, bad := range []string{`{"tolerance":1}`, `{"cooldown_s":-1}`, `{"bogus":1}`, `{"scale_down_stabilization_s":7200}`} {
		if _, err := ParseSettings(json.RawMessage(bad)); err == nil {
			t.Errorf("%s accepted", bad)
		}
	}
}
