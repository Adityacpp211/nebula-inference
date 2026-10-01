package routing

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"
)

var t0 = time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)

func hb(inflight, queue, slots int) *Heartbeat {
	return &Heartbeat{State: "ready", Accepting: true, InFlight: inflight, QueueDepth: queue,
		ParallelSlots: slots, ModelVersion: "m:v1"}
}

func opts(now time.Time) FilterOptions {
	return FilterOptions{Now: now, StaleAfter: 3 * time.Second, HeartbeatsLive: true, ModelVersion: "m:v1"}
}

func TestFilter(t *testing.T) {
	t.Parallel()
	fresh := t0.Add(-time.Second)
	cases := []struct {
		name string
		ep   Endpoint
		req  Request
		o    FilterOptions
		want string
	}{
		{"ready with a fresh heartbeat", Endpoint{Ready: true, Heartbeat: hb(0, 0, 1), HeardAt: fresh}, Request{}, opts(t0), ""},
		{"never heartbeated is judged by readiness", Endpoint{Ready: true}, Request{}, opts(t0), ""},
		{"not ready beats everything", Endpoint{Ready: false, Heartbeat: hb(0, 0, 1), HeardAt: fresh}, Request{}, opts(t0), ExcludedNotReady},
		{"breaker open", Endpoint{Ready: true, Breaker: BreakerOpen}, Request{}, opts(t0), ExcludedBreakerOpen},
		{"stale while heartbeats are live", Endpoint{Ready: true, Heartbeat: hb(0, 0, 1), HeardAt: t0.Add(-4 * time.Second)}, Request{}, opts(t0), ExcludedStale},
		{"stale is ignored when the channel is down", Endpoint{Ready: true, Heartbeat: hb(0, 0, 1), HeardAt: t0.Add(-time.Minute)},
			Request{}, FilterOptions{Now: t0, HeartbeatsLive: false, ModelVersion: "m:v1"}, ""},
		{"not accepting", Endpoint{Ready: true, Heartbeat: &Heartbeat{State: "draining", Accepting: false}, HeardAt: fresh}, Request{}, opts(t0), ExcludedNotAccepting},
		{"wrong model version", Endpoint{Ready: true, Heartbeat: &Heartbeat{State: "ready", Accepting: true, ModelVersion: "m:v2"}, HeardAt: fresh},
			Request{}, opts(t0), ExcludedVersion},
		{"context window too small", Endpoint{Ready: true, ContextWindow: 2048}, Request{RequiredContext: 4096}, opts(t0), ExcludedContextWindow},
		{"context window fits", Endpoint{Ready: true, ContextWindow: 8192}, Request{RequiredContext: 4096}, opts(t0), ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			c.ep.ID = "e"
			_, excluded := Filter([]Endpoint{c.ep}, c.req, c.o)
			if got := excluded["e"]; got != c.want {
				t.Fatalf("excluded %q, want %q", got, c.want)
			}
		})
	}
}

// The Phase 6 staleness test: a heartbeat that stops arriving removes the endpoint
// exactly at the documented window, not before and not long after.
func TestStaleHeartbeatRemovesEndpointWithinWindow(t *testing.T) {
	t.Parallel()
	ep := Endpoint{ID: "pod-a", Ready: true, Heartbeat: hb(0, 0, 1), HeardAt: t0}
	for _, c := range []struct {
		after time.Duration
		want  bool
	}{
		{0, true}, {time.Second, true}, {3 * time.Second, true},
		{3*time.Second + time.Millisecond, false}, {10 * time.Second, false},
	} {
		eligible, _ := Filter([]Endpoint{ep}, Request{}, opts(t0.Add(c.after)))
		if got := len(eligible) == 1; got != c.want {
			t.Errorf("%v after the last heartbeat: eligible=%v, want %v", c.after, got, c.want)
		}
	}
}

func TestLeastLoaded(t *testing.T) {
	t.Parallel()
	now := t0
	eps := []Endpoint{
		{ID: "a", Ready: true, Heartbeat: hb(4, 2, 4), HeardAt: now},                   // 1.5 per slot
		{ID: "b", Ready: true, Heartbeat: hb(1, 0, 4), HeardAt: now},                   // 0.25
		{ID: "c", Ready: true, Heartbeat: hb(0, 0, 1), HeardAt: now, LocalInFlight: 1}, // 1.0 (local)
	}
	s := &LeastLoaded{}
	for i := 0; i < 5; i++ {
		if got := eps[s.Select(Request{}, eps, opts(now))].ID; got != "b" {
			t.Fatalf("picked %s, want b", got)
		}
	}
	// Equal load: the choice rotates rather than piling onto the first endpoint.
	idle := []Endpoint{{ID: "a", Ready: true}, {ID: "b", Ready: true}, {ID: "c", Ready: true}}
	seen := map[string]int{}
	for i := 0; i < 30; i++ {
		seen[idle[s.Select(Request{}, idle, opts(now))].ID]++
	}
	if len(seen) != 3 {
		t.Fatalf("ties were not shared: %v", seen)
	}
	// A stale heartbeat's load is not believed; only local observation counts.
	stale := []Endpoint{
		{ID: "a", Ready: true, Heartbeat: hb(0, 0, 1), HeardAt: now.Add(-time.Hour), LocalInFlight: 3},
		{ID: "b", Ready: true, Heartbeat: hb(9, 9, 1), HeardAt: now.Add(-time.Hour)},
	}
	o := opts(now)
	o.HeartbeatsLive = false
	if got := stale[s.Select(Request{}, stale, o)].ID; got != "b" {
		t.Fatalf("picked %s, want b (its stale load must be ignored)", got)
	}
}

func TestRoundRobin(t *testing.T) {
	t.Parallel()
	eps := []Endpoint{{ID: "a"}, {ID: "b"}, {ID: "c"}}
	s := &RoundRobin{}
	var got string
	for i := 0; i < 6; i++ {
		got += eps[s.Select(Request{}, eps, opts(t0))].ID
	}
	if got != "abcabc" {
		t.Fatalf("order %s", got)
	}
}

func TestLatencyAware(t *testing.T) {
	t.Parallel()
	eps := []Endpoint{
		{ID: "slow", Ready: true, TTFTEWMA: 400},
		{ID: "fast", Ready: true, TTFTEWMA: 90},
	}
	s := &LatencyAware{}
	if got := eps[s.Select(Request{}, eps, opts(t0))].ID; got != "fast" {
		t.Fatalf("picked %s", got)
	}
	// An unmeasured endpoint is explored.
	eps = append(eps, Endpoint{ID: "new", Ready: true})
	if got := eps[s.Select(Request{}, eps, opts(t0))].ID; got != "new" {
		t.Fatalf("picked %s, want the unmeasured endpoint", got)
	}
	// The heartbeat's figure stands in for a local one.
	eps = []Endpoint{
		{ID: "a", Ready: true, Heartbeat: &Heartbeat{Accepting: true, State: "ready", TTFTMSEWMA: 50}, HeardAt: t0},
		{ID: "b", Ready: true, TTFTEWMA: 80},
	}
	if got := eps[s.Select(Request{}, eps, opts(t0))].ID; got != "a" {
		t.Fatalf("picked %s", got)
	}
}

func TestCapabilityBasedPrefersSmallestWindowThatFits(t *testing.T) {
	t.Parallel()
	eps := []Endpoint{
		{ID: "big", Ready: true, ContextWindow: 32768},
		{ID: "small", Ready: true, ContextWindow: 4096},
	}
	s := &CapabilityBased{}
	if got := eps[s.Select(Request{RequiredContext: 1000}, eps, opts(t0))].ID; got != "small" {
		t.Fatalf("picked %s", got)
	}
}

func TestNewResolvesPolicies(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		p        Policy
		want     string
		noteless bool
	}{
		{Policy{}, StrategyLeastLoaded, true},
		{Policy{Strategy: StrategyRoundRobin}, StrategyRoundRobin, true},
		{Policy{Strategy: StrategyLatencyAware}, StrategyLatencyAware, true},
		{Policy{Strategy: StrategyCapabilityBased}, StrategyCapabilityBased, true},
		{Policy{Strategy: StrategyFailover, Config: json.RawMessage(`{"primary":"round_robin","fallback_model":"b"}`)}, StrategyRoundRobin, true},
		{Policy{Strategy: StrategyCostAware}, StrategyLeastLoaded, false},
		{Policy{Strategy: "quantum"}, StrategyLeastLoaded, false},
	} {
		s, note := New(c.p)
		if s.Name() != c.want || (note == "") != c.noteless {
			t.Errorf("%+v: %s %q", c.p, s.Name(), note)
		}
	}
	p := Policy{Strategy: StrategyFailover, Config: json.RawMessage(`{"fallback_model":"backup"}`)}
	if p.Fallback() != "backup" || (Policy{Strategy: StrategyLeastLoaded}).Fallback() != "" {
		t.Fatal("fallback")
	}
}

func all(int) bool { return true }

// Weighted resolution within tolerance over 10 000 keys, and stable per key.
func TestPickWeightedDistributionAndStability(t *testing.T) {
	t.Parallel()
	weights := []int{50, 30, 20}
	counts := make([]int, 3)
	const n = 10000
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("req-%d", i)
		a := PickWeighted(weights, k, all)
		if b := PickWeighted(weights, k, all); a != b {
			t.Fatalf("key %s is not stable: %d then %d", k, a, b)
		}
		counts[a]++
	}
	for i, w := range weights {
		got := float64(counts[i]) / n * 100
		if math.Abs(got-float64(w)) > 2 {
			t.Errorf("target %d: %.1f%%, want %d%% ± 2", i, got, w)
		}
	}
}

// A target with no eligible endpoint gives its share to the others by weight; the
// keys that were already elsewhere do not move.
func TestPickWeightedShiftsTrafficFromAnUnusableTarget(t *testing.T) {
	t.Parallel()
	weights := []int{50, 30, 20}
	dead := func(i int) bool { return i != 0 }
	counts := make([]int, 3)
	const n = 10000
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("req-%d", i)
		before := PickWeighted(weights, k, all)
		after := PickWeighted(weights, k, dead)
		if after == 0 {
			t.Fatal("a key was sent to the unusable target")
		}
		if before != 0 && before != after {
			t.Fatalf("key %s moved from a healthy target (%d -> %d)", k, before, after)
		}
		counts[after]++
	}
	share1 := float64(counts[1]) / n
	if math.Abs(share1-0.6) > 0.03 {
		t.Errorf("target 1 now serves %.2f, want 0.60 (30 of the surviving 50)", share1)
	}
	if PickWeighted(weights, "x", func(int) bool { return false }) != -1 {
		t.Fatal("no usable target must be -1")
	}
	// Zero weight never absorbs someone else's traffic.
	if got := PickWeighted([]int{100, 0}, "x", func(i int) bool { return i == 1 }); got != -1 {
		t.Fatalf("a zero-weight target received traffic: %d", got)
	}
}

func TestBreaker(t *testing.T) {
	t.Parallel()
	b := NewBreaker(BreakerConfig{Threshold: 3, Cooldown: time.Second, MaxCooldown: 4 * time.Second})
	now := t0

	// Upstream failures open it at the threshold.
	b.Failure(now, false)
	b.Failure(now, false)
	if b.State(now) != BreakerClosed {
		t.Fatal("opened before the threshold")
	}
	b.Failure(now, false)
	if b.State(now) != BreakerOpen || b.Acquire(now) {
		t.Fatal("should be open")
	}

	// After the cooldown, exactly one trial is admitted.
	now = now.Add(time.Second)
	if b.State(now) != BreakerHalfOpen || !b.Acquire(now) {
		t.Fatal("should admit a trial")
	}
	if b.Acquire(now) || b.State(now) != BreakerOpen {
		t.Fatal("a second concurrent trial was admitted")
	}
	// The trial fails: open again, for twice as long.
	b.Failure(now, false)
	if b.State(now.Add(1500*time.Millisecond)) != BreakerOpen {
		t.Fatal("cooldown should have doubled")
	}
	now = now.Add(2 * time.Second)
	if !b.Acquire(now) {
		t.Fatal("second trial")
	}
	b.Success()
	if b.State(now) != BreakerClosed {
		t.Fatal("success must close it")
	}

	// A connection failure opens it at once.
	b.Failure(now, true)
	if b.State(now) != BreakerOpen {
		t.Fatal("unreachable must open immediately")
	}

	// A trial released without a verdict frees the probe.
	now = now.Add(time.Second)
	if !b.Acquire(now) {
		t.Fatal("trial")
	}
	b.Release()
	if !b.Acquire(now) {
		t.Fatal("released trial should allow another probe")
	}
}

func TestEWMA(t *testing.T) {
	t.Parallel()
	e := NewEWMA(0.5)
	if e.Value() != 0 {
		t.Fatal("empty")
	}
	e.Observe(100)
	e.Observe(200)
	if e.Value() != 150 {
		t.Fatalf("%v", e.Value())
	}
}

// A saturated endpoint is avoided while any other exists, and still chosen when it
// is the only one: saturation is "full", not "broken".
func TestSaturatedEndpointIsAvoided(t *testing.T) {
	t.Parallel()
	eps := []Endpoint{
		{ID: "a", Ready: true, Saturated: true},
		{ID: "b", Ready: true, LocalInFlight: 5},
	}
	for _, s := range []Strategy{&LeastLoaded{}, &LatencyAware{}} {
		if got := eps[s.Select(Request{}, eps, opts(t0))].ID; got != "b" {
			t.Errorf("%s picked the saturated endpoint", s.Name())
		}
	}
	only := []Endpoint{{ID: "a", Ready: true, Saturated: true}}
	if got := (&LeastLoaded{}).Select(Request{}, only, opts(t0)); got != 0 {
		t.Fatalf("%d", got)
	}
}
