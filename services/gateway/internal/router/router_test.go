package router_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"

	"github.com/adityasatwar321/nebula/packages/routing"
	"github.com/adityasatwar321/nebula/packages/telemetry"
	"github.com/adityasatwar321/nebula/services/gateway/internal/gwtest"
	"github.com/adityasatwar321/nebula/services/gateway/internal/router"
	"github.com/adityasatwar321/nebula/services/gateway/internal/routes"
)

// clock is a settable time source; tests move it instead of sleeping.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) Add(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

const (
	depA = "0192f3c1-0000-7000-8000-00000000000a"
	depB = "0192f3c1-0000-7000-8000-00000000000b"
)

func payload(weightA, weightB int) *routes.Payload {
	return &routes.Payload{Version: fmt.Sprintf("v-%d-%d", weightA, weightB), Routes: []*routes.Route{{
		ID: "route-1", Model: "chat", Org: "acme", ContextWindow: 4096,
		Targets: []*routes.Target{
			{Deployment: "a", DeploymentID: depA, ModelVersion: "m:v1", Weight: weightA, Label: "baseline"},
			{Deployment: "b", DeploymentID: depB, ModelVersion: "m:v2", Weight: weightB, Label: "canary"},
		},
	}}}
}

func newRouter(t *testing.T, c *clock, live bool) (*router.Router, *routes.Route) {
	t.Helper()
	r := router.New(router.Options{Now: c.Now, StaleAfter: 3 * time.Second,
		HeartbeatsLive: func() bool { return live },
		Breaker:        routing.BreakerConfig{Threshold: 3, Cooldown: 2 * time.Second, MaxCooldown: 8 * time.Second}})
	tbl, err := routes.FromPayload(payload(50, 50), c.Now())
	if err != nil {
		t.Fatal(err)
	}
	r.SetTable(tbl)
	rt, _ := tbl.Lookup("acme", "chat")
	return r, rt
}

func pods(prefix string, n int, ready bool) []router.Discovered {
	out := make([]router.Discovered, n)
	for i := range out {
		out[i] = router.Discovered{ID: fmt.Sprintf("%s-%d", prefix, i), URL: fmt.Sprintf("http://%s-%d:8090", prefix, i), Ready: ready}
	}
	return out
}

func selectN(t *testing.T, r *router.Router, rt *routes.Route, n int) map[string]int {
	t.Helper()
	counts := map[string]int{}
	for i := 0; i < n; i++ {
		sel, err := r.Select(rt, router.Request{Key: fmt.Sprintf("req-%d", i)})
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		counts[sel.Target.Deployment]++
		sel.Done(router.Succeeded)
	}
	return counts
}

// The Phase 6 exit criterion in miniature: a 50/50 route splits evenly; when every
// endpoint of one target goes away, all of its traffic shifts to the other with no
// request failing; when they return, the split returns.
func TestTrafficShiftsWhenATargetLosesItsEndpoints(t *testing.T) {
	t.Parallel()
	c := &clock{t: time.Unix(1_800_000_000, 0)}
	r, rt := newRouter(t, c, false)
	r.SetEndpoints(depA, pods("a", 2, true))
	r.SetEndpoints(depB, pods("b", 2, true))

	counts := selectN(t, r, rt, 10000)
	if math.Abs(float64(counts["a"])/10000-0.5) > 0.02 {
		t.Fatalf("split %v, want 50/50 ± 2%%", counts)
	}

	// All of b's pods terminate: EndpointSlices mark them not ready.
	r.SetEndpoints(depB, pods("b", 2, false))
	if counts := selectN(t, r, rt, 1000); counts["a"] != 1000 {
		t.Fatalf("with b down: %v", counts)
	}
	// ...and then they are gone entirely.
	r.SetEndpoints(depB, nil)
	if counts := selectN(t, r, rt, 1000); counts["a"] != 1000 {
		t.Fatalf("with b gone: %v", counts)
	}
	// Replacements come up; the split comes back, and keys return to their target.
	r.SetEndpoints(depB, pods("b2", 2, true))
	counts = selectN(t, r, rt, 10000)
	if math.Abs(float64(counts["a"])/10000-0.5) > 0.02 {
		t.Fatalf("recovered split %v", counts)
	}

	// Nothing anywhere: a named error that says why.
	r.SetEndpoints(depA, nil)
	r.SetEndpoints(depB, nil)
	_, err := r.Select(rt, router.Request{Key: "x"})
	var none *router.NoEndpointError
	if !errors.As(err, &none) || none.Reasons[routing.ExcludedNoEndpoints] != 2 {
		t.Fatalf("want NoEndpointError with no_endpoints=2, got %v", err)
	}
}

// A heartbeat that stops removes the endpoint exactly at the staleness window
// while the channel is live, and a new heartbeat brings it back.
func TestStaleHeartbeatRemovesEndpointWithinWindow(t *testing.T) {
	t.Parallel()
	c := &clock{t: time.Unix(1_800_000_000, 0)}
	r, rt := newRouter(t, c, true)
	r.SetEndpoints(depA, pods("a", 1, true))
	pin := router.Request{Key: "k", Pin: "a"}

	hb := routing.Heartbeat{Pod: "a-0", DeploymentID: depA, ModelVersion: "m:v1", State: "ready", Accepting: true,
		Instance: "i1", Sequence: 1, ParallelSlots: 4}
	if !r.ObserveHeartbeat(hb) {
		t.Fatal("first heartbeat not applied")
	}
	c.Add(3 * time.Second)
	sel, err := r.Select(rt, pin)
	if err != nil {
		t.Fatalf("at the window's edge the endpoint is still eligible: %v", err)
	}
	sel.Done(router.Succeeded)

	c.Add(time.Millisecond)
	_, err = r.Select(rt, pin)
	var none *router.NoEndpointError
	if !errors.As(err, &none) || none.Reasons[routing.ExcludedStale] != 1 {
		t.Fatalf("past the window: %v", err)
	}

	hb.Sequence = 2
	r.ObserveHeartbeat(hb)
	if _, err := r.Select(rt, pin); err != nil {
		t.Fatalf("a fresh heartbeat restores it: %v", err)
	}
}

// Silence proves nothing when the heartbeat channel itself is down.
func TestHeartbeatsDownFallsBackToReadiness(t *testing.T) {
	t.Parallel()
	c := &clock{t: time.Unix(1_800_000_000, 0)}
	r, rt := newRouter(t, c, false)
	r.SetEndpoints(depA, pods("a", 1, true))
	r.ObserveHeartbeat(routing.Heartbeat{Pod: "a-0", State: "ready", Accepting: true, Sequence: 1})
	c.Add(time.Hour)
	if _, err := r.Select(rt, router.Request{Key: "k", Pin: "a"}); err != nil {
		t.Fatalf("a stale heartbeat must not exclude when heartbeats are down: %v", err)
	}
}

func TestHeartbeatOrdering(t *testing.T) {
	t.Parallel()
	c := &clock{t: time.Unix(1_800_000_000, 0)}
	r, _ := newRouter(t, c, true)
	base := routing.Heartbeat{Pod: "p", Instance: "one", Sequence: 10}
	if !r.ObserveHeartbeat(base) {
		t.Fatal("first")
	}
	old := base
	old.Sequence = 9
	if r.ObserveHeartbeat(old) {
		t.Fatal("an out-of-order heartbeat was applied")
	}
	if r.ObserveHeartbeat(base) {
		t.Fatal("a duplicate was applied")
	}
	restarted := routing.Heartbeat{Pod: "p", Instance: "two", Sequence: 1}
	if !r.ObserveHeartbeat(restarted) {
		t.Fatal("a restarted worker's first heartbeat must replace the old process's")
	}
}

// LeastLoaded reads the heartbeat's load and this gateway's own in-flight count.
func TestLeastLoadedUsesHeartbeatsAndLocalInFlight(t *testing.T) {
	t.Parallel()
	c := &clock{t: time.Unix(1_800_000_000, 0)}
	r, rt := newRouter(t, c, true)
	r.SetEndpoints(depA, pods("a", 2, true))
	r.ObserveHeartbeat(routing.Heartbeat{Pod: "a-0", State: "ready", Accepting: true, Sequence: 1, InFlight: 3, ParallelSlots: 4})
	r.ObserveHeartbeat(routing.Heartbeat{Pod: "a-1", State: "ready", Accepting: true, Sequence: 1, InFlight: 0, ParallelSlots: 4})

	// a-1 is idle, so it takes requests until its local in-flight count passes
	// a-0's reported load.
	var held []*router.Selection
	for i := 0; i < 3; i++ {
		sel, err := r.Select(rt, router.Request{Key: fmt.Sprint(i), Pin: "a"})
		if err != nil {
			t.Fatal(err)
		}
		if sel.EndpointID != "a-1" {
			t.Fatalf("request %d went to %s", i, sel.EndpointID)
		}
		held = append(held, sel)
	}
	for _, s := range held {
		s.Done(router.Succeeded)
	}
}

// A connection failure is believed at once; the endpoint is probed again after
// the cooldown, by one request.
func TestBreakerExcludesAnUnreachableEndpoint(t *testing.T) {
	t.Parallel()
	c := &clock{t: time.Unix(1_800_000_000, 0)}
	r, rt := newRouter(t, c, false)
	r.SetEndpoints(depA, pods("a", 2, true))
	req := router.Request{Key: "k", Pin: "a"}

	first, _ := r.Select(rt, req)
	first.Done(router.Unreachable)
	for i := 0; i < 20; i++ {
		sel, err := r.Select(rt, req)
		if err != nil || sel.EndpointID == first.EndpointID {
			t.Fatalf("the unreachable endpoint was selected again (%v)", err)
		}
		sel.Done(router.Succeeded)
	}

	// Refreshing endpoints keeps the verdict: the breaker belongs to the pod.
	r.SetEndpoints(depA, pods("a", 2, true))
	if sel, _ := r.Select(rt, req); sel.EndpointID == first.EndpointID {
		t.Fatal("an endpoint refresh reset the breaker")
	}

	c.Add(2 * time.Second)
	probe := 0
	var sels []*router.Selection
	for i := 0; i < 10; i++ {
		sel, err := r.Select(rt, router.Request{Key: fmt.Sprint(i), Pin: "a"})
		if err != nil {
			t.Fatal(err)
		}
		if sel.EndpointID == first.EndpointID {
			probe++
		}
		sels = append(sels, sel)
	}
	if probe != 1 {
		t.Fatalf("%d requests probed the recovering endpoint, want exactly 1", probe)
	}
	for _, s := range sels {
		s.Done(router.Succeeded)
	}
}

func TestSelectExcludesTriedEndpoints(t *testing.T) {
	t.Parallel()
	c := &clock{t: time.Unix(1_800_000_000, 0)}
	r, rt := newRouter(t, c, false)
	r.SetEndpoints(depA, pods("a", 2, true))
	req := router.Request{Key: "k", Pin: "a", Exclude: map[string]bool{"a-0": true}}
	sel, err := r.Select(rt, req)
	if err != nil || sel.EndpointID != "a-1" {
		t.Fatalf("%v %v", sel, err)
	}
	req.Exclude["a-1"] = true
	if _, err := r.Select(rt, req); err == nil {
		t.Fatal("every endpoint tried must be an error")
	}
}

func ptr[T any](v T) *T { return &v }

func TestMergeEndpointSlices(t *testing.T) {
	t.Parallel()
	slice := &discoveryv1.EndpointSlice{
		AddressType: discoveryv1.AddressTypeIPv4,
		Ports:       []discoveryv1.EndpointPort{{Name: ptr("http"), Port: ptr(int32(8090))}},
		Endpoints: []discoveryv1.Endpoint{
			{Addresses: []string{"10.0.0.1"}, TargetRef: &corev1.ObjectReference{Name: "pod-a"},
				Conditions: discoveryv1.EndpointConditions{Ready: ptr(true)}},
			{Addresses: []string{"10.0.0.2"}, TargetRef: &corev1.ObjectReference{Name: "pod-b"},
				Conditions: discoveryv1.EndpointConditions{Ready: ptr(false)}},
			// Terminating but still serving: no new work.
			{Addresses: []string{"10.0.0.3"}, TargetRef: &corev1.ObjectReference{Name: "pod-c"},
				Conditions: discoveryv1.EndpointConditions{Ready: ptr(true), Terminating: ptr(true)}},
		},
	}
	got := router.Merge([]*discoveryv1.EndpointSlice{slice, slice})
	if len(got) != 3 {
		t.Fatalf("%+v", got)
	}
	want := map[string]router.Discovered{
		"pod-a": {ID: "pod-a", URL: "http://10.0.0.1:8090", Ready: true},
		"pod-b": {ID: "pod-b", URL: "http://10.0.0.2:8090", Ready: false},
		"pod-c": {ID: "pod-c", URL: "http://10.0.0.3:8090", Ready: false},
	}
	for _, d := range got {
		if want[d.ID] != d {
			t.Errorf("%+v, want %+v", d, want[d.ID])
		}
	}
}

// Axiom A8, tested: a gateway that starts while the control plane is down routes
// from the snapshot another replica left in Redis.
func TestColdStartWithControlPlaneDownRoutesFromSnapshot(t *testing.T) {
	t.Parallel()
	mr := gwtest.Miniredis(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	store := router.RedisSnapshots{Client: rdb, Key: "nebula:routing:snapshot", OpTimeout: time.Second}
	ctx := context.Background()
	body, _ := json.Marshal(payload(100, 0))

	// A healthy replica: fetches the table, discovers endpoints, snapshots both.
	warm := router.New(router.Options{})
	src := &router.TableSource{Router: warm, Snapshots: store, Logger: telemetry.Discard(), Interval: time.Second,
		Fetch: func(context.Context, string) ([]byte, string, error) { return body, `"v1"`, nil }}
	warm.SetEndpoints(depA, pods("a", 2, true))
	if err := src.Poll(ctx); err != nil {
		t.Fatal(err)
	}

	// A new replica with the control plane down.
	cold := router.New(router.Options{})
	down := errors.New("connection refused")
	coldSrc := &router.TableSource{Router: cold, Snapshots: store, Logger: telemetry.Discard(), Interval: time.Hour,
		Fetch: func(context.Context, string) ([]byte, string, error) { return nil, "", down }}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { coldSrc.Run(runCtx); close(done) }()
	deadline := time.Now().Add(5 * time.Second)
	for !coldSrc.Synced.Load() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
	if !coldSrc.FromSnapshot.Load() {
		t.Fatal("the cold replica did not load the snapshot")
	}
	rt, ok := cold.Table().Lookup("acme", "chat")
	if !ok {
		t.Fatal("the route is missing after the snapshot")
	}
	sel, err := cold.Select(rt, router.Request{Key: "first-request"})
	if err != nil {
		t.Fatalf("the cold replica cannot route: %v", err)
	}
	if !strings.HasPrefix(sel.EndpointID, "a-") {
		t.Fatalf("selected %s", sel.EndpointID)
	}
	sel.Done(router.Succeeded)
}

// Heartbeats over a real (embedded) NATS server reach the router, and the channel
// only counts as live once it has been up for the settle period.
func TestHeartbeatsOverNATS(t *testing.T) {
	t.Parallel()
	ns, err := natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(30 * time.Second) {
		t.Fatal("nats not ready")
	}
	t.Cleanup(ns.Shutdown)

	r := router.New(router.Options{})
	revoked := make(chan string, 1)
	hbs := &router.Heartbeats{URL: ns.ClientURL(), Router: r, Logger: telemetry.Discard(), Settle: 50 * time.Millisecond,
		Also: map[string]func([]byte){"nebula.gateway.credentials.revoked": func(b []byte) { revoked <- string(b) }}}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = hbs.Run(ctx) }()

	pub, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pub.Close)
	deadline := time.Now().Add(5 * time.Second)
	for seq := uint64(1); hbs.Applied() == 0 && time.Now().Before(deadline); seq++ {
		b, _ := json.Marshal(routing.Heartbeat{Pod: "pod-1", DeploymentID: depA, Instance: "i", Sequence: seq,
			State: "ready", Accepting: true})
		_ = pub.Publish("nebula.worker.heartbeat."+depA+".pod-1", b)
		time.Sleep(10 * time.Millisecond)
	}
	if hbs.Applied() == 0 {
		t.Fatal("no heartbeat reached the router")
	}
	for !hbs.Live() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !hbs.Live() {
		t.Fatal("the channel never became live")
	}

	// The same connection carries the revocation broadcast between replicas.
	if err := hbs.Publish("nebula.gateway.credentials.revoked", []byte("nbk_abc1234")); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-revoked:
		if got != "nbk_abc1234" {
			t.Fatalf("got %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the broadcast did not arrive")
	}
}
