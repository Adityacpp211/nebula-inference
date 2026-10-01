// Package router is the gateway's live routing state and the only caller of
// packages/routing (docs/architecture.md §6.2, ADR-0004).
//
// It merges three sources, in the documented priority order:
//
//  1. endpoint existence and readiness — Kubernetes EndpointSlices, or a static
//     route file's endpoints;
//  2. worker heartbeats over core NATS — load, acceptance, loaded version;
//  3. local observation — every dispatch's outcome and time to first token,
//     believed immediately through a per-endpoint breaker.
//
// and resolves one request at a time: a route's weighted target (skipping targets
// with nothing eligible), then an endpoint of it through the mandatory filter and
// the route's strategy. The state lives here; every decision is made by
// packages/routing, which is pure.
//
// Routing state is per gateway replica. Weighted splits are therefore exact only
// in aggregate, and load figures are this replica's view plus the last heartbeat
// (docs/components.md §2.1).
package router

import (
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adityasatwar321/nebula/packages/routing"
	"github.com/adityasatwar321/nebula/services/gateway/internal/routes"
)

// Options configure a Router.
type Options struct {
	// Now is the clock. Injected so tests move time instead of sleeping.
	Now func() time.Time
	// StaleAfter drops an endpoint whose last heartbeat is older, while heartbeats
	// are flowing.
	StaleAfter time.Duration
	Breaker    routing.BreakerConfig
	// HeartbeatsLive reports whether the heartbeat channel is up. Nil means no
	// channel is configured, which is "not live": silence then proves nothing.
	HeartbeatsLive func() bool
	Logger         *slog.Logger
}

// Discovered is one endpoint as a discovery source reports it.
type Discovered struct {
	// ID is the pod name, or the URL for a static endpoint.
	ID    string `json:"id"`
	URL   string `json:"url"`
	Ready bool   `json:"ready"`
}

// Router holds routing state for one gateway replica. Safe for concurrent use.
type Router struct {
	o Options

	table   atomic.Pointer[routes.Table]
	payload atomic.Pointer[[]byte] // the control plane's table as received, for snapshots

	mu         sync.RWMutex
	deps       map[string]*deployment
	heartbeats map[string]*heard // by pod
	byURL      map[string]string // advertise URL -> pod
	strategies map[string]routing.Strategy
	lastPrune  time.Time
	notes      map[string]bool // strategy resolution notes already logged
}

type deployment struct {
	endpoints map[string]*endpoint
	static    bool
}

type endpoint struct {
	id, url  string
	ready    bool
	breaker  *routing.Breaker
	inflight atomic.Int64
	ttft     *routing.EWMA
}

type heard struct {
	hb routing.Heartbeat
	at time.Time
}

// New builds an empty router.
func New(o Options) *Router {
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.StaleAfter <= 0 {
		o.StaleAfter = 3 * time.Second
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	r := &Router{
		o: o, deps: map[string]*deployment{}, heartbeats: map[string]*heard{}, byURL: map[string]string{},
		strategies: map[string]routing.Strategy{}, notes: map[string]bool{},
	}
	r.table.Store(routes.Empty())
	return r
}

// Table is the current route table.
func (r *Router) Table() *routes.Table { return r.table.Load() }

// SetTable installs a route table. Endpoints written into a static table are
// registered here; discovered endpoints are untouched, so a table refresh never
// drops a replica Kubernetes still reports.
func (r *Router) SetTable(t *routes.Table) {
	r.mu.Lock()
	defer r.mu.Unlock()
	live := map[string]bool{}
	for _, rt := range t.All() {
		for _, tg := range rt.Targets {
			if len(tg.Endpoints) == 0 {
				continue
			}
			key := tg.Key(rt)
			live[key] = true
			eps := make([]Discovered, 0, len(tg.Endpoints))
			for _, u := range tg.Endpoints {
				eps = append(eps, Discovered{ID: u, URL: u, Ready: true})
			}
			r.setLocked(key, eps, true)
		}
	}
	for key, d := range r.deps {
		if d.static && !live[key] {
			delete(r.deps, key)
		}
	}
	// Strategies belong to (route, deployment) pairs; a policy change rebuilds them.
	r.strategies = map[string]routing.Strategy{}
	r.table.Store(t)
}

// SetPayload records the control plane's table as received, for snapshots.
func (r *Router) SetPayload(b []byte) { r.payload.Store(&b) }

// SetEndpoints replaces one deployment's discovered endpoints. Endpoints that
// survive keep their breaker, latency average and in-flight count.
func (r *Router) SetEndpoints(key string, eps []Discovered) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(eps) == 0 {
		delete(r.deps, strings.ToLower(key))
		return
	}
	r.setLocked(strings.ToLower(key), eps, false)
}

func (r *Router) setLocked(key string, eps []Discovered, static bool) {
	d := r.deps[key]
	if d == nil {
		d = &deployment{endpoints: map[string]*endpoint{}}
		r.deps[key] = d
	}
	d.static = static
	next := make(map[string]*endpoint, len(eps))
	for _, e := range eps {
		ep := d.endpoints[e.ID]
		if ep == nil {
			ep = &endpoint{id: e.ID, breaker: routing.NewBreaker(r.o.Breaker), ttft: routing.NewEWMA(0.2)}
		}
		ep.url, ep.ready = e.URL, e.Ready
		next[e.ID] = ep
	}
	d.endpoints = next
}

// Endpoints lists what is known of every deployment, for snapshots and debugging.
func (r *Router) Endpoints() map[string][]Discovered {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := map[string][]Discovered{}
	for key, d := range r.deps {
		if d.static {
			continue
		}
		for _, id := range routing.SortedKeys(d.endpoints) {
			ep := d.endpoints[id]
			out[key] = append(out[key], Discovered{ID: ep.id, URL: ep.url, Ready: ep.ready})
		}
	}
	return out
}

// ObserveHeartbeat records a worker heartbeat. Out-of-order heartbeats from the
// same worker process are discarded (docs/events.md §3.1); a new process in the
// same pod (a container restart) starts a new sequence and replaces the old one.
// It reports whether the heartbeat was applied.
func (r *Router) ObserveHeartbeat(hb routing.Heartbeat) bool {
	if hb.Pod == "" {
		return false
	}
	now := r.o.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	if prev, ok := r.heartbeats[hb.Pod]; ok && prev.hb.Instance == hb.Instance && hb.Sequence <= prev.hb.Sequence {
		return false
	}
	r.heartbeats[hb.Pod] = &heard{hb: hb, at: now}
	if hb.AdvertiseURL != "" {
		r.byURL[strings.TrimSuffix(hb.AdvertiseURL, "/")] = hb.Pod
	}
	// Pods come and go; forget heartbeats nobody has sent for a long while.
	if now.Sub(r.lastPrune) > 30*time.Second {
		r.lastPrune = now
		for pod, h := range r.heartbeats {
			if now.Sub(h.at) > 20*r.o.StaleAfter {
				delete(r.heartbeats, pod)
				if h.hb.AdvertiseURL != "" {
					delete(r.byURL, strings.TrimSuffix(h.hb.AdvertiseURL, "/"))
				}
			}
		}
	}
	return true
}

func (r *Router) live() bool { return r.o.HeartbeatsLive != nil && r.o.HeartbeatsLive() }

// snapshotLocked builds the selection view of one deployment.
func (r *Router) snapshotLocked(d *deployment, contextWindow int) []routing.Endpoint {
	if d == nil {
		return nil
	}
	now := r.o.Now()
	out := make([]routing.Endpoint, 0, len(d.endpoints))
	for _, ep := range d.endpoints {
		e := routing.Endpoint{
			ID: ep.id, URL: ep.url, Ready: ep.ready,
			Breaker:       ep.breaker.State(now),
			LocalInFlight: int(ep.inflight.Load()),
			TTFTEWMA:      ep.ttft.Value(),
			ContextWindow: contextWindow,
		}
		h := r.heartbeats[ep.id]
		if h == nil {
			if pod, ok := r.byURL[ep.url]; ok {
				h = r.heartbeats[pod]
			}
		}
		if h != nil {
			hb := h.hb
			e.Heartbeat, e.HeardAt = &hb, h.at
		}
		out = append(out, e)
	}
	return out
}

// Request is one request to place.
type Request struct {
	// Key is the bucketing key (the request id by default).
	Key string
	// Pin is a deployment name or id (nebula.deployment_id); it bypasses weighting.
	Pin string
	// RequiredContext is the request's token upper bound.
	RequiredContext int
	// Exclude lists endpoint ids already tried for this request.
	Exclude map[string]bool
}

// NoEndpointError means nothing can serve the request now. Reasons counts why
// endpoints were excluded, so the log line explains the 503.
type NoEndpointError struct {
	Route   string
	Reasons map[string]int
}

func (e *NoEndpointError) Error() string {
	if len(e.Reasons) == 0 {
		return fmt.Sprintf("route %s has no endpoints", e.Route)
	}
	parts := make([]string, 0, len(e.Reasons))
	for _, k := range routing.SortedKeys(e.Reasons) {
		parts = append(parts, fmt.Sprintf("%s=%d", k, e.Reasons[k]))
	}
	return fmt.Sprintf("route %s has no eligible endpoint (%s)", e.Route, strings.Join(parts, " "))
}

// ErrNoTarget is re-exported for callers that only import the router.
var ErrNoTarget = routes.ErrNoTarget

// Select resolves a route's target and picks an endpoint. The returned Selection
// must be finished with Done exactly once.
func (r *Router) Select(route *routes.Route, req Request) (*Selection, error) {
	targets := route.Targets
	weights := make([]int, len(targets))
	for i, t := range targets {
		weights[i] = t.Weight
	}
	if req.Pin != "" {
		t, err := route.Pinned(req.Pin)
		if err != nil {
			return nil, err
		}
		targets, weights = []*routes.Target{t}, []int{100}
	}

	now := r.o.Now()
	fo := routing.FilterOptions{Now: now, StaleAfter: r.o.StaleAfter, HeartbeatsLive: r.live()}
	rq := routing.Request{Key: req.Key, RequiredContext: req.RequiredContext}
	reasons := map[string]int{}

	r.mu.RLock()
	eligible := make([][]routing.Endpoint, len(targets))
	for i, t := range targets {
		d := r.deps[t.Key(route)]
		eps := r.snapshotLocked(d, route.TargetContext(t))
		if len(eps) == 0 {
			reasons[routing.ExcludedNoEndpoints]++
			continue
		}
		o := fo
		o.ModelVersion = t.ModelVersion
		ok, excluded := routing.Filter(eps, rq, o)
		for _, why := range excluded {
			reasons[why]++
		}
		for _, e := range ok {
			if req.Exclude[e.ID] {
				reasons[routing.ExcludedAlreadyTried]++
				continue
			}
			eligible[i] = append(eligible[i], e)
		}
		routing.SortByID(eligible[i])
	}
	r.mu.RUnlock()

	idx := routing.PickWeighted(weights, req.Key, func(i int) bool { return len(eligible[i]) > 0 })
	if idx < 0 {
		return nil, &NoEndpointError{Route: route.Model, Reasons: reasons}
	}
	target := targets[idx]
	key := target.Key(route)
	strategy := r.strategy(route, key)
	o := fo
	o.ModelVersion = target.ModelVersion

	cands := eligible[idx]
	for len(cands) > 0 {
		j := strategy.Select(rq, cands, o)
		r.mu.RLock()
		var ep *endpoint
		if d := r.deps[key]; d != nil {
			ep = d.endpoints[cands[j].ID]
		}
		r.mu.RUnlock()
		// Acquire claims a half-open breaker's single probe; losing that race, or the
		// endpoint vanishing since the snapshot, moves on to the next candidate.
		if ep != nil && ep.breaker.Acquire(now) {
			ep.inflight.Add(1)
			return &Selection{Route: route, Target: target, URL: ep.url, EndpointID: ep.id,
				Strategy: strategy.Name(), ep: ep, r: r}, nil
		}
		cands = append(cands[:j:j], cands[j+1:]...)
	}
	reasons[routing.ExcludedBreakerOpen]++
	return nil, &NoEndpointError{Route: route.Model, Reasons: reasons}
}

// strategy returns the (route, deployment) pair's strategy instance.
func (r *Router) strategy(route *routes.Route, depKey string) routing.Strategy {
	id := route.ID + "/" + depKey
	r.mu.RLock()
	s := r.strategies[id]
	r.mu.RUnlock()
	if s != nil {
		return s
	}
	s, note := routing.New(route.Policy)
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing := r.strategies[id]; existing != nil {
		return existing
	}
	r.strategies[id] = s
	if note != "" && !r.notes[route.ID+note] {
		r.notes[route.ID+note] = true
		r.o.Logger.Warn("routing policy resolved to a fallback strategy",
			slog.String("route", route.Model), slog.String("note", note))
	}
	return s
}

// Outcome is how a dispatch ended, as far as endpoint health is concerned.
type Outcome int

// Outcomes.
const (
	// Succeeded: the worker answered and served the request.
	Succeeded Outcome = iota
	// Unreachable: no connection could be made. Opens the breaker at once.
	Unreachable
	// Failed: the worker failed the request (a 5xx, a broken stream).
	Failed
	// Refused: the worker declined before doing any work (saturated, draining,
	// loading). Not a health failure; the next heartbeat or readiness change says
	// the same thing more precisely.
	Refused
	// Abandoned: no verdict (the client left first).
	Abandoned
)

// Selection is one endpoint chosen for one attempt.
type Selection struct {
	Route      *routes.Route
	Target     *routes.Target
	URL        string
	EndpointID string
	Strategy   string

	ep   *endpoint
	r    *Router
	done atomic.Bool
}

// FirstToken records the time to first token this gateway observed.
func (s *Selection) FirstToken(d time.Duration) {
	if s.ep != nil && d > 0 {
		s.ep.ttft.Observe(float64(d) / float64(time.Millisecond))
	}
}

// Done ends the attempt. Only the first call counts.
func (s *Selection) Done(o Outcome) {
	if s.ep == nil || !s.done.CompareAndSwap(false, true) {
		return
	}
	s.ep.inflight.Add(-1)
	now := s.r.o.Now()
	switch o {
	case Succeeded:
		s.ep.breaker.Success()
	case Unreachable:
		s.ep.breaker.Failure(now, true)
	case Failed:
		s.ep.breaker.Failure(now, false)
	case Refused, Abandoned:
		s.ep.breaker.Release()
	}
}

// ---------------------------------------------------------------------------
// snapshots
// ---------------------------------------------------------------------------

// Snapshot is the routing state a cold-starting gateway loads so its first request
// is not served blind (docs/architecture.md §6.2).
type Snapshot struct {
	SavedAt   time.Time               `json:"saved_at"`
	Table     []byte                  `json:"table"`
	Endpoints map[string][]Discovered `json:"endpoints"`
}

// ErrNoSnapshot means there is nothing to snapshot yet.
var ErrNoSnapshot = errors.New("no routing table has been received yet")

// Snapshot captures the current state.
func (r *Router) Snapshot() (Snapshot, error) {
	p := r.payload.Load()
	if p == nil {
		return Snapshot{}, ErrNoSnapshot
	}
	return Snapshot{SavedAt: r.o.Now().UTC(), Table: *p, Endpoints: r.Endpoints()}, nil
}

// Restore installs a snapshot's endpoints. The table is installed by the caller,
// which decodes it the same way as a live one.
func (r *Router) Restore(s Snapshot) {
	keys := make([]string, 0, len(s.Endpoints))
	for k := range s.Endpoints {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		r.SetEndpoints(k, s.Endpoints[k])
	}
}

// Eligible counts a target's endpoints that would pass the filter now, for
// /v1/models and health output.
func (r *Router) Eligible(route *routes.Route, t *routes.Target) int {
	fo := routing.FilterOptions{Now: r.o.Now(), StaleAfter: r.o.StaleAfter, HeartbeatsLive: r.live(),
		ModelVersion: t.ModelVersion}
	r.mu.RLock()
	eps := r.snapshotLocked(r.deps[t.Key(route)], route.TargetContext(t))
	r.mu.RUnlock()
	ok, _ := routing.Filter(eps, routing.Request{}, fo)
	return len(ok)
}
