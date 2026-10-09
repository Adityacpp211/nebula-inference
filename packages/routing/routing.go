// Package routing decides where one inference request goes (docs/architecture.md
// §6.1, ADR-0004).
//
// Two steps, kept apart because canaries and experiments depend on the separation:
//
//   - Resolution picks a route's weighted target — a deployment — from a bucketing
//     key, deterministically (PickWeighted).
//   - Selection picks one endpoint of that deployment: the mandatory Filter first,
//     then a Strategy over what survives.
//
// Everything here is a pure function of its inputs plus an injected clock. The
// package owns no goroutines, no network and no global state, so every decision it
// makes is reproducible in a table test. The gateway's router
// (services/gateway/internal/router) owns the live state and calls in here.
//
// No routing decision is made anywhere else. A handler that picks an endpoint
// itself is a bug, because it bypasses the filter.
package routing

import (
	"math"
	"sort"
	"time"
)

// Heartbeat is a worker's load report on nebula.worker.heartbeat.<deployment>.<pod>
// (docs/events.md §3.1). Latest-wins per pod, ordered by Sequence.
type Heartbeat struct {
	Pod            string `json:"pod"`
	Node           string `json:"node,omitempty"`
	DeploymentID   string `json:"deployment_id"`
	ModelVersionID string `json:"model_version_id,omitempty"`
	ModelVersion   string `json:"model_version"`
	Runtime        string `json:"runtime"`
	State          string `json:"state"`
	// Instance identifies the worker process; a restart in the same pod starts a
	// new instance, and so a new sequence.
	Instance            string  `json:"instance,omitempty"`
	Sequence            uint64  `json:"sequence"`
	EmittedAtMS         int64   `json:"emitted_at_ms"`
	InFlight            int     `json:"inflight"`
	QueueDepth          int     `json:"queue_depth"`
	QueueWaitMSEWMA     float64 `json:"queue_wait_ms_ewma"`
	ParallelSlots       int     `json:"parallel_slots"`
	SlotsBusy           int     `json:"slots_busy"`
	TTFTMSEWMA          float64 `json:"ttft_ms_ewma"`
	TokensPerSecondEWMA float64 `json:"tokens_per_second_ewma"`
	Accepting           bool    `json:"accepting"`
	ContextWindow       int     `json:"context_window,omitempty"`
	// AdvertiseURL is set by a worker outside Kubernetes, where no EndpointSlice
	// names it, so a static endpoint can be matched to its heartbeats.
	AdvertiseURL string `json:"advertise_url,omitempty"`
}

// Endpoint is one worker as selection sees it: a snapshot, never mutated by a
// strategy.
type Endpoint struct {
	// ID is stable for the endpoint's life: the pod name, or the URL of a static
	// endpoint.
	ID  string
	URL string
	// Ready is Kubernetes' readiness from the EndpointSlice (always true for a
	// static endpoint). Authoritative for existence; nothing overrides a false.
	Ready bool

	// Heartbeat is the latest report, or nil if none has arrived. HeardAt is the
	// gateway's own receive time, so staleness never subtracts two clocks.
	Heartbeat *Heartbeat
	HeardAt   time.Time

	// Breaker is this gateway's local verdict.
	Breaker BreakerState
	// LocalInFlight counts requests this gateway has in flight to the endpoint.
	LocalInFlight int
	// Saturated is set while the endpoint's last answer was "at capacity" (429)
	// and its Retry-After has not passed. A saturated endpoint stays eligible — it
	// is healthy — but every strategy prefers any other, and the admission queue
	// counts it as having no room.
	Saturated bool
	// TTFTEWMA is this gateway's own time-to-first-token average in ms; zero means
	// no sample yet.
	TTFTEWMA float64
	// ContextWindow of the model version the endpoint serves.
	ContextWindow int
}

// Load is the endpoint's occupancy per slot, the quantity LeastLoaded minimises.
//
// The heartbeat's in-flight plus queued count covers every gateway; this
// gateway's own in-flight count covers the second since the last heartbeat. The
// larger is used rather than the sum, because the heartbeat already includes some
// of this gateway's requests and double counting them would steer traffic away
// from exactly the replicas this gateway just used.
func (e *Endpoint) Load(fresh bool) float64 {
	if e.Saturated {
		return math.Inf(1)
	}
	slots := 1
	reported := 0
	if fresh && e.Heartbeat != nil {
		reported = e.Heartbeat.InFlight + e.Heartbeat.QueueDepth
		if e.Heartbeat.ParallelSlots > 0 {
			slots = e.Heartbeat.ParallelSlots
		}
	}
	n := reported
	if e.LocalInFlight > n {
		n = e.LocalInFlight
	}
	return float64(n) / float64(slots)
}

// Request is what selection knows about the request being placed.
type Request struct {
	// Key is the bucketing key: the request id by default.
	Key string
	// RequiredContext is prompt plus completion ceiling, in tokens (an upper bound).
	RequiredContext int
}

// FilterOptions parameterise the mandatory filter.
type FilterOptions struct {
	Now time.Time
	// StaleAfter is how old a heartbeat may be before the endpoint is dropped: three
	// heartbeat intervals by default (docs/architecture.md §6.2).
	StaleAfter time.Duration
	// HeartbeatsLive says whether the heartbeat channel itself is up. When it is
	// not, silence proves nothing about a worker, and routing degrades to readiness
	// plus local observation (docs/events.md §3.1) instead of dropping everything.
	HeartbeatsLive bool
	// ModelVersion is the version the target pins. An endpoint reporting another
	// one is mid-rollout or misrouted, and would answer 409.
	ModelVersion string
}

// Exclusion reasons, reported by Filter so an empty result is explainable.
const (
	ExcludedNotReady       = "not_ready"
	ExcludedStale          = "heartbeat_stale"
	ExcludedNotAccepting   = "not_accepting"
	ExcludedBreakerOpen    = "breaker_open"
	ExcludedVersion        = "model_version_mismatch"
	ExcludedContextWindow  = "context_window"
	ExcludedAlreadyTried   = "already_tried"
	ExcludedNoEndpoints    = "no_endpoints"
	ExcludedTargetDisabled = "weight_zero"
)

// Filter removes every endpoint that must not receive this request. It is not
// optional and not a strategy: every selection goes through it first.
//
// It returns the eligible endpoints and, for the rest, why each was excluded.
func Filter(eps []Endpoint, req Request, o FilterOptions) (eligible []Endpoint, excluded map[string]string) {
	excluded = map[string]string{}
	for _, e := range eps {
		if reason := exclude(&e, req, o); reason != "" {
			excluded[e.ID] = reason
			continue
		}
		eligible = append(eligible, e)
	}
	return eligible, excluded
}

func exclude(e *Endpoint, req Request, o FilterOptions) string {
	if !e.Ready {
		return ExcludedNotReady
	}
	if e.Breaker == BreakerOpen {
		return ExcludedBreakerOpen
	}
	if hb := e.Heartbeat; hb != nil {
		fresh := Fresh(e, o)
		// Staleness counts only while the channel is up: a quiet NATS is not a dead
		// worker. A worker that has never heartbeated is judged by readiness alone,
		// which is how a pod is served in its first second, and how workers run
		// without NATS at all.
		if o.HeartbeatsLive && !fresh {
			return ExcludedStale
		}
		if fresh {
			if !hb.Accepting || (hb.State != "" && hb.State != "ready") {
				return ExcludedNotAccepting
			}
			if o.ModelVersion != "" && hb.ModelVersion != "" && hb.ModelVersion != o.ModelVersion {
				return ExcludedVersion
			}
		}
	}
	if req.RequiredContext > 0 && e.ContextWindow > 0 && req.RequiredContext > e.ContextWindow {
		return ExcludedContextWindow
	}
	return ""
}

// Fresh reports whether an endpoint's heartbeat is recent enough to act on.
func Fresh(e *Endpoint, o FilterOptions) bool {
	if e.Heartbeat == nil {
		return false
	}
	stale := o.StaleAfter
	if stale <= 0 {
		stale = 3 * time.Second
	}
	return o.Now.Sub(e.HeardAt) <= stale
}

// Strategy picks one of the eligible endpoints. It is called only with a
// non-empty slice, already filtered, sorted by ID.
type Strategy interface {
	Name() string
	Select(req Request, eligible []Endpoint, o FilterOptions) int
}

// SortByID orders endpoints so a strategy's choice does not depend on map
// iteration order upstream.
func SortByID(eps []Endpoint) {
	sort.Slice(eps, func(i, j int) bool { return eps[i].ID < eps[j].ID })
}

// argmin returns the index of the smallest score, breaking ties by rotating from
// start so equal endpoints share load instead of the first one taking it all.
func argmin(n, start int, score func(int) float64) int {
	best, bestScore := start%n, math.Inf(1)
	for k := 0; k < n; k++ {
		i := (start + k) % n
		if s := score(i); s < bestScore {
			best, bestScore = i, s
		}
	}
	return best
}
