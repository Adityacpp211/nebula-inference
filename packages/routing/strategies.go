package routing

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"
	"sync/atomic"
)

// Strategy names, as stored in routing_policies.strategy. Text plus this registry
// rather than an enum, so a new strategy needs no migration (docs/data-model.md §5).
const (
	StrategyRoundRobin      = "round_robin"
	StrategyLeastLoaded     = "least_loaded"
	StrategyLatencyAware    = "latency_aware"
	StrategyCapabilityBased = "capability_based"
	StrategyFailover        = "failover"
	StrategyCostAware       = "cost_aware"

	// DefaultStrategy is what a route with no policy uses.
	DefaultStrategy = StrategyLeastLoaded
)

// Policy is a routing_policies row as the router needs it.
type Policy struct {
	Strategy string          `json:"strategy"`
	Config   json.RawMessage `json:"config,omitempty"`
}

// failoverConfig is the config of a failover policy.
type failoverConfig struct {
	// Primary is the strategy used while the route has eligible endpoints.
	Primary string `json:"primary"`
	// FallbackModel is the route, in the same organization, that serves when this
	// one has no eligible endpoint at all.
	FallbackModel string `json:"fallback_model"`
}

// Fallback returns the route a failover policy falls back to, or "".
func (p Policy) Fallback() string {
	if p.Strategy != StrategyFailover {
		return ""
	}
	var c failoverConfig
	_ = json.Unmarshal(p.Config, &c)
	return c.FallbackModel
}

// New builds a fresh strategy instance for a policy. Instances hold per-deployment
// state (a round-robin cursor), so the router keeps one per deployment.
//
// A strategy this build does not implement resolves to the default and says so in
// the returned note, rather than failing every request of a route whose policy row
// is ahead of the code.
func New(p Policy) (s Strategy, note string) {
	switch p.Strategy {
	case "", StrategyLeastLoaded:
		return &LeastLoaded{}, ""
	case StrategyRoundRobin:
		return &RoundRobin{}, ""
	case StrategyLatencyAware:
		return &LatencyAware{}, ""
	case StrategyCapabilityBased:
		return &CapabilityBased{}, ""
	case StrategyFailover:
		var c failoverConfig
		if len(p.Config) > 0 {
			if err := json.Unmarshal(p.Config, &c); err != nil {
				return &LeastLoaded{}, fmt.Sprintf("failover config unreadable (%v); using %s", err, DefaultStrategy)
			}
		}
		if c.Primary == StrategyFailover {
			return &LeastLoaded{}, "failover cannot wrap itself; using " + DefaultStrategy
		}
		primary, note := New(Policy{Strategy: c.Primary})
		return primary, note
	case StrategyCostAware:
		// Needs pricing_profiles, which arrive with the cost engine (Phase 13).
		return &LeastLoaded{}, "cost_aware needs the Phase 13 cost engine; using " + DefaultStrategy
	}
	return &LeastLoaded{}, fmt.Sprintf("unknown strategy %q; using %s", p.Strategy, DefaultStrategy)
}

// RoundRobin cycles through eligible endpoints. The baseline, and the control arm
// when another strategy is benchmarked.
type RoundRobin struct{ next atomic.Uint64 }

// Name implements Strategy.
func (*RoundRobin) Name() string { return StrategyRoundRobin }

// Select implements Strategy.
func (s *RoundRobin) Select(_ Request, eligible []Endpoint, _ FilterOptions) int {
	return int((s.next.Add(1) - 1) % uint64(len(eligible)))
}

// LeastLoaded picks the endpoint with the fewest requests per slot. The default:
// inference request cost varies by orders of magnitude, so an even request count
// is an uneven load.
type LeastLoaded struct{ next atomic.Uint64 }

// Name implements Strategy.
func (*LeastLoaded) Name() string { return StrategyLeastLoaded }

// Select implements Strategy.
func (s *LeastLoaded) Select(_ Request, eligible []Endpoint, o FilterOptions) int {
	start := int(s.next.Add(1) % uint64(len(eligible)))
	return argmin(len(eligible), start, func(i int) float64 {
		return eligible[i].Load(Fresh(&eligible[i], o))
	})
}

// LatencyAware prefers the endpoint with the lowest time to first token, scaled
// by its load so the fastest replica is not simply buried. For heterogeneous
// hardware.
//
// An endpoint with no sample scores zero and is tried, which is how a new replica
// earns a measurement at all.
type LatencyAware struct{ next atomic.Uint64 }

// Name implements Strategy.
func (*LatencyAware) Name() string { return StrategyLatencyAware }

// Select implements Strategy.
func (s *LatencyAware) Select(_ Request, eligible []Endpoint, o FilterOptions) int {
	start := int(s.next.Add(1) % uint64(len(eligible)))
	return argmin(len(eligible), start, func(i int) float64 {
		e := &eligible[i]
		fresh := Fresh(e, o)
		ttft := e.TTFTEWMA
		if ttft == 0 && fresh {
			ttft = e.Heartbeat.TTFTMSEWMA
		}
		return ttft * (1 + e.Load(fresh))
	})
}

// CapabilityBased serves long-context requests only from endpoints that can hold
// them — which the Filter already enforces for every strategy — and prefers the
// smallest window that fits, keeping large-context replicas free for the requests
// only they can serve. Ties go to the least loaded.
type CapabilityBased struct{ next atomic.Uint64 }

// Name implements Strategy.
func (*CapabilityBased) Name() string { return StrategyCapabilityBased }

// Select implements Strategy.
func (s *CapabilityBased) Select(_ Request, eligible []Endpoint, o FilterOptions) int {
	start := int(s.next.Add(1) % uint64(len(eligible)))
	return argmin(len(eligible), start, func(i int) float64 {
		e := &eligible[i]
		// Window dominates; load breaks ties within one window size.
		return float64(e.ContextWindow)*1e3 + e.Load(Fresh(e, o))
	})
}

// ---------------------------------------------------------------------------
// resolution: weighted targets
// ---------------------------------------------------------------------------

// Bucket maps a key onto [0, 100). FNV-1a rather than a cryptographic hash: the
// distribution has to be uniform, not unpredictable, and it runs per request.
func Bucket(key string) int {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum64() % 100)
}

// PickWeighted chooses among weighted targets for a key.
//
// With every target usable, the choice is Bucket(key) against the cumulative
// weights: the same key always lands on the same target for the same weights,
// which is what makes an experiment's assignment sticky (ADR-0009).
//
// A target that is not usable (no eligible endpoint) gives its share to the others
// in proportion to their weights, still deterministically in the key. That is the
// "traffic shifts within seconds" of the Phase 6 exit criterion: the requests
// that would have gone to a dead target are spread, not failed. A zero-weight
// target never receives traffic this way — zero means "not now", including during
// someone else's outage.
//
// It returns -1 when no target is usable.
func PickWeighted(weights []int, key string, usable func(int) bool) int {
	total := 0
	for i, w := range weights {
		if w > 0 && usable(i) {
			total += w
		}
	}
	if total == 0 {
		return -1
	}
	// Fast path: the target the key would get with everything up.
	b := Bucket(key)
	acc := 0
	for i, w := range weights {
		acc += w
		if w > 0 && b < acc {
			if usable(i) {
				return i
			}
			break
		}
	}
	// Redistribute over the usable targets with a second, independent bucket, so
	// the displaced keys spread by weight rather than all landing on a neighbour.
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	_, _ = h.Write([]byte{0xff})
	r := int(h.Sum64() % uint64(total))
	acc = 0
	for i, w := range weights {
		if w <= 0 || !usable(i) {
			continue
		}
		acc += w
		if r < acc {
			return i
		}
	}
	return -1 // unreachable
}

// SortedKeys returns a map's keys in order; a helper for deterministic snapshots.
func SortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
