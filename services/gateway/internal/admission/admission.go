// Package admission holds the gateway's admission queues: one packages/queue gate
// per deployment, sized by the router's view of that deployment's free capacity
// (docs/architecture.md §6.3, step 9 of the request path).
//
// The gateway queue is about fairness across tenants and priorities; the worker's
// own queue is about keeping the runtime's batch window full. Two bounded tiers,
// deliberately.
package admission

import (
	"context"
	"sync"
	"time"

	"github.com/adityasatwar321/nebula/packages/queue"
)

// Capacity reports how many requests may be in flight to a deployment now;
// router.Router.Capacity.
type Capacity func(key string) int

// Options configure the queues.
type Options struct {
	MaxDepth  int
	AgingStep time.Duration
	// Sweep is how often expired entries are removed and freed capacity granted.
	Sweep    time.Duration
	Capacity Capacity
	Now      func() time.Time
}

// Queues is the set of per-deployment gates.
type Queues struct {
	o Options

	mu    sync.Mutex
	gates map[string]*queue.Gate
	used  map[string]time.Time
}

// New builds the set.
func New(o Options) *Queues {
	if o.Sweep <= 0 {
		o.Sweep = 50 * time.Millisecond
	}
	return &Queues{o: o, gates: map[string]*queue.Gate{}, used: map[string]time.Time{}}
}

// Gate returns the deployment's gate, creating it on first use.
func (q *Queues) Gate(key string) *queue.Gate {
	q.mu.Lock()
	defer q.mu.Unlock()
	g := q.gates[key]
	if g == nil {
		g = queue.New(queue.Config{MaxDepth: q.o.MaxDepth, AgingStep: q.o.AgingStep, Now: q.o.Now},
			func() int { return q.o.Capacity(key) })
		q.gates[key] = g
	}
	q.used[key] = time.Now()
	return g
}

// Run sweeps every gate until ctx ends, and forgets gates idle for ten minutes
// (a deleted deployment's gate would otherwise live forever).
func (q *Queues) Run(ctx context.Context) {
	t := time.NewTicker(q.o.Sweep)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		q.mu.Lock()
		gates := make([]*queue.Gate, 0, len(q.gates))
		for key, g := range q.gates {
			if s := g.Stats(); s.Depth == 0 && s.InFlight == 0 && time.Since(q.used[key]) > 10*time.Minute {
				delete(q.gates, key)
				delete(q.used, key)
				continue
			}
			gates = append(gates, g)
		}
		q.mu.Unlock()
		for _, g := range gates {
			g.Tick()
		}
	}
}

// Stats reports every gate, by deployment key.
func (q *Queues) Stats() map[string]queue.Stats {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make(map[string]queue.Stats, len(q.gates))
	for k, g := range q.gates {
		out[k] = g.Stats()
	}
	return out
}
