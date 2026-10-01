// Package queue is NEBULA's admission queue: a concurrency gate with a bounded,
// prioritised, deadline-respecting waiting room in front of it
// (docs/architecture.md §6.3).
//
// The queue exists to turn "momentarily over capacity" into latency instead of
// errors, and only within a budget:
//
//   - Bounded. A full queue refuses at once with a Retry-After estimate; it never
//     grows. Memory under sustained overload is flat by construction.
//   - Three priorities, with aging: an entry's effective priority rises one level
//     per aging step it has waited, so LOW cannot starve behind a steady stream of
//     HIGH.
//   - Absolute deadlines. An entry whose deadline passes is removed — by the
//     sweeper, not only at dequeue — and reported as a queue timeout, which is not
//     an inference error.
//   - Cancellation removes the entry immediately.
//
// A Gate knows nothing about HTTP, routes or workers. Capacity is a function the
// owner supplies, re-read on every decision, because the number of replicas behind
// a gate changes while requests wait. All time comes from an injected clock, so
// tests are deterministic.
package queue

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

// Priority orders waiting entries. It comes from the API key's policy, never from
// the request body, so a caller cannot promote itself.
type Priority int

// Priorities.
const (
	Low Priority = iota
	Normal
	High
)

// ParsePriority maps the API's priority names, defaulting to Normal.
func ParsePriority(s string) Priority {
	switch s {
	case "LOW", "low":
		return Low
	case "HIGH", "high":
		return High
	}
	return Normal
}

func (p Priority) String() string {
	switch p {
	case Low:
		return "LOW"
	case High:
		return "HIGH"
	}
	return "NORMAL"
}

// Drop reasons, the labels of Stats.Dropped.
const (
	ReasonFull      = "queue_full"
	ReasonTimeout   = "queue_timeout"
	ReasonCancelled = "client_cancelled"
	ReasonRejected  = "rejected" // the caller asked not to wait
)

// ErrTimeout means the entry's deadline passed while it waited.
var ErrTimeout = errors.New("the request's deadline passed while it was queued")

// ErrCancelled means the caller's context ended while it waited.
var ErrCancelled = errors.New("the request was cancelled while it was queued")

// FullError means the queue is at its bound. RetryAfter is an estimate of when a
// place is likely to free up; never zero.
type FullError struct {
	Depth      int
	RetryAfter time.Duration
	// WouldWait is set when the caller asked not to wait (reject mode) rather than
	// the queue being full.
	WouldWait bool
}

func (e *FullError) Error() string {
	if e.WouldWait {
		return fmt.Sprintf("no capacity now and the request asked not to wait (retry after %s)", e.RetryAfter)
	}
	return fmt.Sprintf("admission queue full at %d (retry after %s)", e.Depth, e.RetryAfter)
}

// Config parameterises a Gate.
type Config struct {
	// MaxDepth bounds the waiting room. Zero means no waiting: a request either
	// runs now or is refused.
	MaxDepth int
	// AgingStep is how long an entry waits to gain one priority level. Zero
	// disables aging.
	AgingStep time.Duration
	// Now is the clock.
	Now func() time.Time
}

// Stats is a snapshot of a gate, the per-deployment metric set.
type Stats struct {
	InFlight  int            `json:"in_flight"`
	Capacity  int            `json:"capacity"`
	Depth     int            `json:"depth"`
	ByPrio    map[string]int `json:"depth_by_priority"`
	OldestAge time.Duration  `json:"oldest_age_ns"`
	Admitted  uint64         `json:"admitted_total"`
	Queued    uint64         `json:"queued_total"`
	Dropped   map[string]int `json:"dropped_total"`
	// Wait quantiles over the recent window of granted entries that waited.
	WaitP50 time.Duration `json:"wait_p50_ns"`
	WaitP95 time.Duration `json:"wait_p95_ns"`
	WaitP99 time.Duration `json:"wait_p99_ns"`
}

type entry struct {
	prio     Priority
	enqueued time.Time
	deadline time.Time
	seq      uint64
	// done is closed exactly once with the outcome set; granted entries hold a slot.
	done    chan struct{}
	granted bool
	err     error
}

// Gate is one admission queue in front of a concurrency limit. Safe for
// concurrent use.
type Gate struct {
	cfg      Config
	capacity func() int
	// wallClock is set when the gate runs on real time, so a waiter can also set a
	// timer of its own. With an injected clock only Tick expires entries.
	wallClock bool

	mu       sync.Mutex
	inflight int
	waiting  []*entry
	seq      uint64
	admitted uint64
	queued   uint64
	dropped  map[string]int
	waits    ring
	service  float64 // EWMA of how long a slot is held, seconds
}

// New builds a gate. capacity is consulted on every decision.
func New(cfg Config, capacity func() int) *Gate {
	wall := cfg.Now == nil
	if wall {
		cfg.Now = time.Now
	}
	return &Gate{cfg: cfg, capacity: capacity, wallClock: wall, dropped: map[string]int{}, waits: newRing(512)}
}

// Ticket is a held slot. Release it exactly once.
type Ticket struct {
	g        *Gate
	acquired time.Time
	// Waited is how long the request queued before it was granted.
	Waited   time.Duration
	released bool
}

// Release frees the slot and admits the next waiter if capacity allows.
func (t *Ticket) Release() {
	if t == nil || t.g == nil {
		return
	}
	g := t.g
	g.mu.Lock()
	defer g.mu.Unlock()
	if t.released {
		return
	}
	t.released = true
	g.inflight--
	held := g.cfg.Now().Sub(t.acquired).Seconds()
	if g.service == 0 {
		g.service = held
	} else {
		g.service = 0.2*held + 0.8*g.service
	}
	g.grantLocked(g.cfg.Now())
}

// Acquire returns a ticket once the request may run.
//
// It runs now if there is capacity and nobody is waiting (waiting entries are
// never overtaken by a newcomer). Otherwise it waits in the queue until granted,
// until its deadline, or until ctx ends. With reject set it never waits: no
// capacity now is a FullError with WouldWait.
func (g *Gate) Acquire(ctx context.Context, prio Priority, deadline time.Time, reject bool) (*Ticket, error) {
	now := g.cfg.Now()
	g.mu.Lock()
	if len(g.waiting) == 0 && g.inflight < g.capacity() {
		g.inflight++
		g.admitted++
		g.mu.Unlock()
		return &Ticket{g: g, acquired: now}, nil
	}
	if reject {
		g.dropped[ReasonRejected]++
		ra := g.retryAfterLocked()
		g.mu.Unlock()
		return nil, &FullError{Depth: len(g.waiting), RetryAfter: ra, WouldWait: true}
	}
	if !deadline.IsZero() && !now.Before(deadline) {
		g.dropped[ReasonTimeout]++
		g.mu.Unlock()
		return nil, ErrTimeout
	}
	if len(g.waiting) >= g.cfg.MaxDepth {
		g.dropped[ReasonFull]++
		ra := g.retryAfterLocked()
		depth := len(g.waiting)
		g.mu.Unlock()
		return nil, &FullError{Depth: depth, RetryAfter: ra}
	}
	g.seq++
	e := &entry{prio: prio, enqueued: now, deadline: deadline, seq: g.seq, done: make(chan struct{})}
	g.waiting = append(g.waiting, e)
	g.queued++
	g.mu.Unlock()

	var timer <-chan time.Time
	if !deadline.IsZero() && g.wallClock {
		// The sweeper expires entries too; this timer means a waiter never depends
		// on the sweeper's interval for its own deadline.
		t := time.NewTimer(time.Until(deadline))
		defer t.Stop()
		timer = t.C
	}
	select {
	case <-e.done:
	case <-ctx.Done():
		g.remove(e, ReasonCancelled, ErrCancelled)
	case <-timer:
		g.remove(e, ReasonTimeout, ErrTimeout)
	}
	<-e.done
	if !e.granted {
		return nil, e.err
	}
	waited := e.enqueued
	return &Ticket{g: g, acquired: g.cfg.Now(), Waited: g.cfg.Now().Sub(waited)}, nil
}

// remove takes a waiting entry out with a failure, unless it was granted first.
func (g *Gate) remove(e *entry, reason string, err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	select {
	case <-e.done:
		return // granted (or removed) already
	default:
	}
	for i, w := range g.waiting {
		if w == e {
			g.waiting = append(g.waiting[:i], g.waiting[i+1:]...)
			break
		}
	}
	g.dropped[reason]++
	e.err = err
	close(e.done)
}

// Tick expires overdue entries and grants waiters capacity has room for. The
// sweeper calls it; tests call it with a moved clock.
func (g *Gate) Tick() {
	now := g.cfg.Now()
	g.mu.Lock()
	defer g.mu.Unlock()
	kept := g.waiting[:0]
	for _, e := range g.waiting {
		if !e.deadline.IsZero() && !now.Before(e.deadline) {
			g.dropped[ReasonTimeout]++
			e.err = ErrTimeout
			close(e.done)
			continue
		}
		kept = append(kept, e)
	}
	for i := len(kept); i < len(g.waiting); i++ {
		g.waiting[i] = nil
	}
	g.waiting = kept
	g.grantLocked(now)
}

// Run sweeps every interval until ctx ends.
func (g *Gate) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			g.Tick()
		}
	}
}

// grantLocked admits waiters, best first, while capacity allows.
func (g *Gate) grantLocked(now time.Time) {
	for len(g.waiting) > 0 && g.inflight < g.capacity() {
		best := 0
		for i := 1; i < len(g.waiting); i++ {
			if g.before(g.waiting[i], g.waiting[best], now) {
				best = i
			}
		}
		e := g.waiting[best]
		g.waiting = append(g.waiting[:best], g.waiting[best+1:]...)
		g.inflight++
		g.admitted++
		g.waits.add(now.Sub(e.enqueued))
		e.granted = true
		close(e.done)
	}
}

// effective is an entry's priority after aging.
func (g *Gate) effective(e *entry, now time.Time) Priority {
	p := e.prio
	if g.cfg.AgingStep > 0 {
		p += Priority(now.Sub(e.enqueued) / g.cfg.AgingStep)
	}
	if p > High {
		p = High
	}
	return p
}

// before orders entries: higher effective priority first, then the one that has
// waited longer.
func (g *Gate) before(a, b *entry, now time.Time) bool {
	pa, pb := g.effective(a, now), g.effective(b, now)
	if pa != pb {
		return pa > pb
	}
	return a.seq < b.seq
}

// retryAfterLocked estimates when a place frees: the queue ahead drains at
// capacity per service time. Bounded to [1s, 30s] so a client neither hammers nor
// gives up on a transient spike.
func (g *Gate) retryAfterLocked() time.Duration {
	c := g.capacity()
	if c < 1 {
		c = 1
	}
	svc := g.service
	if svc == 0 {
		svc = 1
	}
	secs := math.Ceil(svc * float64(len(g.waiting)+1) / float64(c))
	switch {
	case secs < 1:
		secs = 1
	case secs > 30:
		secs = 30
	}
	return time.Duration(secs) * time.Second
}

// Stats reports the gate's state.
func (g *Gate) Stats() Stats {
	now := g.cfg.Now()
	g.mu.Lock()
	defer g.mu.Unlock()
	s := Stats{InFlight: g.inflight, Capacity: g.capacity(), Depth: len(g.waiting),
		ByPrio: map[string]int{}, Admitted: g.admitted, Queued: g.queued, Dropped: map[string]int{}}
	for _, e := range g.waiting {
		s.ByPrio[e.prio.String()]++
		if age := now.Sub(e.enqueued); age > s.OldestAge {
			s.OldestAge = age
		}
	}
	for k, v := range g.dropped {
		s.Dropped[k] = v
	}
	s.WaitP50, s.WaitP95, s.WaitP99 = g.waits.quantile(0.5), g.waits.quantile(0.95), g.waits.quantile(0.99)
	return s
}

// ring keeps the most recent wait durations for quantiles.
type ring struct {
	buf  []time.Duration
	next int
	full bool
}

func newRing(n int) ring { return ring{buf: make([]time.Duration, n)} }

func (r *ring) add(d time.Duration) {
	r.buf[r.next] = d
	r.next = (r.next + 1) % len(r.buf)
	if r.next == 0 {
		r.full = true
	}
}

func (r *ring) quantile(q float64) time.Duration {
	n := r.next
	if r.full {
		n = len(r.buf)
	}
	if n == 0 {
		return 0
	}
	cp := append([]time.Duration(nil), r.buf[:n]...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	return cp[int(math.Ceil(q*float64(n)))-1]
}
