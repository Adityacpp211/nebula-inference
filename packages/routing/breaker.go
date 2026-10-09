package routing

import (
	"sync"
	"time"
)

// BreakerState is a circuit breaker's position.
type BreakerState int

// Breaker states.
const (
	BreakerClosed BreakerState = iota
	// BreakerHalfOpen admits a single trial request.
	BreakerHalfOpen
	BreakerOpen
)

func (s BreakerState) String() string {
	switch s {
	case BreakerHalfOpen:
		return "half_open"
	case BreakerOpen:
		return "open"
	}
	return "closed"
}

// BreakerConfig tunes a Breaker.
type BreakerConfig struct {
	// Threshold is how many consecutive upstream failures open the breaker. A
	// failure to connect opens it at once regardless: a replica that cannot be
	// reached is not a replica that is having a bad moment.
	Threshold int
	// Cooldown is how long the first opening lasts; each re-opening doubles it up to
	// MaxCooldown.
	Cooldown    time.Duration
	MaxCooldown time.Duration
}

// DefaultBreakerConfig is what the gateway uses unless configured otherwise.
var DefaultBreakerConfig = BreakerConfig{Threshold: 3, Cooldown: 2 * time.Second, MaxCooldown: 30 * time.Second}

// Breaker is one endpoint's local failure memory (docs/architecture.md §6.2,
// source 3). Locally observed failure is believed immediately; it does not wait
// for a heartbeat or for Kubernetes.
//
// The clock is passed to every method rather than read inside, so tests move time
// explicitly and never sleep.
type Breaker struct {
	cfg BreakerConfig

	mu       sync.Mutex
	state    BreakerState
	failures int
	until    time.Time
	cooldown time.Duration
	trial    bool // a half-open trial is in flight
}

// NewBreaker builds a closed breaker.
func NewBreaker(cfg BreakerConfig) *Breaker {
	if cfg.Threshold <= 0 {
		cfg.Threshold = DefaultBreakerConfig.Threshold
	}
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = DefaultBreakerConfig.Cooldown
	}
	if cfg.MaxCooldown < cfg.Cooldown {
		cfg.MaxCooldown = cfg.Cooldown
	}
	return &Breaker{cfg: cfg, cooldown: cfg.Cooldown}
}

// State reports the state at now, moving open to half-open once the cooldown has
// passed. A half-open breaker whose trial is out reports open, so only one request
// probes a recovering endpoint.
func (b *Breaker) State(now time.Time) BreakerState {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.stateLocked(now)
}

func (b *Breaker) stateLocked(now time.Time) BreakerState {
	if b.state == BreakerOpen && !now.Before(b.until) {
		b.state = BreakerHalfOpen
		b.trial = false
	}
	if b.state == BreakerHalfOpen && b.trial {
		return BreakerOpen
	}
	return b.state
}

// Acquire records that a request is being sent. In half-open it claims the single
// trial; it returns false if the breaker does not admit a request now.
func (b *Breaker) Acquire(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.stateLocked(now) {
	case BreakerOpen:
		return false
	case BreakerHalfOpen:
		b.trial = true
	}
	return true
}

// Success closes the breaker and resets its memory.
func (b *Breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.state, b.failures, b.trial, b.cooldown = BreakerClosed, 0, false, b.cfg.Cooldown
}

// Failure records a failed request. unreachable is a failure to connect, which
// opens the breaker at once.
func (b *Breaker) Failure(now time.Time, unreachable bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	st := b.stateLocked(now)
	b.failures++
	if st == BreakerHalfOpen || b.trial || unreachable || b.failures >= b.cfg.Threshold {
		if st == BreakerHalfOpen || b.trial {
			// The trial failed: back off harder before the next one.
			b.cooldown *= 2
			if b.cooldown > b.cfg.MaxCooldown {
				b.cooldown = b.cfg.MaxCooldown
			}
		}
		b.state, b.until, b.trial = BreakerOpen, now.Add(b.cooldown), false
	}
}

// Release abandons a half-open trial that ended with no verdict (the client left,
// say), so the next request may probe instead of the endpoint staying blocked.
func (b *Breaker) Release() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.trial = false
}

// EWMA is an exponentially weighted moving average with a fixed smoothing factor.
// Safe for concurrent use.
type EWMA struct {
	alpha float64

	mu    sync.Mutex
	value float64
	set   bool
}

// NewEWMA builds an average where each sample carries weight alpha.
func NewEWMA(alpha float64) *EWMA { return &EWMA{alpha: alpha} }

// Observe adds a sample.
func (e *EWMA) Observe(v float64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.set {
		e.value, e.set = v, true
		return
	}
	e.value = e.alpha*v + (1-e.alpha)*e.value
}

// Value returns the average, or zero before any sample.
func (e *EWMA) Value() float64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.value
}
