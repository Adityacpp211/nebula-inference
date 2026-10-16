// Package reliability holds NEBULA's retry rules (docs/reliability.md): what a
// failure means, whether it may be retried, and how much retrying the system can
// afford.
//
// Three rules carry the weight:
//
//   - only a failure before any work started is retried, so a request is never
//     generated twice and a token already sent is never sent again;
//   - retries spend from a budget proportional to traffic, so a total outage
//     multiplies load by at most (1 + ratio), not by the attempt cap;
//   - waits between attempts on the same target use exponential backoff with full
//     jitter, so synchronized clients do not retry in lockstep.
package reliability

import (
	"math"
	"math/rand/v2"
	"sync"
	"time"
)

// Failure is what is known about one failed attempt, independent of transport.
type Failure struct {
	// Status is the upstream HTTP status, or 0 when no response arrived.
	Status int
	// Connect is true when the connection itself failed: refused, reset before a
	// response, DNS, dial timeout. Nothing reached the upstream.
	Connect bool
	// Timeout is true when the attempt's own deadline passed.
	Timeout bool
	// Started is true once any output reached the client (headers committed or a
	// token streamed). Nothing after this point is ever retried.
	Started bool
	// ClientGone is true when the client cancelled or disconnected.
	ClientGone bool
	// Reason is the upstream's machine-readable reason, when it sent one.
	Reason string
}

// Class is a row of the classification table.
type Class struct {
	Name string
	// Retry: another attempt on a different replica may succeed and cannot
	// duplicate work.
	Retry bool
	// Breaker: the failure counts against the endpoint's circuit breaker.
	Breaker bool
	// Description is the row's rationale, as docs/reliability.md states it.
	Description string
}

// The classification table. Order matters: the first matching row wins, and the
// "started" and "client gone" rows come first because nothing overrides them.
var (
	ClassClientGone  = Class{"client_gone", false, false, "the client left; finishing or retrying serves no one"}
	ClassStarted     = Class{"started", false, true, "output reached the client: a retry would duplicate tokens"}
	ClassConnect     = Class{"connect", true, true, "nothing reached the replica, so nothing ran"}
	ClassSaturated   = Class{"saturated", true, false, "429 before work: the replica is healthy and full"}
	ClassUnavailable = Class{"unavailable", true, true, "503 before work: draining, not ready, or model not loaded"}
	ClassTimeout     = Class{"timeout", false, true, "the attempt ran out of time; work may have started"}
	ClassClientError = Class{"client_error", false, false, "4xx: the request is wrong and will be wrong everywhere"}
	ClassServerError = Class{"server_error", false, true, "5xx after the replica accepted the request: it may have run"}
	ClassOK          = Class{"ok", false, false, "not a failure"}
)

// Table lists every row, for docs and tests.
func Table() []Class {
	return []Class{ClassClientGone, ClassStarted, ClassConnect, ClassSaturated, ClassUnavailable,
		ClassTimeout, ClassClientError, ClassServerError, ClassOK}
}

// Classify finds a failure's row.
func Classify(f Failure) Class {
	switch {
	case f.ClientGone:
		return ClassClientGone
	case f.Started:
		return ClassStarted
	case f.Connect:
		return ClassConnect
	case f.Status == 429:
		return ClassSaturated
	case f.Status == 503:
		return ClassUnavailable
	case f.Timeout:
		return ClassTimeout
	case f.Status >= 400 && f.Status < 500:
		return ClassClientError
	case f.Status >= 500:
		return ClassServerError
	}
	return ClassOK
}

// Backoff is the wait before attempt n (n >= 1) on the same target: full jitter,
// uniform in [0, min(cap, base·2^(n-1))]. Full jitter rather than equal jitter
// because it spreads synchronized clients furthest for the same mean wait.
func Backoff(n int, base, ceiling time.Duration, r *rand.Rand) time.Duration {
	if n < 1 || base <= 0 {
		return 0
	}
	limit := float64(base) * math.Pow(2, float64(n-1))
	if ceiling > 0 && limit > float64(ceiling) {
		limit = float64(ceiling)
	}
	if r == nil {
		return time.Duration(rand.Float64() * limit) //nolint:gosec // jitter spreads load; it guards no secret
	}
	return time.Duration(r.Float64() * limit)
}

// Budget caps retries at a fraction of recent requests, plus a small floor so a
// quiet service can still retry at all. It is the Finagle/Envoy retry budget:
// under a total outage every request fails, and the budget — not the attempt
// cap — is what keeps the retries from multiplying the load.
type Budget struct {
	// Ratio is retries allowed per request, e.g. 0.2.
	Ratio float64
	// MinPerSecond is retries allowed regardless of traffic.
	MinPerSecond float64
	// Window is how far back requests and retries are counted.
	Window time.Duration
	// Now is the clock; nil uses time.Now.
	Now func() time.Time

	mu      sync.Mutex
	buckets []bucket
}

type bucket struct {
	second            int64
	requests, retries float64
}

// Request records one request, which earns Ratio retries.
func (b *Budget) Request() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.current().requests++
}

// Retry reports whether a retry may be spent now, and spends it if so.
func (b *Budget) Retry() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	cur := b.current()
	var requests, retries float64
	for _, k := range b.buckets {
		requests += k.requests
		retries += k.retries
	}
	allowed := b.Ratio*requests + b.MinPerSecond*b.window().Seconds()
	if retries+1 > allowed {
		return false
	}
	cur.retries++
	return true
}

func (b *Budget) window() time.Duration {
	if b.Window <= 0 {
		return 10 * time.Second
	}
	return b.Window
}

// current returns this second's bucket, dropping buckets outside the window.
func (b *Budget) current() *bucket {
	now := time.Now
	if b.Now != nil {
		now = b.Now
	}
	sec := now().Unix()
	oldest := sec - int64(b.window().Seconds()) + 1
	keep := b.buckets[:0]
	for _, k := range b.buckets {
		if k.second >= oldest {
			keep = append(keep, k)
		}
	}
	b.buckets = keep
	if n := len(b.buckets); n > 0 && b.buckets[n-1].second == sec {
		return &b.buckets[n-1]
	}
	b.buckets = append(b.buckets, bucket{second: sec})
	return &b.buckets[len(b.buckets)-1]
}
