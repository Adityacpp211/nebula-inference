// Package ratelimit is the gateway's admission control: requests per minute,
// tokens per minute and concurrency, each per API key and per organization
// (docs/architecture.md §6, stage 6 ADMIT).
//
// # Algorithm
//
// RPM and TPM are token buckets whose capacity is the per-minute limit and whose
// refill rate is limit/60 per second: a full minute's allowance may be spent in a
// burst, and it comes back continuously rather than at a minute boundary, so there
// is no thundering herd at :00.
//
// TPM uses optimistic reservation. The gateway does not tokenize (axiom A2), so
// at admission it cannot know what a request will cost. It reserves an upper
// bound — the prompt's bytes plus max_tokens, which is more than the request can
// ever consume — and when the request finishes, Lease.Release returns the
// difference between the reservation and the runtime's actual count. A burst is
// therefore limited pessimistically and the long-run rate is exact.
//
// Concurrency is a sorted set of leases scored by expiry. A lease is removed on
// release; a gateway that dies holding one does not leak it, because expired
// leases are swept on every admission.
//
// # Atomicity
//
// All six checks and all six mutations run in one Lua script. A request is either
// admitted against every limit or charged against none: two scripts, or a check
// followed by a write, would let two replicas both see the last token and both
// take it.
//
// # When Redis is down
//
// The limiter falls back to an in-process implementation of the same algorithm,
// with every limit scaled by FallbackFraction because each replica now counts
// alone (docs/architecture.md §3.2: "conservative"). This is approximate by
// construction, is reported in every decision as Degraded, and is logged when it
// starts and stops. No request is refused merely because Redis is unreachable.
package ratelimit

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// Limits is one principal's allowance. Zero means unlimited on that dimension.
type Limits struct {
	RPM         int
	TPM         int
	Concurrency int
}

// Request is one admission.
type Request struct {
	OrgID string
	KeyID string
	Key   Limits
	Org   Limits
	// Tokens is the reservation: an upper bound on what the request can consume.
	Tokens int
	// LeaseTTL bounds how long the concurrency slot is held if Release is never
	// called. Set it to the request's deadline plus a margin.
	LeaseTTL time.Duration
	// LeaseID identifies the concurrency slot. The request id.
	LeaseID string
}

// Reason is why a request was refused. The values are the X-Nebula-Reason
// vocabulary of docs/api.md §1.
type Reason string

// Refusal reasons.
const (
	ReasonRPM         Reason = "rate_limited_rpm"
	ReasonTPM         Reason = "rate_limited_tpm"
	ReasonConcurrency Reason = "concurrency_limit"
)

// Scope says whose limit refused the request.
type Scope string

// Scopes.
const (
	ScopeKey Scope = "key"
	ScopeOrg Scope = "org"
)

// Snapshot is a bucket's state after the decision, for the x-ratelimit-* headers.
// Limit zero means that dimension is unlimited and no header is sent.
type Snapshot struct {
	Limit     int
	Remaining int
	// Reset is how long until the bucket is full again.
	Reset time.Duration
}

// Decision is the outcome of an admission.
type Decision struct {
	Allowed bool
	Reason  Reason
	Scope   Scope
	// RetryAfter is when the refused request could first succeed, given no other
	// traffic. Always at least one second, because Retry-After is in seconds and
	// rounding down invites an immediate retry into the same refusal.
	RetryAfter time.Duration
	// Requests and Tokens describe the KEY's buckets: the headers speak to the
	// caller about its own allowance, as OpenAI's do.
	Requests Snapshot
	Tokens   Snapshot
	// Degraded is true when the decision came from the in-process fallback.
	Degraded bool
}

// ---------------------------------------------------------------------------
// the limiter
// ---------------------------------------------------------------------------

// Limiter admits requests.
type Limiter struct {
	redis     redis.UniversalClient
	prefix    string
	opTimeout time.Duration
	local     *memory
	logger    *slog.Logger
	now       func() time.Time

	degraded atomic.Bool
	// retryAt is when Redis may next be tried after a failure (unix nanos). Without
	// it every request would pay a full Redis timeout during an outage before
	// falling back — turning a soft dependency into a latency tax on every request.
	retryAt atomic.Int64
}

// redisCooldown is how long the limiter stays on the fallback after a Redis
// failure before probing Redis again with a real request.
const redisCooldown = time.Second

// Options configures a Limiter.
type Options struct {
	// Redis is the shared counter store. Nil runs the in-process limiter only,
	// which is legal in development and refused in production by configuration.
	Redis            redis.UniversalClient
	KeyPrefix        string
	OpTimeout        time.Duration
	FallbackFraction float64
	Logger           *slog.Logger
	Now              func() time.Time
}

// New builds a limiter.
func New(o Options) *Limiter {
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	if o.OpTimeout <= 0 {
		o.OpTimeout = 150 * time.Millisecond
	}
	if o.FallbackFraction <= 0 || o.FallbackFraction > 1 {
		o.FallbackFraction = 0.5
	}
	l := &Limiter{
		redis:     o.Redis,
		prefix:    o.KeyPrefix,
		opTimeout: o.OpTimeout,
		local:     newMemory(o.FallbackFraction),
		logger:    o.Logger,
		now:       o.Now,
	}
	if o.Redis == nil {
		l.degraded.Store(true)
	}
	return l
}

// Degraded reports whether the limiter is currently counting in process only.
func (l *Limiter) Degraded() bool { return l.degraded.Load() }

// Admit decides one request. A refused request returns a nil lease. An admitted
// request's lease must be released exactly once when the request ends.
func (l *Limiter) Admit(ctx context.Context, req Request) (Decision, *Lease) {
	now := l.now()
	if l.redis != nil && now.UnixNano() >= l.retryAt.Load() {
		d, err := l.admitRedis(ctx, req, now)
		if err == nil {
			l.recovered()
			if !d.Allowed {
				return d, nil
			}
			return d, &Lease{l: l, req: req, redis: true}
		}
		l.fellBack(ctx, err)
	}
	d := l.local.admit(req, now)
	d.Degraded = true
	if !d.Allowed {
		return d, nil
	}
	return d, &Lease{l: l, req: req}
}

func (l *Limiter) fellBack(ctx context.Context, err error) {
	l.retryAt.Store(l.now().Add(redisCooldown).UnixNano())
	if l.degraded.CompareAndSwap(false, true) {
		l.logger.ErrorContext(ctx, "rate limiting has fallen back to the in-process limiter: limits are now per replica and approximate",
			slog.String("cause", err.Error()))
	}
}

func (l *Limiter) recovered() {
	if l.degraded.CompareAndSwap(true, false) {
		l.logger.Info("rate limiting is using Redis again")
	}
}

// Lease is an admitted request's hold on its concurrency slot and token
// reservation.
type Lease struct {
	l     *Limiter
	req   Request
	redis bool
	once  sync.Once
}

// Release returns the concurrency slot and settles the token reservation against
// what the request actually consumed. actualTokens is prompt plus completion tokens
// as the runtime counted them; pass the reservation itself when the true count is
// unknown, which charges the request in full rather than for free.
func (le *Lease) Release(ctx context.Context, actualTokens int) {
	if le == nil {
		return
	}
	if le.req.Tokens == 0 && le.req.Key.Concurrency == 0 && le.req.Org.Concurrency == 0 {
		return // nothing was reserved and no slot was taken: nothing to settle
	}
	le.once.Do(func() {
		now := le.l.now()
		if le.redis {
			err := le.l.releaseRedis(ctx, le.req, actualTokens, now)
			if err == nil {
				return
			}
			// The slot now expires with its lease instead of being freed. Logged
			// rather than retried: the lease TTL is the bound on the damage.
			le.l.logger.WarnContext(ctx, "releasing a rate-limit lease failed; it expires with its TTL",
				slog.String("cause", err.Error()))
			return
		}
		le.l.local.release(le.req, actualTokens, now)
	})
}

// ---------------------------------------------------------------------------
// Redis
// ---------------------------------------------------------------------------

//go:embed admit.lua
var admitSource string

//go:embed release.lua
var releaseSource string

var (
	admitScript   = redis.NewScript(admitSource)
	releaseScript = redis.NewScript(releaseSource)
)

// bucketTTL is how long an idle bucket is kept. After a minute of no traffic a
// bucket is full, which is exactly what a missing key means to the script, so
// expiring it loses nothing.
const bucketTTL = 2 * time.Minute

// keys returns the six keys of a request. Every key carries the org id as a hash
// tag, so all six hash to one Redis Cluster slot and the script may touch them all.
func (l *Limiter) keys(req Request) []string {
	tag := "{" + req.OrgID + "}"
	k := l.prefix + "rl:" + tag + ":k:" + req.KeyID
	o := l.prefix + "rl:" + tag + ":o"
	return []string{k + ":rpm", k + ":tpm", k + ":conc", o + ":rpm", o + ":tpm", o + ":conc"}
}

func (l *Limiter) admitRedis(ctx context.Context, req Request, now time.Time) (Decision, error) {
	ctx, cancel := context.WithTimeout(ctx, l.opTimeout)
	defer cancel()
	leaseTTL := req.LeaseTTL
	if leaseTTL <= 0 {
		leaseTTL = 10 * time.Minute
	}
	res, err := admitScript.Run(ctx, l.redis, l.keys(req),
		now.UnixMilli(),
		req.Key.RPM, req.Key.TPM, req.Key.Concurrency,
		req.Org.RPM, req.Org.TPM, req.Org.Concurrency,
		req.Tokens, req.LeaseID, now.Add(leaseTTL).UnixMilli(), bucketTTL.Milliseconds(),
	).Int64Slice()
	if err != nil {
		return Decision{}, err
	}
	if len(res) != 10 {
		return Decision{}, fmt.Errorf("rate-limit script returned %d values, want 10", len(res))
	}
	return decode(res, req), nil
}

// decode turns the script's reply into a Decision. Reply layout:
//
//	[allowed, reason, scope, retry_ms, rpm_remaining, rpm_reset_ms, tpm_remaining, tpm_reset_ms, _, _]
func decode(res []int64, req Request) Decision {
	d := Decision{Allowed: res[0] == 1}
	switch res[1] {
	case 1:
		d.Reason = ReasonRPM
	case 2:
		d.Reason = ReasonTPM
	case 3:
		d.Reason = ReasonConcurrency
	}
	switch res[2] {
	case 1:
		d.Scope = ScopeKey
	case 2:
		d.Scope = ScopeOrg
	}
	d.RetryAfter = retryAfter(time.Duration(res[3]) * time.Millisecond)
	if req.Key.RPM > 0 {
		d.Requests = Snapshot{Limit: req.Key.RPM, Remaining: int(res[4]), Reset: time.Duration(res[5]) * time.Millisecond}
	}
	if req.Key.TPM > 0 {
		d.Tokens = Snapshot{Limit: req.Key.TPM, Remaining: int(res[6]), Reset: time.Duration(res[7]) * time.Millisecond}
	}
	return d
}

func (l *Limiter) releaseRedis(ctx context.Context, req Request, actual int, now time.Time) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), l.opTimeout)
	defer cancel()
	keys := l.keys(req)
	kDelta := reserved(req.Tokens, req.Key.TPM) - actual
	oDelta := reserved(req.Tokens, req.Org.TPM) - actual
	return releaseScript.Run(ctx, l.redis, []string{keys[1], keys[4], keys[2], keys[5]},
		now.UnixMilli(), req.Key.TPM, req.Org.TPM, kDelta, oDelta, req.LeaseID, bucketTTL.Milliseconds(),
	).Err()
}

// reserved is what a bucket was actually charged at admission: the reservation,
// capped at the bucket's capacity so a request larger than a whole minute's
// allowance is still admissible once, against a full bucket.
func reserved(tokens, capacity int) int {
	if capacity > 0 && tokens > capacity {
		return capacity
	}
	return tokens
}

func retryAfter(d time.Duration) time.Duration {
	if d < time.Second {
		return time.Second
	}
	return d.Round(time.Second)
}

// ---------------------------------------------------------------------------
// in-process fallback
// ---------------------------------------------------------------------------

// memory is the same algorithm as the Lua script, in process, with every limit
// scaled by a fraction. It is the reference the Redis path is tested against.
type memory struct {
	fraction float64

	mu      sync.Mutex
	buckets map[string]*bucket
	leases  map[string]map[string]time.Time // concurrency key → lease id → expiry
	calls   int
}

type bucket struct {
	tokens float64
	ts     time.Time
}

func newMemory(fraction float64) *memory {
	return &memory{fraction: fraction, buckets: map[string]*bucket{}, leases: map[string]map[string]time.Time{}}
}

func (m *memory) scale(n int) int {
	if n <= 0 {
		return 0
	}
	s := int(math.Floor(float64(n) * m.fraction))
	if s < 1 {
		s = 1
	}
	return s
}

func (m *memory) level(key string, capacity int, now time.Time) float64 {
	b, ok := m.buckets[key]
	if !ok {
		return float64(capacity)
	}
	elapsed := now.Sub(b.ts)
	if elapsed < 0 {
		elapsed = 0
	}
	t := b.tokens + elapsed.Seconds()*float64(capacity)/60
	return math.Min(t, float64(capacity))
}

func (m *memory) liveLeases(key string, now time.Time) int {
	set := m.leases[key]
	for id, exp := range set {
		if !now.Before(exp) {
			delete(set, id)
		}
	}
	return len(set)
}

func (m *memory) admit(req Request, now time.Time) Decision {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sweep(now)

	k := "k:" + req.KeyID
	o := "o:" + req.OrgID
	key := Limits{RPM: m.scale(req.Key.RPM), TPM: m.scale(req.Key.TPM), Concurrency: m.scale(req.Key.Concurrency)}
	org := Limits{RPM: m.scale(req.Org.RPM), TPM: m.scale(req.Org.TPM), Concurrency: m.scale(req.Org.Concurrency)}

	type check struct {
		name   string
		cap    int
		cost   float64
		reason Reason
		scope  Scope
	}
	buckets := []check{
		{k + ":rpm", key.RPM, 1, ReasonRPM, ScopeKey},
		{k + ":tpm", key.TPM, float64(reserved(req.Tokens, key.TPM)), ReasonTPM, ScopeKey},
		{o + ":rpm", org.RPM, 1, ReasonRPM, ScopeOrg},
		{o + ":tpm", org.TPM, float64(reserved(req.Tokens, org.TPM)), ReasonTPM, ScopeOrg},
	}
	levels := make([]float64, len(buckets))
	for i, c := range buckets {
		if c.cap <= 0 {
			continue
		}
		levels[i] = m.level(c.name, c.cap, now)
		if levels[i] < c.cost {
			wait := time.Duration((c.cost - levels[i]) * 60 / float64(c.cap) * float64(time.Second))
			return Decision{Reason: c.reason, Scope: c.scope, RetryAfter: retryAfter(wait)}
		}
	}
	for _, c := range []struct {
		name  string
		cap   int
		scope Scope
	}{{k + ":conc", key.Concurrency, ScopeKey}, {o + ":conc", org.Concurrency, ScopeOrg}} {
		if c.cap > 0 && m.liveLeases(c.name, now) >= c.cap {
			return Decision{Reason: ReasonConcurrency, Scope: c.scope, RetryAfter: time.Second}
		}
	}

	d := Decision{Allowed: true}
	for i, c := range buckets {
		if c.cap <= 0 {
			continue
		}
		left := levels[i] - c.cost
		m.buckets[c.name] = &bucket{tokens: left, ts: now}
		snap := Snapshot{Limit: c.cap, Remaining: int(math.Floor(left)),
			Reset: time.Duration((float64(c.cap) - left) * 60 / float64(c.cap) * float64(time.Second))}
		switch c.name {
		case k + ":rpm":
			d.Requests = snap
		case k + ":tpm":
			d.Tokens = snap
		}
	}
	ttl := req.LeaseTTL
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	for _, c := range []struct {
		name string
		cap  int
	}{{k + ":conc", key.Concurrency}, {o + ":conc", org.Concurrency}} {
		if c.cap <= 0 {
			continue
		}
		if m.leases[c.name] == nil {
			m.leases[c.name] = map[string]time.Time{}
		}
		m.leases[c.name][req.LeaseID] = now.Add(ttl)
	}
	return d
}

func (m *memory) release(req Request, actual int, now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := "k:" + req.KeyID
	o := "o:" + req.OrgID
	for _, c := range []struct {
		name string
		cap  int
	}{{k + ":tpm", m.scale(req.Key.TPM)}, {o + ":tpm", m.scale(req.Org.TPM)}} {
		if c.cap <= 0 {
			continue
		}
		t := m.level(c.name, c.cap, now) + float64(reserved(req.Tokens, c.cap)-actual)
		t = math.Max(-float64(c.cap), math.Min(t, float64(c.cap)))
		m.buckets[c.name] = &bucket{tokens: t, ts: now}
	}
	delete(m.leases[k+":conc"], req.LeaseID)
	delete(m.leases[o+":conc"], req.LeaseID)
}

// sweep bounds memory: every 1024 admissions, drop buckets idle long enough to be
// full again and empty lease sets.
func (m *memory) sweep(now time.Time) {
	m.calls++
	if m.calls%1024 != 0 {
		return
	}
	for k, b := range m.buckets {
		if now.Sub(b.ts) > bucketTTL {
			delete(m.buckets, k)
		}
	}
	for k := range m.leases {
		if m.liveLeases(k, now) == 0 {
			delete(m.leases, k)
		}
	}
}

// ErrNoRedis is returned by Ping when the limiter has no Redis.
var ErrNoRedis = errors.New("rate limiter has no Redis configured")

// Ping checks the Redis the limiter uses, for /healthz.
func (l *Limiter) Ping(ctx context.Context) error {
	if l.redis == nil {
		return ErrNoRedis
	}
	return l.redis.Ping(ctx).Err()
}

// FormatReset renders a reset duration the way OpenAI's headers do: "1s", "6m0s".
func FormatReset(d time.Duration) string {
	if d <= 0 {
		return "0s"
	}
	if d < time.Second {
		return strconv.FormatInt(d.Milliseconds(), 10) + "ms"
	}
	return d.Round(time.Second).String()
}
