package ratelimit_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/adityasatwar321/nebula/services/gateway/internal/gwtest"
	"github.com/adityasatwar321/nebula/services/gateway/internal/ratelimit"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// backend runs every behavioural test twice: against Redis (the Lua script, in
// miniredis) and against the in-process fallback. The fallback is the reference;
// the two must agree, or a Redis outage silently changes what the limits mean.
type backend struct {
	name     string
	fraction float64
	redis    bool
}

var backends = []backend{{"redis", 1, true}, {"memory", 1, false}}

func newLimiter(t *testing.T, b backend, c *clock) (*ratelimit.Limiter, *miniredis.Miniredis) {
	t.Helper()
	var rdb redis.UniversalClient
	var mr *miniredis.Miniredis
	if b.redis {
		mr = gwtest.Miniredis(t)
		client := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
		t.Cleanup(func() { _ = client.Close() })
		rdb = client
	}
	return ratelimit.New(ratelimit.Options{
		Redis: rdb, KeyPrefix: "t:", OpTimeout: time.Second, FallbackFraction: b.fraction, Now: c.now,
	}), mr
}

func req(id string, key, org ratelimit.Limits, tokens int) ratelimit.Request {
	return ratelimit.Request{OrgID: "org-1", KeyID: "key-1", Key: key, Org: org, Tokens: tokens,
		LeaseTTL: time.Minute, LeaseID: id}
}

func TestRPMBucket(t *testing.T) {
	t.Parallel()
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			c := &clock{t: time.Unix(1_800_000_000, 0)}
			l, _ := newLimiter(t, b, c)
			ctx := context.Background()
			lim := ratelimit.Limits{RPM: 3}

			for i := range 3 {
				d, lease := l.Admit(ctx, req("r", lim, ratelimit.Limits{}, 0))
				if !d.Allowed {
					t.Fatalf("request %d refused: %+v", i, d)
				}
				if d.Requests.Limit != 3 || d.Requests.Remaining != 2-i {
					t.Errorf("request %d snapshot: %+v", i, d.Requests)
				}
				lease.Release(ctx, 0)
			}
			d, lease := l.Admit(ctx, req("r", lim, ratelimit.Limits{}, 0))
			if d.Allowed || lease != nil || d.Reason != ratelimit.ReasonRPM || d.Scope != ratelimit.ScopeKey {
				t.Fatalf("fourth request: %+v", d)
			}
			// One request per 20s refills at 3 RPM.
			if d.RetryAfter != 20*time.Second {
				t.Errorf("retry after: %s, want 20s", d.RetryAfter)
			}
			c.advance(20 * time.Second)
			if d, _ := l.Admit(ctx, req("r", lim, ratelimit.Limits{}, 0)); !d.Allowed {
				t.Errorf("after refill: %+v", d)
			}
		})
	}
}

// Optimistic reservation: the reservation is charged at admission and the unused
// part refunded on release, so the long-run rate counts actual tokens.
func TestTPMReservationAndRefund(t *testing.T) {
	t.Parallel()
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			c := &clock{t: time.Unix(1_800_000_000, 0)}
			l, _ := newLimiter(t, b, c)
			ctx := context.Background()
			lim := ratelimit.Limits{TPM: 1000}

			d, lease := l.Admit(ctx, req("a", lim, ratelimit.Limits{}, 800))
			if !d.Allowed || d.Tokens.Remaining != 200 {
				t.Fatalf("first: %+v", d)
			}
			if d, _ := l.Admit(ctx, req("b", lim, ratelimit.Limits{}, 300)); d.Allowed || d.Reason != ratelimit.ReasonTPM {
				t.Fatalf("second must be refused while 800 are reserved: %+v", d)
			}
			lease.Release(ctx, 100) // it only used 100: 700 come back
			d, _ = l.Admit(ctx, req("c", lim, ratelimit.Limits{}, 300))
			if !d.Allowed || d.Tokens.Remaining != 600 {
				t.Fatalf("after refund: %+v", d)
			}
		})
	}
}

// A request that used more than it reserved is charged the difference, and the
// overdraft delays later requests rather than vanishing.
func TestTPMOverdraftIsCharged(t *testing.T) {
	t.Parallel()
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			c := &clock{t: time.Unix(1_800_000_000, 0)}
			l, _ := newLimiter(t, b, c)
			ctx := context.Background()
			lim := ratelimit.Limits{TPM: 600}
			_, lease := l.Admit(ctx, req("a", lim, ratelimit.Limits{}, 100))
			lease.Release(ctx, 700) // 600 more than reserved
			d, _ := l.Admit(ctx, req("b", lim, ratelimit.Limits{}, 1))
			if d.Allowed {
				t.Fatalf("an overdrawn bucket must refuse: %+v", d)
			}
			// Level is −100 (500 − 600); 1 token needs 101 → 101 * 60/600 s ≈ 10.1s.
			if d.RetryAfter != 10*time.Second {
				t.Errorf("retry after %s, want 10s", d.RetryAfter)
			}
		})
	}
}

func TestReservationLargerThanTheBucketIsAdmissibleOnce(t *testing.T) {
	t.Parallel()
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			c := &clock{t: time.Unix(1_800_000_000, 0)}
			l, _ := newLimiter(t, b, c)
			d, _ := l.Admit(context.Background(), req("a", ratelimit.Limits{TPM: 100}, ratelimit.Limits{}, 5000))
			if !d.Allowed || d.Tokens.Remaining != 0 {
				t.Fatalf("a request bigger than a minute's allowance must fit a full bucket once: %+v", d)
			}
		})
	}
}

func TestConcurrencyLeases(t *testing.T) {
	t.Parallel()
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			c := &clock{t: time.Unix(1_800_000_000, 0)}
			l, _ := newLimiter(t, b, c)
			ctx := context.Background()
			lim := ratelimit.Limits{Concurrency: 2}
			_, l1 := l.Admit(ctx, req("a", lim, ratelimit.Limits{}, 1))
			_, l2 := l.Admit(ctx, req("b", lim, ratelimit.Limits{}, 1))
			if d, _ := l.Admit(ctx, req("c", lim, ratelimit.Limits{}, 1)); d.Allowed || d.Reason != ratelimit.ReasonConcurrency {
				t.Fatalf("third concurrent: %+v", d)
			}
			l1.Release(ctx, 1)
			l1.Release(ctx, 1) // idempotent: must not free a second slot
			_, l3 := l.Admit(ctx, req("c", lim, ratelimit.Limits{}, 1))
			if l3 == nil {
				t.Fatal("a released slot must be reusable")
			}
			if d, _ := l.Admit(ctx, req("d", lim, ratelimit.Limits{}, 1)); d.Allowed {
				t.Fatal("double release freed an extra slot")
			}
			_ = l2
			// A gateway that dies holding a slot does not leak it: the lease expires.
			c.advance(time.Minute + time.Second)
			if d, _ := l.Admit(ctx, req("e", lim, ratelimit.Limits{}, 1)); !d.Allowed {
				t.Fatalf("expired leases must be swept: %+v", d)
			}
		})
	}
}

// All-or-nothing: a request refused by the org's limit must not have been charged
// against the key's.
func TestRefusalChargesNothing(t *testing.T) {
	t.Parallel()
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			c := &clock{t: time.Unix(1_800_000_000, 0)}
			l, _ := newLimiter(t, b, c)
			ctx := context.Background()
			key := ratelimit.Limits{RPM: 10}
			org := ratelimit.Limits{RPM: 1}
			if d, _ := l.Admit(ctx, req("a", key, org, 0)); !d.Allowed {
				t.Fatal("first request refused")
			}
			for range 5 {
				d, _ := l.Admit(ctx, req("b", key, org, 0))
				if d.Allowed || d.Scope != ratelimit.ScopeOrg {
					t.Fatalf("org limit: %+v", d)
				}
			}
			// Lift the org limit: the key must still have 9 left, not 4.
			d, _ := l.Admit(ctx, req("c", key, ratelimit.Limits{}, 0))
			if !d.Allowed || d.Requests.Remaining != 8 {
				t.Fatalf("refused requests were charged to the key: %+v", d)
			}
		})
	}
}

// The property the Lua script exists for: under concurrent admission, exactly the
// limit is admitted — never one more.
func TestConcurrentAdmissionNeverOvershoots(t *testing.T) {
	t.Parallel()
	c := &clock{t: time.Unix(1_800_000_000, 0)}
	l, _ := newLimiter(t, backend{"redis", 1, true}, c)
	var admitted atomic.Int64
	var wg sync.WaitGroup
	for i := range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if d, _ := l.Admit(context.Background(), req("r"+string(rune(i)), ratelimit.Limits{RPM: 50}, ratelimit.Limits{}, 0)); d.Allowed {
				admitted.Add(1)
			}
		}()
	}
	wg.Wait()
	if n := admitted.Load(); n != 50 {
		t.Errorf("admitted %d of 200 against an RPM of 50", n)
	}
}

// Redis going away must not refuse traffic: the limiter falls back, reports it,
// and scales limits down because each replica now counts alone.
func TestFallbackWhenRedisIsDown(t *testing.T) {
	t.Parallel()
	c := &clock{t: time.Unix(1_800_000_000, 0)}
	l, mr := newLimiter(t, backend{"redis", 0.5, true}, c)
	ctx := context.Background()
	if d, _ := l.Admit(ctx, req("a", ratelimit.Limits{RPM: 10}, ratelimit.Limits{}, 0)); !d.Allowed || d.Degraded {
		t.Fatalf("healthy: %+v", d)
	}
	mr.Close()
	admitted := 0
	for range 20 {
		d, _ := l.Admit(ctx, req("b", ratelimit.Limits{RPM: 10}, ratelimit.Limits{}, 0))
		if !d.Degraded {
			t.Fatalf("a decision made without Redis must say so: %+v", d)
		}
		if d.Allowed {
			admitted++
		}
	}
	if admitted != 5 {
		t.Errorf("fallback admitted %d; want 5 (10 RPM × fraction 0.5)", admitted)
	}
	if !l.Degraded() {
		t.Error("the limiter must report itself degraded")
	}
}

// During an outage the limiter must not pay a Redis timeout on every request: after
// one failure it stays on the fallback for a cooldown, then probes again, and
// returns to Redis once it answers.
func TestFallbackCooldownAndRecovery(t *testing.T) {
	t.Parallel()
	c := &clock{t: time.Unix(1_800_000_000, 0)}
	mr := gwtest.Miniredis(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = client.Close() })
	l := ratelimit.New(ratelimit.Options{Redis: client, OpTimeout: time.Second, FallbackFraction: 1, Now: c.now})
	ctx := context.Background()

	mr.SetError("LOADING simulated outage")
	if d, _ := l.Admit(ctx, req("a", ratelimit.Limits{RPM: 100}, ratelimit.Limits{}, 0)); !d.Degraded {
		t.Fatal("a failing Redis must produce a degraded decision")
	}
	mr.SetError("")
	// Within the cooldown Redis is not consulted, even though it is back.
	if d, _ := l.Admit(ctx, req("b", ratelimit.Limits{RPM: 100}, ratelimit.Limits{}, 0)); !d.Degraded {
		t.Fatal("within the cooldown the fallback must be used without trying Redis")
	}
	c.advance(2 * time.Second)
	if d, _ := l.Admit(ctx, req("c", ratelimit.Limits{RPM: 100}, ratelimit.Limits{}, 0)); d.Degraded {
		t.Fatal("after the cooldown the limiter must return to Redis")
	}
	if l.Degraded() {
		t.Error("recovery must clear the degraded flag")
	}
}

func TestFormatReset(t *testing.T) {
	t.Parallel()
	for d, want := range map[time.Duration]string{0: "0s", 250 * time.Millisecond: "250ms", 20 * time.Second: "20s", 6 * time.Minute: "6m0s"} {
		if got := ratelimit.FormatReset(d); got != want {
			t.Errorf("FormatReset(%s) = %q, want %q", d, got, want)
		}
	}
}
