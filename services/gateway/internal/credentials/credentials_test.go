package credentials_test

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/adityasatwar321/nebula/packages/auth"
	"github.com/adityasatwar321/nebula/packages/httpx"
	"github.com/adityasatwar321/nebula/services/gateway/internal/controlplane"
	"github.com/adityasatwar321/nebula/services/gateway/internal/credentials"
	"github.com/adityasatwar321/nebula/services/gateway/internal/gwtest"
)

const pepper = "test-pepper-0123456789abcdef0123456789abcdef"

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

// fakeCP is the control plane's credential lookup.
type fakeCP struct {
	mu    sync.Mutex
	creds map[string]*controlplane.Credential
	down  bool
	calls atomic.Int64
	delay time.Duration
}

func (f *fakeCP) LookupCredential(_ context.Context, prefix string) (*controlplane.Credential, error) {
	f.calls.Add(1)
	if f.delay > 0 {
		time.Sleep(f.delay)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down {
		return nil, controlplane.ErrUnavailable
	}
	c, ok := f.creds[prefix]
	if !ok {
		return nil, controlplane.ErrNotFound
	}
	cp := *c
	return &cp, nil
}

func (f *fakeCP) set(down bool) { f.mu.Lock(); f.down = down; f.mu.Unlock() }

type fixture struct {
	r     *credentials.Resolver
	cp    *fakeCP
	clk   *clock
	key   string
	cred  *controlplane.Credential
	redis redis.UniversalClient
}

func newFixture(t *testing.T, withRedis bool) *fixture {
	t.Helper()
	h, err := auth.NewHasher(pepper)
	if err != nil {
		t.Fatal(err)
	}
	g, err := h.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cred := &controlplane.Credential{
		KeyID: uuid.New(), Prefix: g.Prefix, KeyHash: g.Hash, OrgID: uuid.New(), OrgSlug: "acme",
		Scopes: []string{"inference:invoke"}, Priority: "HIGH",
	}
	cp := &fakeCP{creds: map[string]*controlplane.Credential{g.Prefix: cred}}
	clk := &clock{t: time.Unix(1_800_000_000, 0)}
	f := &fixture{cp: cp, clk: clk, key: g.Plaintext, cred: cred}
	var rdb redis.UniversalClient
	if withRedis {
		mr := gwtest.Miniredis(t)
		c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { _ = c.Close() })
		rdb = c
		f.redis = c
	}
	f.r = credentials.New(credentials.Options{
		Hasher: h, Lookup: cp, Redis: rdb, RedisPrefix: "t:", RedisTTL: 30 * time.Second,
		OpTimeout: time.Second, LocalTTL: 5 * time.Second, NegativeTTL: 5 * time.Second,
		StaleGrace: time.Minute, MaxEntries: 16, Now: clk.now,
	})
	return f
}

func code(t *testing.T, err error) string {
	t.Helper()
	var e *httpx.APIError
	if !errors.As(err, &e) {
		t.Fatalf("want *httpx.APIError, got %v", err)
	}
	return e.Code
}

func TestAuthenticateBuildsIdentity(t *testing.T) {
	t.Parallel()
	f := newFixture(t, false)
	p, err := f.r.Authenticate(context.Background(), f.key)
	if err != nil {
		t.Fatal(err)
	}
	id := p.Identity
	if id.OrgID != f.cred.OrgID || id.OrgSlug != "acme" || id.ActorID != f.cred.KeyID ||
		id.ActorLabel != f.cred.Prefix || string(id.Priority) != "HIGH" || !id.Has(auth.ScopeInferenceInvoke) {
		t.Errorf("identity: %+v", id)
	}
	if p.Degraded {
		t.Error("a fresh lookup is not degraded")
	}
}

// The cache decides whether to re-read the record, never whether the secret is
// right: a real prefix with a wrong secret fails on a warm cache too.
func TestWrongSecretOnAWarmCache(t *testing.T) {
	t.Parallel()
	f := newFixture(t, false)
	if _, err := f.r.Authenticate(context.Background(), f.key); err != nil {
		t.Fatal(err)
	}
	forged := f.key[:auth.PrefixLen] + "0000000000000000000000000000000000000000000"
	if c := code(t, errNot(f.r.Authenticate(context.Background(), forged))); c != "invalid_credential" {
		t.Errorf("got %q", c)
	}
}

func errNot(_ credentials.Principal, err error) error { return err }

func TestMalformedKeyCostsNoLookup(t *testing.T) {
	t.Parallel()
	f := newFixture(t, false)
	for _, k := range []string{"nbk_short", "not-a-key", f.key + "x"} {
		if c := code(t, errNot(f.r.Authenticate(context.Background(), k))); c != "invalid_credential" {
			t.Errorf("%q: %q", k, c)
		}
	}
	if n := f.cp.calls.Load(); n != 0 {
		t.Errorf("malformed keys caused %d control-plane lookups", n)
	}
}

func TestUnknownPrefixIsNegativelyCached(t *testing.T) {
	t.Parallel()
	f := newFixture(t, false)
	h, _ := auth.NewHasher(pepper)
	other, _ := h.Generate()
	for range 5 {
		if c := code(t, errNot(f.r.Authenticate(context.Background(), other.Plaintext))); c != "invalid_credential" {
			t.Fatalf("got %q", c)
		}
	}
	if n := f.cp.calls.Load(); n != 1 {
		t.Errorf("an unknown prefix caused %d lookups within the negative TTL; want 1", n)
	}
	f.clk.advance(6 * time.Second)
	_, _ = f.r.Authenticate(context.Background(), other.Plaintext)
	if n := f.cp.calls.Load(); n != 2 {
		t.Errorf("after the negative TTL the prefix must be looked up again; calls=%d", n)
	}
}

func TestRevokedAndExpired(t *testing.T) {
	t.Parallel()
	f := newFixture(t, false)
	past := f.clk.now().Add(-time.Hour)
	f.cred.RevokedAt = &past
	if c := code(t, errNot(f.r.Authenticate(context.Background(), f.key))); c != "credential_revoked" {
		t.Errorf("revoked: %q", c)
	}

	g := newFixture(t, false)
	exp := g.clk.now().Add(time.Second)
	g.cred.ExpiresAt = &exp
	if _, err := g.r.Authenticate(context.Background(), g.key); err != nil {
		t.Fatalf("before expiry: %v", err)
	}
	g.clk.advance(2 * time.Second)
	if c := code(t, errNot(g.r.Authenticate(context.Background(), g.key))); c != "credential_expired" {
		t.Errorf("expired (served from cache): %q", c)
	}
}

// Axiom A8: with the control plane down, a key already seen keeps working for the
// grace period, marked degraded; an unseen key fails closed with a 503.
func TestControlPlaneOutage(t *testing.T) {
	t.Parallel()
	f := newFixture(t, false)
	if _, err := f.r.Authenticate(context.Background(), f.key); err != nil {
		t.Fatal(err)
	}
	f.cp.set(true)
	f.clk.advance(30 * time.Second) // past the local TTL, inside the grace
	p, err := f.r.Authenticate(context.Background(), f.key)
	if err != nil || !p.Degraded {
		t.Fatalf("within grace: degraded=%v err=%v", p.Degraded, err)
	}
	f.clk.advance(2 * time.Minute) // past TTL + grace
	_, err = f.r.Authenticate(context.Background(), f.key)
	var e *httpx.APIError
	if !errors.As(err, &e) || e.Status != http.StatusServiceUnavailable || e.Reason != "control_plane_degraded" {
		t.Fatalf("past grace must fail closed with 503, got %v", err)
	}

	h, _ := auth.NewHasher(pepper)
	unseen, _ := h.Generate()
	if _, err := f.r.Authenticate(context.Background(), unseen.Plaintext); !errors.As(err, &e) || e.Status != 503 {
		t.Errorf("an unseen key during an outage must be 503, got %v", err)
	}
}

// A cold cache under load is one lookup per prefix, not one per request.
func TestConcurrentMissesShareOneLookup(t *testing.T) {
	t.Parallel()
	f := newFixture(t, false)
	f.cp.delay = 50 * time.Millisecond
	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := f.r.Authenticate(context.Background(), f.key); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if n := f.cp.calls.Load(); n != 1 {
		t.Errorf("50 concurrent misses made %d lookups; want 1", n)
	}
}

// Redis is the tier shared between replicas: a second resolver (another gateway
// replica) finds the record there without asking the control plane.
func TestRedisTierIsShared(t *testing.T) {
	t.Parallel()
	f := newFixture(t, true)
	if _, err := f.r.Authenticate(context.Background(), f.key); err != nil {
		t.Fatal(err)
	}
	h, _ := auth.NewHasher(pepper)
	replica2 := credentials.New(credentials.Options{
		Hasher: h, Lookup: f.cp, Redis: f.redis, RedisPrefix: "t:", RedisTTL: 30 * time.Second,
		OpTimeout: time.Second, LocalTTL: 5 * time.Second, NegativeTTL: 5 * time.Second, Now: f.clk.now,
	})
	if _, err := replica2.Authenticate(context.Background(), f.key); err != nil {
		t.Fatal(err)
	}
	if n := f.cp.calls.Load(); n != 1 {
		t.Errorf("the second replica went to the control plane; calls=%d", n)
	}

	// Revocation through the gateway evicts the shared entry at once.
	replica2.Invalidate(context.Background(), f.cred.Prefix)
	now := f.clk.now()
	f.cred.RevokedAt = &now
	f.clk.advance(6 * time.Second) // replica 1's local entry expires
	if c := code(t, errNot(f.r.Authenticate(context.Background(), f.key))); c != "credential_revoked" {
		t.Errorf("after invalidation the revocation must be seen: %q", c)
	}
}

func TestCacheIsBounded(t *testing.T) {
	t.Parallel()
	f := newFixture(t, false)
	h, _ := auth.NewHasher(pepper)
	for range 100 {
		k, _ := h.Generate()
		_, _ = f.r.Authenticate(context.Background(), k.Plaintext)
	}
	// Not observable directly; the property is that the known key still works and
	// nothing panicked while evicting under a flood of unknown prefixes.
	if _, err := f.r.Authenticate(context.Background(), f.key); err != nil {
		t.Fatal(err)
	}
}
