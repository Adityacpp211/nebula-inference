// Package credentials authenticates API keys at the gateway.
//
// The lookup path is the one docs/security-boundaries.md §2 (B1, step 4) fixes:
// parse the key's shape, find its record by indexed prefix, verify the HMAC in
// constant time with the pepper held in memory, then check revocation and expiry.
// The record comes from three tiers, fastest first:
//
//  1. an in-process cache with a short TTL — the only tier a revocation cannot
//     reach on other replicas, so its TTL is the cross-replica revocation delay;
//  2. Redis, shared by every replica, with the longer key-cache TTL, and evicted
//     explicitly when a key is revoked through the gateway;
//  3. the control plane's internal credential lookup, which reads PostgreSQL.
//
// Two degraded modes, both documented in docs/architecture.md §8.4:
//
//   - Redis down: tier 2 is skipped. Nothing fails; the control plane takes the
//     load the cache was absorbing.
//   - Control plane down: a record already in tier 1 keeps authenticating past its
//     TTL for a bounded grace period (axiom A8), and the request is marked degraded.
//     A key the gateway has never seen cannot be verified and gets a 503 — failing
//     closed, never open.
//
// A revocation made through one replica's admin proxy is broadcast to the others
// over NATS (RevokedSubject), which evict it from their in-process tier at once.
// Without NATS, or for a revocation made directly at the control plane, other
// replicas' in-process entries last until their short TTL passes.
//
// A cache hit never decides whether a secret is right: the stored hash is verified
// on every request. The cache only decides whether the record must be re-read.
package credentials

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"

	"github.com/adityasatwar321/nebula/packages/auth"
	"github.com/adityasatwar321/nebula/packages/db/models"
	"github.com/adityasatwar321/nebula/packages/httpx"
	"github.com/adityasatwar321/nebula/services/gateway/internal/controlplane"
)

// Lookuper fetches a credential record. Satisfied by *controlplane.Client.
type Lookuper interface {
	LookupCredential(ctx context.Context, prefix string) (*controlplane.Credential, error)
}

// Principal is an authenticated caller.
type Principal struct {
	Identity auth.Identity
	// Policy is the key's rate-limit policy, nil when the key has none.
	Policy *controlplane.RatePolicy
	// Degraded is true when the record was served past its TTL because the control
	// plane could not be reached.
	Degraded bool
}

// Options configures a Resolver.
type Options struct {
	Hasher      *auth.Hasher
	Lookup      Lookuper
	Redis       redis.UniversalClient // nil disables tier 2
	RedisPrefix string
	RedisTTL    time.Duration
	OpTimeout   time.Duration
	LocalTTL    time.Duration
	NegativeTTL time.Duration
	StaleGrace  time.Duration
	MaxEntries  int
	Logger      *slog.Logger
	Now         func() time.Time
}

// Resolver authenticates keys.
type Resolver struct {
	o     Options
	group singleflight.Group

	mu      sync.Mutex
	entries map[string]*entry
}

type entry struct {
	cred     *controlplane.Credential // nil for a negative entry
	fetched  time.Time
	negative bool
}

// New builds a resolver.
func New(o Options) *Resolver {
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.MaxEntries < 1 {
		o.MaxEntries = 4096
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	return &Resolver{o: o, entries: map[string]*entry{}}
}

// Authenticate turns a presented key into a principal, or an *httpx.APIError.
func (r *Resolver) Authenticate(ctx context.Context, presented string) (Principal, error) {
	prefix, err := auth.ParsePrefix(presented)
	if err != nil {
		return Principal{}, invalidKey()
	}

	cred, negative, degraded, err := r.record(ctx, prefix)
	if err != nil {
		return Principal{}, err
	}
	if negative {
		// Same cost as a real verification, so an unknown prefix and a known one are
		// indistinguishable by latency.
		r.o.Hasher.Verify(presented, make([]byte, auth.HashLen))
		return Principal{}, invalidKey()
	}
	if !r.o.Hasher.Verify(presented, cred.KeyHash) {
		return Principal{}, invalidKey()
	}
	// Revocation and expiry are checked after verification, so presenting garbage
	// against a real prefix cannot reveal that the prefix exists.
	switch {
	case cred.RevokedAt != nil:
		return Principal{}, unauthenticated("this API key has been revoked", "credential_revoked")
	case cred.ExpiresAt != nil && !r.o.Now().Before(*cred.ExpiresAt):
		return Principal{}, unauthenticated("this API key has expired", "credential_expired")
	}

	ident, err := identityOf(cred)
	if err != nil {
		return Principal{}, httpx.ErrInternal(err)
	}
	return Principal{Identity: ident, Policy: cred.RateLimitPolicy, Degraded: degraded}, nil
}

// record finds a credential through the three tiers.
func (r *Resolver) record(ctx context.Context, prefix string) (cred *controlplane.Credential, negative, degraded bool, err error) {
	now := r.o.Now()

	r.mu.Lock()
	e := r.entries[prefix]
	r.mu.Unlock()
	if e != nil {
		ttl := r.o.LocalTTL
		if e.negative {
			ttl = r.o.NegativeTTL
		}
		if now.Sub(e.fetched) < ttl {
			return e.cred, e.negative, false, nil
		}
	}

	// The shared tier keeps a record for its TTL plus the stale grace. Within the
	// TTL it is simply used; past it, only as a fallback when the control plane
	// cannot answer — which is what lets a replica that restarts during a
	// control-plane outage keep authenticating keys it has never seen itself
	// (axiom A8), with the same hard cap as the in-process tier.
	shared, sharedAt := r.fromRedis(ctx, prefix)
	if shared != nil && now.Sub(sharedAt) < r.o.RedisTTL {
		r.store(prefix, &entry{cred: shared, fetched: now})
		return shared, false, false, nil
	}

	// One control-plane call per prefix at a time, however many requests are
	// waiting for it: a cold cache under load is otherwise a thundering herd
	// against the one service that must stay responsive.
	v, err, _ := r.group.Do(prefix, func() (any, error) {
		return r.o.Lookup.LookupCredential(context.WithoutCancel(ctx), prefix)
	})
	switch {
	case err == nil:
		cred, _ := v.(*controlplane.Credential)
		r.toRedis(ctx, prefix, cred)
		r.store(prefix, &entry{cred: cred, fetched: now})
		return cred, false, false, nil

	case errors.Is(err, controlplane.ErrNotFound):
		r.store(prefix, &entry{fetched: now, negative: true})
		return nil, true, false, nil

	default:
		// The control plane cannot answer. A positive record we already hold keeps
		// working for the grace period; nothing else does.
		if e != nil && !e.negative && now.Sub(e.fetched) < r.o.LocalTTL+r.o.StaleGrace {
			r.o.Logger.WarnContext(ctx, "serving a cached credential past its TTL: the control plane is unreachable",
				slog.String("key_prefix", prefix),
				slog.Duration("age", now.Sub(e.fetched)),
				slog.String("cause", err.Error()))
			return e.cred, false, true, nil
		}
		if shared != nil && now.Sub(sharedAt) < r.o.RedisTTL+r.o.StaleGrace {
			r.o.Logger.WarnContext(ctx, "serving a shared cached credential past its TTL: the control plane is unreachable",
				slog.String("key_prefix", prefix),
				slog.Duration("age", now.Sub(sharedAt)),
				slog.String("cause", err.Error()))
			// Kept locally with its original age, so the grace stays bounded.
			r.store(prefix, &entry{cred: shared, fetched: sharedAt.Add(r.o.RedisTTL - r.o.LocalTTL)})
			return shared, false, true, nil
		}
		r.o.Logger.ErrorContext(ctx, "cannot verify a credential: the control plane is unreachable",
			slog.String("key_prefix", prefix), slog.String("cause", err.Error()))
		return nil, false, false, &httpx.APIError{
			Status:  http.StatusServiceUnavailable,
			Message: "credentials cannot be verified right now; retry shortly",
			Type:    httpx.TypeServiceUnavailable,
			Code:    "credential_verification_unavailable",
			Reason:  "control_plane_degraded",
		}
	}
}

// Invalidate drops a key from every tier this replica can reach. Called when a
// revocation passes through the admin proxy.
func (r *Resolver) Invalidate(ctx context.Context, prefix string) {
	r.mu.Lock()
	delete(r.entries, prefix)
	r.mu.Unlock()
	if r.o.Redis == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.o.OpTimeout)
	defer cancel()
	if err := r.o.Redis.Del(ctx, r.redisKey(prefix)).Err(); err != nil {
		// The shared entry now lives until its TTL. Logged loudly: this is a
		// revocation taking longer than it should.
		r.o.Logger.ErrorContext(ctx, "could not evict a revoked key from the shared cache; it expires with its TTL",
			slog.String("key_prefix", prefix), slog.Duration("ttl", r.o.RedisTTL), slog.String("cause", err.Error()))
	}
}

// RevokedSubject carries a revoked key's prefix between gateway replicas.
const RevokedSubject = "nebula.gateway.credentials.revoked"

// InvalidateLocal drops a key from this replica's in-process tier only: the
// receiving end of the broadcast, where the shared tier is already evicted.
func (r *Resolver) InvalidateLocal(prefix string) {
	r.mu.Lock()
	delete(r.entries, prefix)
	r.mu.Unlock()
}

func (r *Resolver) redisKey(prefix string) string { return r.o.RedisPrefix + "cred:" + prefix }

// sharedRecord is the shared tier's value: the record and when it was fetched
// from the control plane.
type sharedRecord struct {
	FetchedAtMS int64                    `json:"fetched_at_ms"`
	Cred        *controlplane.Credential `json:"cred"`
}

func (r *Resolver) fromRedis(ctx context.Context, prefix string) (*controlplane.Credential, time.Time) {
	if r.o.Redis == nil {
		return nil, time.Time{}
	}
	ctx, cancel := context.WithTimeout(ctx, r.o.OpTimeout)
	defer cancel()
	b, err := r.o.Redis.Get(ctx, r.redisKey(prefix)).Bytes()
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			r.o.Logger.DebugContext(ctx, "credential cache read failed; falling through to the control plane",
				slog.String("cause", err.Error()))
		}
		return nil, time.Time{}
	}
	var rec sharedRecord
	if err := json.Unmarshal(b, &rec); err != nil || rec.Cred == nil || rec.Cred.Prefix != prefix {
		return nil, time.Time{}
	}
	return rec.Cred, time.UnixMilli(rec.FetchedAtMS)
}

func (r *Resolver) toRedis(ctx context.Context, prefix string, cred *controlplane.Credential) {
	if r.o.Redis == nil {
		return
	}
	b, err := json.Marshal(sharedRecord{FetchedAtMS: r.o.Now().UnixMilli(), Cred: cred})
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.o.OpTimeout)
	defer cancel()
	if err := r.o.Redis.Set(ctx, r.redisKey(prefix), b, r.o.RedisTTL+r.o.StaleGrace).Err(); err != nil {
		r.o.Logger.DebugContext(ctx, "credential cache write failed", slog.String("cause", err.Error()))
	}
}

func (r *Resolver) store(prefix string, e *entry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.entries[prefix]; !exists && len(r.entries) >= r.o.MaxEntries {
		r.evictLocked()
	}
	r.entries[prefix] = e
}

// evictLocked drops entries too old to be used even in degraded mode, then, if
// nothing was freed, the oldest. Bounded memory is the requirement; a flood of
// invented prefixes must not grow the map without limit.
func (r *Resolver) evictLocked() {
	now := r.o.Now()
	limit := r.o.LocalTTL + r.o.StaleGrace
	freed := false
	for k, e := range r.entries {
		if (e.negative && now.Sub(e.fetched) >= r.o.NegativeTTL) || now.Sub(e.fetched) >= limit {
			delete(r.entries, k)
			freed = true
		}
	}
	if freed {
		return
	}
	var oldestKey string
	var oldest time.Time
	for k, e := range r.entries {
		if oldestKey == "" || e.fetched.Before(oldest) {
			oldestKey, oldest = k, e.fetched
		}
	}
	delete(r.entries, oldestKey)
}

// identityOf builds the identity for a verified credential.
func identityOf(c *controlplane.Credential) (auth.Identity, error) {
	scopes, err := auth.ParseScopes(c.Scopes)
	if err != nil {
		return auth.Identity{}, err
	}
	var zero uuid.UUID
	if c.OrgID == zero {
		return auth.Identity{}, errors.New("credential record has no organization")
	}
	return auth.Identity{
		OrgID:             c.OrgID,
		OrgSlug:           c.OrgSlug,
		ActorType:         models.ActorAPIKey,
		ActorID:           c.KeyID,
		UserID:            c.UserID,
		ActorLabel:        c.Prefix,
		Scopes:            scopes,
		Role:              models.UserRole(c.Role),
		Priority:          models.Priority(c.Priority),
		RateLimitPolicyID: c.RateLimitPolicyID,
	}, nil
}

func invalidKey() *httpx.APIError { return unauthenticated("invalid API key", "invalid_credential") }

func unauthenticated(message, code string) *httpx.APIError {
	return &httpx.APIError{Status: http.StatusUnauthorized, Message: message, Type: httpx.TypeAuthentication, Code: code}
}
