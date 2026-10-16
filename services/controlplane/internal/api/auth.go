package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/adityasatwar321/nebula/packages/auth"
	"github.com/adityasatwar321/nebula/packages/db/models"
	"github.com/adityasatwar321/nebula/packages/httpx"
	"github.com/adityasatwar321/nebula/packages/telemetry"
	"github.com/adityasatwar321/nebula/services/controlplane/internal/store"
)

// authenticate resolves the caller's credential into an auth.Identity.
//
// The sequence matters, and each step is there for a reason:
//
//  1. Parse the presented key's SHAPE before touching the database. A malformed
//     bearer token costs no query, which is what keeps a flood of junk tokens from
//     becoming a flood of database round-trips.
//  2. Look the key up by its indexed prefix — one indexed equality, not a scan of
//     every key computing a hash.
//  3. Verify in constant time. The verification runs on a cache hit too: the cache
//     is keyed by prefix, and a right prefix with a wrong secret must not
//     authenticate.
//  4. Reject revoked and expired keys AFTER verification, so a caller cannot learn
//     that a prefix exists by presenting garbage against it.
//
// The org comes from the key row alone. Nothing downstream may read a tenant from
// the request (docs/security-boundaries.md §2, B2).
func (a *API) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var ident auth.Identity
		if r.Header.Get(auth.HeaderAuthContext) != "" {
			// The gateway authenticated the caller and signed the result (B2). The
			// signed context is used INSTEAD of any bearer token: a request carrying
			// both is one the gateway built, and the key has already been checked.
			v, err := a.verifyContext(r)
			if err != nil {
				a.fail(w, r, err)
				return
			}
			if v.ActorType == auth.ActorService {
				// A service identity has no tenant and no scopes. It may call the
				// internal surface and nothing else.
				a.fail(w, r, forbidden("a service identity cannot call tenant endpoints", "service_identity"))
				return
			}
			ident = v
		} else {
			presented, err := bearerToken(r)
			if err != nil {
				a.fail(w, r, err)
				return
			}
			ident, err = a.resolveKey(r.Context(), presented)
			if err != nil {
				a.fail(w, r, err)
				return
			}
		}

		ctx := auth.WithIdentity(r.Context(), ident)
		// The org id joins the correlation context so every log line for this
		// request carries the tenant without a handler passing it along.
		ctx = telemetry.WithOrgID(ctx, ident.OrgID.String())
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// bearerToken extracts an API key from the Authorization header.
//
// Only nbk_ keys are accepted in Phase 2. Dashboard session JWTs share this header
// (docs/api.md §1) and arrive with the dashboard in Phase 7; a token that is not
// an API key is rejected with a specific code rather than a generic 401, because
// "your credential type is not supported yet" and "your key is wrong" are
// different problems for the caller.
func bearerToken(r *http.Request) (string, error) {
	h := r.Header.Get("Authorization")
	if h == "" {
		return "", unauthenticated(
			"an API key is required: send Authorization: Bearer nbk_...", "missing_credential")
	}
	scheme, value, ok := strings.Cut(h, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return "", unauthenticated(
			"Authorization must use the Bearer scheme", "malformed_credential")
	}
	value = strings.TrimSpace(value)
	if !strings.HasPrefix(value, auth.KeyPrefix) {
		return "", unauthenticated(
			"only API keys are accepted by this endpoint; session tokens arrive with the dashboard",
			"unsupported_credential")
	}
	return value, nil
}

// resolveKey turns a presented key into an identity.
func (a *API) resolveKey(ctx context.Context, presented string) (auth.Identity, error) {
	prefix, err := auth.ParsePrefix(presented)
	if err != nil {
		// A shape error is reported as an ordinary authentication failure: the
		// caller learns their key is not valid, not how keys are shaped.
		return auth.Identity{}, unauthenticated("invalid API key", "invalid_credential")
	}

	if entry, ok := a.keys.get(prefix); ok {
		if !a.Hasher.Verify(presented, entry.hash) {
			return auth.Identity{}, unauthenticated("invalid API key", "invalid_credential")
		}
		if err := usable(entry.key); err != nil {
			return auth.Identity{}, err
		}
		a.touch(ctx, entry.key.ID)
		return entry.identity, nil
	}

	key, err := a.Store.APIKeys.GetByPrefix(ctx, a.Store.Pool(), prefix)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// Verify against a dummy hash anyway, so an unknown prefix and a known
			// one cost the same time. Without this, response latency distinguishes
			// them and becomes an enumeration oracle.
			a.Hasher.Verify(presented, make([]byte, auth.HashLen))
			return auth.Identity{}, unauthenticated("invalid API key", "invalid_credential")
		}
		return auth.Identity{}, httpx.ErrInternal(err)
	}

	if !a.Hasher.Verify(presented, key.KeyHash) {
		return auth.Identity{}, unauthenticated("invalid API key", "invalid_credential")
	}
	if err := usable(*key); err != nil {
		return auth.Identity{}, err
	}

	ident, err := a.identityFor(ctx, key)
	if err != nil {
		return auth.Identity{}, err
	}

	a.keys.put(prefix, cacheEntry{identity: ident, hash: key.KeyHash, key: *key}, a.Config.Auth.KeyCacheTTL.Duration())
	a.touch(ctx, key.ID)
	return ident, nil
}

// usable reports why a verified key may still not authenticate.
func usable(k models.APIKey) error {
	switch {
	case k.RevokedAt != nil:
		return unauthenticated("this API key has been revoked", "credential_revoked")
	case k.ExpiresAt != nil && !time.Now().Before(*k.ExpiresAt):
		return unauthenticated("this API key has expired", "credential_expired")
	}
	return nil
}

// identityFor assembles the identity behind a verified key.
//
// The key's scopes are authoritative, not the owning user's role: an API key
// created by an admin for a CI job carries what it was granted, nothing more. The
// role is loaded only so an interface can display who the key belongs to, and is
// deliberately not consulted for authorisation.
func (a *API) identityFor(ctx context.Context, key *models.APIKey) (auth.Identity, error) {
	scopes, err := auth.ParseScopes(key.Scopes)
	if err != nil {
		// Scopes are validated on creation, so an unknown one here means the row was
		// edited outside the API. Refusing is the safe reading: the alternative is
		// silently dropping authority the operator believed they had granted.
		return auth.Identity{}, httpx.ErrInternal(err)
	}

	ident := auth.Identity{
		OrgID:             key.OrgID,
		ActorType:         models.ActorAPIKey,
		ActorID:           key.ID,
		ActorLabel:        key.Prefix,
		Scopes:            scopes,
		Priority:          key.Priority,
		RateLimitPolicyID: key.RateLimitPolicyID,
		UserID:            key.UserID,
	}

	if key.UserID != nil {
		user, err := a.Store.Users.Get(ctx, a.Store.Pool(), key.OrgID, *key.UserID)
		switch {
		case err == nil:
			ident.Role = user.Role
		case errors.Is(err, store.ErrNotFound):
			// The owning user was removed but the key was not revoked. The key keeps
			// working with its own scopes, which is the documented behaviour: killing
			// live traffic as a side effect of an HR change is worse than a key with
			// no human attached, and revocation is the explicit way to kill it.
		default:
			return auth.Identity{}, httpx.ErrInternal(err)
		}
	}
	return ident, nil
}

// touch records last_used_at, throttled inside the repository so a hot key does
// not write on every request. Failure is logged and ignored: a bookkeeping write
// must not fail an authenticated request.
func (a *API) touch(ctx context.Context, id uuid.UUID) {
	if err := a.Store.APIKeys.TouchUsed(ctx, a.Store.Pool(), id, touchInterval); err != nil {
		a.Logger.WarnContext(ctx, "recording api key usage failed", slog.String("cause", err.Error()))
	}
}

// touchInterval is how stale last_used_at is allowed to be.
const touchInterval = time.Minute

// ---------------------------------------------------------------------------
// scope enforcement
// ---------------------------------------------------------------------------

// require builds middleware that refuses a caller lacking any of the scopes.
//
// Scopes are ANDed: an endpoint that both reads a model and writes a deployment
// requires both. ScopeAdmin does NOT satisfy the others — an admin credential that
// cannot invoke inference is a useful thing to be able to issue — so admin-only
// endpoints ask for ScopeAdmin explicitly.
func (a *API) require(scopes ...auth.Scope) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ident, err := auth.FromContext(r.Context())
			if err != nil {
				// The endpoint was mounted without the authentication middleware.
				// A 500 is correct: it is a wiring bug, not a client error.
				a.fail(w, r, httpx.ErrInternal(err))
				return
			}
			for _, s := range scopes {
				if !ident.Has(s) {
					a.fail(w, r, forbidden(
						"this API key does not carry the "+string(s)+" scope", "insufficient_scope"))
					return
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}

// ---------------------------------------------------------------------------
// key cache
// ---------------------------------------------------------------------------

// cacheEntry is a verified key and the identity built from it.
type cacheEntry struct {
	identity auth.Identity
	hash     []byte
	key      models.APIKey
	expires  time.Time
}

// keyCache is a small TTL cache of verified keys.
//
// It exists because otherwise every request costs a key lookup plus a user lookup,
// and the control plane's own dashboard polls. Two properties keep it honest:
//
//   - The stored hash is still verified on every hit, so the cache never decides
//     whether a secret is correct — only whether the row has to be re-read.
//   - The TTL is short and bounded by configuration, because revocation is not
//     pushed until Phase 4. Until then this TTL is the entire revocation delay, so
//     it is a documented number rather than an accident.
//
// A map with a mutex, not an LRU: the working set is a few hundred keys, and
// eviction only has to bound memory, not maximise hit rate. When the cache is
// full, the oldest-expiring entries are dropped.
type keyCache struct {
	mu      sync.Mutex
	entries map[string]cacheEntry
	max     int
	now     func() time.Time
}

func newKeyCache(capacity int, now func() time.Time) *keyCache {
	if capacity < 1 {
		capacity = 1
	}
	if now == nil {
		now = time.Now
	}
	return &keyCache{entries: make(map[string]cacheEntry, capacity), max: capacity, now: now}
}

func (c *keyCache) get(prefix string) (cacheEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[prefix]
	if !ok {
		return cacheEntry{}, false
	}
	if !c.now().Before(e.expires) {
		delete(c.entries, prefix)
		return cacheEntry{}, false
	}
	return e, true
}

func (c *keyCache) put(prefix string, e cacheEntry, ttl time.Duration) {
	if ttl <= 0 {
		return
	}
	e.expires = c.now().Add(ttl)

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) >= c.max {
		c.evictLocked()
	}
	c.entries[prefix] = e
}

// forget drops an entry, used when a key is revoked through this process so the
// revocation is immediate locally rather than waiting out the TTL.
func (c *keyCache) forget(prefix string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, prefix)
}

// evictLocked removes expired entries, and if that frees nothing, the entry
// closest to expiry.
func (c *keyCache) evictLocked() {
	now := c.now()
	freed := false
	for k, e := range c.entries {
		if !now.Before(e.expires) {
			delete(c.entries, k)
			freed = true
		}
	}
	if freed {
		return
	}
	var oldestKey string
	var oldest time.Time
	for k, e := range c.entries {
		if oldestKey == "" || e.expires.Before(oldest) {
			oldestKey, oldest = k, e.expires
		}
	}
	if oldestKey != "" {
		delete(c.entries, oldestKey)
	}
}
