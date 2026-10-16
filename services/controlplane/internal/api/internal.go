package api

import (
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/adityasatwar321/nebula/packages/auth"
	"github.com/adityasatwar321/nebula/packages/httpx"
	"github.com/adityasatwar321/nebula/services/controlplane/internal/store"
)

// The internal surface: endpoints other NEBULA services call, never a client.
//
// It is mounted under /internal/v1 and deliberately absent from
// packages/api/openapi.yaml, which documents the public contract. It is described
// in docs/api.md §6a instead. Three things keep it internal:
//
//   - every route requires a signed service identity (X-Nebula-Auth-Context with
//     actor_type "service"), which only a holder of the internal secret can mint;
//   - the gateway never proxies /internal/ paths, so it is unreachable through the
//     public edge;
//   - NetworkPolicy admits only the gateway to the control plane (Phase 5).

// HeaderRevokedKeyPrefix is set on a successful revocation. The gateway's admin
// proxy consumes it to drop the key from the shared credential cache at once, so
// revocation does not wait out the cache TTL (docs/security-boundaries.md §3). The
// gateway strips it before the response reaches the client.
const HeaderRevokedKeyPrefix = "X-Nebula-Revoked-Key-Prefix"

// verifyContext checks a signed context and returns the identity in it.
func (a *API) verifyContext(r *http.Request) (auth.Identity, error) {
	if a.Signer == nil {
		return auth.Identity{}, unauthenticated(
			"signed internal identities are not accepted: this control plane has no internal secret",
			"internal_context_unsupported")
	}
	v, err := a.Signer.Verify(r.Header.Get(auth.HeaderAuthContext), auth.AudienceControlPlane)
	if err != nil {
		// The precise reason is logged, not returned: a caller probing B2 learns only
		// that it failed.
		a.logger(r).WarnContext(r.Context(), "rejected internal auth context",
			slog.String("cause", err.Error()))
		return auth.Identity{}, unauthenticated("the internal identity did not verify", "invalid_internal_context")
	}
	return v.Identity, nil
}

// requireService admits only a verified service identity.
func (a *API) requireService(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(auth.HeaderAuthContext) == "" {
			a.fail(w, r, unauthenticated("this endpoint requires a signed service identity", "missing_internal_context"))
			return
		}
		ident, err := a.verifyContext(r)
		if err != nil {
			a.fail(w, r, err)
			return
		}
		if ident.ActorType != auth.ActorService {
			// A tenant identity, even a valid one, must not reach the internal
			// surface: it would let a proxied admin call read another org's keys.
			a.fail(w, r, forbidden("this endpoint is for NEBULA services only", "service_only"))
			return
		}
		next.ServeHTTP(w, r)
	})
}

// InternalRoutes lists the internal surface, for tests and for docs/api.md.
func (a *API) InternalRoutes() []Route {
	return build([]Route{
		{Pattern: "GET /internal/v1/credentials/{prefix}", handler: a.lookupCredential},
		{Pattern: "GET /internal/v1/routing-table", handler: a.routingTable},
	})
}

func (a *API) mountInternal(mux *http.ServeMux) {
	if a.Signer == nil {
		return
	}
	for _, route := range a.InternalRoutes() {
		mux.Handle(route.Pattern, a.requireService(a.handler(route.handler)))
	}
}

// credentialResponse is everything the gateway needs to authenticate and admit a
// request made with one key, in one round trip.
//
// It includes the stored HMAC. That is deliberate and bounded: the hash is useless
// without the pepper, the gateway holds the pepper anyway because it verifies keys
// on the hot path (docs/security-boundaries.md §2, B1 step 4), and returning the
// hash rather than verifying here keeps the plaintext key from ever crossing a
// second network hop.
type credentialResponse struct {
	KeyID             uuid.UUID          `json:"key_id"`
	Prefix            string             `json:"prefix"`
	KeyHash           []byte             `json:"key_hash"`
	OrgID             uuid.UUID          `json:"org_id"`
	OrgSlug           string             `json:"org_slug"`
	UserID            *uuid.UUID         `json:"user_id,omitempty"`
	Role              string             `json:"role,omitempty"`
	Scopes            []string           `json:"scopes"`
	Priority          string             `json:"priority"`
	ExpiresAt         *time.Time         `json:"expires_at,omitempty"`
	RevokedAt         *time.Time         `json:"revoked_at,omitempty"`
	RateLimitPolicyID *uuid.UUID         `json:"rate_limit_policy_id,omitempty"`
	RateLimitPolicy   *ratePolicyPayload `json:"rate_limit_policy,omitempty"`
}

// ratePolicyPayload carries a policy's limits. A nil field is "no limit on this
// dimension from the policy", and the gateway falls back to its default for it.
type ratePolicyPayload struct {
	Name              string `json:"name"`
	RequestsPerMinute *int32 `json:"requests_per_minute,omitempty"`
	TokensPerMinute   *int32 `json:"tokens_per_minute,omitempty"`
	MaxConcurrency    *int32 `json:"max_concurrency,omitempty"`
	MaxQueueDepth     *int32 `json:"max_queue_depth,omitempty"`
}

// lookupCredential returns a key's record by prefix.
//
// Revoked and expired keys are returned, not hidden, with their timestamps: the
// gateway must be able to cache "this key is revoked" and answer the specific 401
// code, and a 404 would be cached as "unknown key" instead.
func (a *API) lookupCredential(w http.ResponseWriter, r *http.Request) error {
	prefix := r.PathValue("prefix")
	if _, err := auth.ParsePrefix(prefix + fixedSecretPad); err != nil {
		return httpx.ErrInvalidRequest("prefix is not a valid API key prefix", "invalid_prefix", "prefix")
	}
	ctx := r.Context()

	key, err := a.Store.APIKeys.GetByPrefix(ctx, a.Store.Pool(), prefix)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return httpx.ErrNotFound("no API key has this prefix")
		}
		return httpx.ErrInternal(err)
	}

	org, err := a.Store.Organizations.Get(ctx, a.Store.Pool(), key.OrgID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// The organization was deleted and the key outlived it. Report the key as
			// unknown: there is no tenant left for it to act in.
			return httpx.ErrNotFound("no API key has this prefix")
		}
		return httpx.ErrInternal(err)
	}

	ident, err := a.identityFor(ctx, key)
	if err != nil {
		return err
	}

	resp := credentialResponse{
		KeyID:             key.ID,
		Prefix:            key.Prefix,
		KeyHash:           key.KeyHash,
		OrgID:             key.OrgID,
		OrgSlug:           org.Slug,
		UserID:            key.UserID,
		Role:              string(ident.Role),
		Scopes:            key.Scopes,
		Priority:          string(key.Priority),
		ExpiresAt:         key.ExpiresAt,
		RevokedAt:         key.RevokedAt,
		RateLimitPolicyID: key.RateLimitPolicyID,
	}
	if key.RateLimitPolicyID != nil {
		p, err := a.Store.RatePolicies.Get(ctx, a.Store.Pool(), key.OrgID, *key.RateLimitPolicyID)
		switch {
		case err == nil:
			resp.RateLimitPolicy = &ratePolicyPayload{
				Name:              p.Name,
				RequestsPerMinute: p.RequestsPerMinute,
				TokensPerMinute:   p.TokensPerMinute,
				MaxConcurrency:    p.MaxConcurrency,
				MaxQueueDepth:     p.MaxQueueDepth,
			}
		case errors.Is(err, store.ErrNotFound):
			// ON DELETE SET NULL makes this unreachable in a consistent database;
			// if it happens anyway the gateway applies its defaults, which is the
			// documented behaviour for a key with no policy.
		default:
			return httpx.ErrInternal(err)
		}
	}

	// A lookup means the key is in use behind a gateway. Recording it here keeps
	// last_used_at meaningful for keys that never call the control plane directly;
	// its resolution is the gateway's cache TTL plus the touch throttle.
	if key.RevokedAt == nil {
		a.touch(ctx, key.ID)
	}

	a.write(w, r, http.StatusOK, resp)
	return nil
}

// fixedSecretPad turns a bare prefix into a string of full key length, so the
// prefix is validated by the one parser that defines the key format rather than
// by a second copy of its rules.
const fixedSecretPad = "0000000000000000000000000000000000000000000"
