package auth

import (
	"context"
	"errors"
	"slices"

	"github.com/google/uuid"

	"github.com/adityasatwar321/nebula/packages/db/models"
)

// Identity is an authenticated caller.
//
// It is assembled once per request by the authentication middleware and is the
// only thing downstream code may use to decide what the caller can see or do.
// Nothing reads an org_id out of a request body or a path parameter: that is the
// most common multi-tenant bug in systems of this shape
// (docs/security-boundaries.md §2, B2).
type Identity struct {
	// OrgID is the tenant. Always set.
	OrgID uuid.UUID
	// ActorType is how the caller authenticated.
	ActorType models.ActorType
	// ActorID is the api_keys.id or users.id, for the audit trail.
	ActorID uuid.UUID
	// UserID is the human the credential belongs to, where there is one. It is
	// separate from ActorID because a key's owner and the key itself are different
	// things: the audit trail records the key, while created_by records the person.
	UserID *uuid.UUID
	// ActorLabel is the key prefix or the user's email, denormalised into audit
	// rows so they stay readable after the actor is deleted.
	ActorLabel string
	// Scopes is the credential's authority.
	Scopes []Scope
	// Role is the human role behind the credential, where there is one.
	Role models.UserRole
	// Priority is the request priority this credential carries. From the key, never
	// from the request body, so callers cannot promote themselves.
	Priority models.Priority
	// RateLimitPolicyID is applied by the gateway in Phase 4.
	RateLimitPolicyID *uuid.UUID
}

// Has reports whether the identity carries a scope.
func (i Identity) Has(s Scope) bool { return slices.Contains(i.Scopes, s) }

// HasAny reports whether the identity carries at least one of the scopes.
func (i Identity) HasAny(scopes ...Scope) bool {
	for _, s := range scopes {
		if i.Has(s) {
			return true
		}
	}
	return false
}

// IsAdmin reports whether the identity carries the admin scope.
func (i Identity) IsAdmin() bool { return i.Has(ScopeAdmin) }

// ErrNoIdentity means the context carries no authenticated caller. Returned by
// FromContext so a handler that forgot the middleware fails loudly rather than
// treating the request as anonymous.
var ErrNoIdentity = errors.New("request has no authenticated identity")

type ctxKey int

const ctxKeyIdentity ctxKey = iota

// WithIdentity returns a context carrying the authenticated caller.
func WithIdentity(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, ctxKeyIdentity, id)
}

// FromContext returns the authenticated caller.
func FromContext(ctx context.Context) (Identity, error) {
	id, ok := ctx.Value(ctxKeyIdentity).(Identity)
	if !ok {
		return Identity{}, ErrNoIdentity
	}
	return id, nil
}

// MustFromContext returns the authenticated caller, panicking when absent.
//
// For use only inside handlers mounted behind the authentication middleware,
// where an absent identity is a wiring bug rather than a runtime condition. The
// panic is caught by the recovery middleware and returns a 500, which is the
// correct outcome for a programming error.
func MustFromContext(ctx context.Context) Identity {
	id, err := FromContext(ctx)
	if err != nil {
		panic("auth.MustFromContext: " + err.Error())
	}
	return id
}
