package auth

import (
	"slices"
	"sort"
	"strings"

	"github.com/adityasatwar321/nebula/packages/db/models"
)

// Scope is a capability granted to a credential.
//
// Scopes, not roles, are what the request path checks: a role is a property of a
// human, while a scope is a property of a credential, and an API key created by an
// admin for a CI job should not carry the admin's full authority.
type Scope string

// The scope vocabulary from docs/api.md §4.
const (
	// ScopeInferenceInvoke grants the OpenAI-compatible endpoints.
	ScopeInferenceInvoke Scope = "inference:invoke"
	// ScopeInferencePin grants the deployment_id override, which bypasses route
	// weighting. Separate from invoke precisely because it can defeat a canary.
	ScopeInferencePin Scope = "inference:pin"

	ScopeModelsRead  Scope = "models:read"
	ScopeModelsWrite Scope = "models:write"

	ScopeDeploymentsRead  Scope = "deployments:read"
	ScopeDeploymentsWrite Scope = "deployments:write"

	ScopeRolloutsWrite Scope = "rollouts:write"

	ScopeUsageRead Scope = "usage:read"
	ScopeAuditRead Scope = "audit:read"

	// ScopeAdmin grants key, policy, pricing, user and node management. It does
	// NOT imply the others: an admin credential that cannot invoke inference is a
	// useful thing to be able to issue.
	ScopeAdmin Scope = "admin"
)

// AllScopes lists every scope, sorted, for validation and documentation.
func AllScopes() []Scope {
	return []Scope{
		ScopeAdmin,
		ScopeAuditRead,
		ScopeDeploymentsRead,
		ScopeDeploymentsWrite,
		ScopeInferenceInvoke,
		ScopeInferencePin,
		ScopeModelsRead,
		ScopeModelsWrite,
		ScopeRolloutsWrite,
		ScopeUsageRead,
	}
}

// ValidScope reports whether s is a known scope.
func ValidScope(s Scope) bool { return slices.Contains(AllScopes(), s) }

// ParseScopes converts stored strings to scopes, rejecting unknown values.
//
// Unknown scopes are an error rather than being ignored: a key carrying a scope
// this build does not understand may have been issued by a newer version, and
// silently dropping it could either grant or deny more than intended.
func ParseScopes(raw []string) ([]Scope, error) {
	out := make([]Scope, 0, len(raw))
	var unknown []string
	for _, r := range raw {
		s := Scope(strings.TrimSpace(r))
		if !ValidScope(s) {
			unknown = append(unknown, r)
			continue
		}
		out = append(out, s)
	}
	if len(unknown) > 0 {
		return nil, &UnknownScopeError{Scopes: unknown}
	}
	return out, nil
}

// UnknownScopeError reports scopes this build does not recognise.
type UnknownScopeError struct {
	Scopes []string
}

func (e *UnknownScopeError) Error() string {
	return "unknown scope(s): " + strings.Join(e.Scopes, ", ")
}

// Strings converts scopes back to their stored form, sorted.
//
// Sorted because the output is stored in a text[] column, echoed in API responses
// and written into audit records. Set order carries no meaning, so leaving it as
// the caller happened to build it makes two identical grants look different in a
// diff and makes audit payloads noisy for no reason.
func Strings(scopes []Scope) []string {
	out := make([]string, len(scopes))
	for i, s := range scopes {
		out[i] = string(s)
	}
	sort.Strings(out)
	return out
}

// roleScopes is the maximum authority each role may delegate to a key it creates.
//
// A credential can never exceed the authority of the identity that created it,
// which is what stops a developer minting an admin key for themselves.
var roleScopes = map[models.UserRole][]Scope{
	models.RoleOwner: AllScopes(),
	models.RoleAdmin: AllScopes(),
	models.RoleDeveloper: {
		ScopeInferenceInvoke, ScopeInferencePin,
		ScopeModelsRead, ScopeModelsWrite,
		ScopeDeploymentsRead, ScopeDeploymentsWrite,
		ScopeRolloutsWrite,
		ScopeUsageRead,
	},
	models.RoleViewer: {
		ScopeInferenceInvoke,
		ScopeModelsRead,
		ScopeDeploymentsRead,
		ScopeUsageRead,
	},
}

// ScopesForRole returns the scopes a role may hold or delegate, sorted.
func ScopesForRole(r models.UserRole) []Scope {
	s := roleScopes[r]
	out := make([]Scope, len(s))
	copy(out, s)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// RoleCanGrant reports whether a role may issue a credential carrying want.
func RoleCanGrant(r models.UserRole, want []Scope) ([]Scope, bool) {
	allowed := ScopesForRole(r)
	var denied []Scope
	for _, w := range want {
		if !slices.Contains(allowed, w) {
			denied = append(denied, w)
		}
	}
	return denied, len(denied) == 0
}

// DefaultScopesForRole is what a key gets when the caller does not ask for
// specific scopes. Deliberately narrow: inference only, because that is what the
// overwhelming majority of keys are for, and a key should not carry authority
// nobody asked for.
func DefaultScopesForRole(r models.UserRole) []Scope {
	switch r {
	case models.RoleViewer:
		return []Scope{ScopeInferenceInvoke}
	default:
		return []Scope{ScopeInferenceInvoke}
	}
}
