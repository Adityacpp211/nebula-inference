package auth_test

import (
	"slices"
	"testing"

	"github.com/adityasatwar321/nebula/packages/auth"
	"github.com/adityasatwar321/nebula/packages/db/models"
)

func TestParseScopesRejectsUnknown(t *testing.T) {
	t.Parallel()

	got, err := auth.ParseScopes([]string{"models:read", "deployments:write"})
	if err != nil {
		t.Fatalf("ParseScopes: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d scopes, want 2", len(got))
	}

	// Surrounding whitespace is tolerated: it comes from hand-edited configuration
	// rather than from a caller meaning a different scope.
	if _, err := auth.ParseScopes([]string{" admin "}); err != nil {
		t.Errorf("ParseScopes refused a scope with surrounding whitespace: %v", err)
	}

	// An unknown scope must be an error, not silently dropped. Dropping it would
	// mean a key created with a typo'd scope quietly carries less authority than the
	// operator believes, and the failure appears later as a mystifying 403.
	for _, bad := range [][]string{
		{"models:read", "models:delete"},
		{"Admin"},
		{""},
		{"*"},
	} {
		if _, err := auth.ParseScopes(bad); err == nil {
			t.Errorf("ParseScopes(%q) accepted an unknown scope", bad)
		}
	}
}

// admin must not imply the others. An admin credential that cannot invoke
// inference is a useful thing to be able to issue, and an implicit grant would
// make every admin key a full-access key.
func TestAdminDoesNotImplyOtherScopes(t *testing.T) {
	t.Parallel()

	id := auth.Identity{Scopes: []auth.Scope{auth.ScopeAdmin}}

	if !id.IsAdmin() {
		t.Fatal("IsAdmin() = false for a credential holding admin")
	}
	for _, s := range auth.AllScopes() {
		if s == auth.ScopeAdmin {
			continue
		}
		if id.Has(s) {
			t.Errorf("admin implied %s", s)
		}
	}
}

func TestIdentityHasAny(t *testing.T) {
	t.Parallel()

	id := auth.Identity{Scopes: []auth.Scope{auth.ScopeModelsRead, auth.ScopeUsageRead}}

	if !id.HasAny(auth.ScopeModelsWrite, auth.ScopeModelsRead) {
		t.Error("HasAny returned false when one scope matched")
	}
	if id.HasAny(auth.ScopeAdmin, auth.ScopeRolloutsWrite) {
		t.Error("HasAny returned true when nothing matched")
	}
	if id.HasAny() {
		t.Error("HasAny() with no arguments must be false: an empty requirement is not a grant")
	}
}

// A role's scopes must be a subset of what exists, and a viewer must never hold a
// write scope. The second half is the one worth testing: a typo in the role table
// would hand every read-only user the ability to delete deployments.
func TestRoleScopes(t *testing.T) {
	t.Parallel()

	all := auth.AllScopes()

	for _, role := range models.UserRoles() {
		scopes := auth.ScopesForRole(role)
		for _, s := range scopes {
			if !slices.Contains(all, s) {
				t.Errorf("role %s grants unknown scope %q", role, s)
			}
		}
	}

	viewer := auth.ScopesForRole(models.RoleViewer)
	for _, forbidden := range []auth.Scope{
		auth.ScopeAdmin, auth.ScopeModelsWrite, auth.ScopeDeploymentsWrite, auth.ScopeRolloutsWrite,
	} {
		if slices.Contains(viewer, forbidden) {
			t.Errorf("viewer holds the write scope %s", forbidden)
		}
	}

	developer := auth.ScopesForRole(models.RoleDeveloper)
	if slices.Contains(developer, auth.ScopeAdmin) {
		t.Error("developer holds admin")
	}
	if !slices.Contains(developer, auth.ScopeDeploymentsWrite) {
		t.Error("developer cannot write deployments, which is the role's whole purpose")
	}

	// Owner and admin hold everything, so the API is administrable by both.
	for _, role := range []models.UserRole{models.RoleOwner, models.RoleAdmin} {
		got := auth.ScopesForRole(role)
		for _, s := range all {
			if !slices.Contains(got, s) {
				t.Errorf("role %s is missing scope %s", role, s)
			}
		}
	}

	// An unknown role grants nothing. Failing closed matters: a role added to the
	// database but not here must not default to full authority.
	if len(auth.ScopesForRole(models.UserRole("auditor"))) != 0 {
		t.Error("an unknown role granted scopes")
	}
}

func TestRoleCanGrant(t *testing.T) {
	t.Parallel()

	// A developer cannot mint an admin key: that is the privilege-escalation path
	// this function exists to close.
	if _, ok := auth.RoleCanGrant(models.RoleDeveloper, []auth.Scope{auth.ScopeAdmin}); ok {
		t.Error("a developer was allowed to grant admin")
	}
	if refused, ok := auth.RoleCanGrant(models.RoleViewer,
		[]auth.Scope{auth.ScopeModelsRead, auth.ScopeModelsWrite}); ok {
		t.Error("a viewer was allowed to grant models:write")
	} else if !slices.Contains(refused, auth.ScopeModelsWrite) {
		t.Errorf("refused scopes %v do not name models:write", refused)
	}

	if _, ok := auth.RoleCanGrant(models.RoleAdmin, auth.AllScopes()); !ok {
		t.Error("an admin could not grant the full scope set")
	}
	if _, ok := auth.RoleCanGrant(models.RoleDeveloper,
		[]auth.Scope{auth.ScopeInferenceInvoke, auth.ScopeDeploymentsRead}); !ok {
		t.Error("a developer could not grant scopes it holds itself")
	}
}

func TestStringsIsStableAndRoundTrips(t *testing.T) {
	t.Parallel()

	in := []auth.Scope{auth.ScopeUsageRead, auth.ScopeAdmin, auth.ScopeModelsRead}
	first := auth.Strings(in)
	second := auth.Strings(in)

	if !slices.Equal(first, second) {
		t.Fatalf("Strings is not deterministic: %v then %v", first, second)
	}
	if !slices.IsSorted(first) {
		// Stored scopes are compared in tests and shown in audit records; an
		// unstable order makes both noisy.
		t.Errorf("Strings returned an unsorted slice: %v", first)
	}

	back, err := auth.ParseScopes(first)
	if err != nil {
		t.Fatalf("ParseScopes on the output of Strings: %v", err)
	}
	if len(back) != len(in) {
		t.Fatalf("round trip produced %d scopes, want %d", len(back), len(in))
	}
}
