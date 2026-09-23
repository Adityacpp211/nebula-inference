package controlplane_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/adityasatwar321/nebula/packages/auth"
	"github.com/adityasatwar321/nebula/packages/db/models"
	"github.com/adityasatwar321/nebula/packages/testsupport/dbtest"
)

// Authentication failures must be indistinguishable between "no such key" and
// "wrong key", and must carry no hint about which prefixes exist. The distinction
// that IS reported is the credential TYPE, because "session tokens are not accepted
// here yet" is a different problem for the caller than "your key is wrong".
func TestAuthenticationRejections(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	// A well-formed key that does not exist. Built from the real generator so its
	// shape passes every check except the lookup.
	unknown, err := f.Hasher.Generate()
	if err != nil {
		t.Fatalf("generating a key: %v", err)
	}

	// A key with a real prefix but the wrong secret, which is the case a naive cache
	// would get wrong.
	wrongSecret := f.Key[:auth.PrefixLen] + strings.Repeat("Z", len(f.Key)-auth.PrefixLen)

	cases := []struct {
		name     string
		header   string
		wantCode string
	}{
		{"no header", "", "missing_credential"},
		{"wrong scheme", "Basic dXNlcjpwYXNz", "malformed_credential"},
		{"session token", "Bearer eyJhbGciOiJIUzI1NiJ9.e30.x", "unsupported_credential"},
		// Go trims the trailing space when the header is set, so this arrives as a
		// bare "Bearer" with no credential at all.
		{"bearer with no value", "Bearer ", "malformed_credential"},
		{"truncated key", "Bearer " + auth.KeyPrefix + "short", "invalid_credential"},
		{"unknown key", "Bearer " + unknown.Plaintext, "invalid_credential"},
		{"right prefix wrong secret", "Bearer " + wrongSecret, "invalid_credential"},
	}

	for _, c := range cases {
		req, err := http.NewRequestWithContext(dbtest.Context(t),
			http.MethodGet, f.Server.URL+"/v1/me", http.NoBody)
		if err != nil {
			t.Fatalf("building request: %v", err)
		}
		if c.header != "" {
			req.Header.Set("Authorization", c.header)
		}

		resp, err := f.Server.Client().Do(req)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		body := decodeError(t, resp)
		resp.Body.Close()

		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("%s: status = %d, want 401", c.name, resp.StatusCode)
		}
		if body.Error.Code != c.wantCode {
			t.Errorf("%s: code = %q, want %q", c.name, body.Error.Code, c.wantCode)
		}
		if body.Error.Type != "authentication_error" {
			t.Errorf("%s: type = %q, want authentication_error", c.name, body.Error.Type)
		}
		// The message must not echo the credential, or a proxy log becomes a
		// credential store.
		if strings.Contains(body.Error.Message, f.Key) ||
			strings.Contains(body.Error.Message, unknown.Plaintext) {
			t.Errorf("%s: the error message contains the presented key", c.name)
		}
	}

	// The valid key still works, so none of the above is a blanket refusal.
	if code := f.do(t, http.MethodGet, "/v1/me", nil, nil); code != http.StatusOK {
		t.Errorf("GET /v1/me with a valid key = %d, want 200", code)
	}
}

// A revoked key stops working, and the process that revoked it stops honouring it at
// once rather than after the cache TTL.
func TestRevocationTakesEffectImmediately(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	var created struct {
		ID  uuid.UUID `json:"id"`
		Key string    `json:"key"`
	}
	if code := f.do(t, http.MethodPost, "/v1/api-keys", map[string]any{
		"name":   "throwaway",
		"scopes": []string{"models:read"},
	}, &created); code != http.StatusCreated {
		t.Fatalf("creating a key = %d, want 201", code)
	}
	if created.Key == "" {
		t.Fatal("the plaintext key was not returned; it can never be recovered")
	}

	// It works, which also puts it in the cache.
	if code := f.doWithKey(t, created.Key, http.MethodGet, "/v1/models/registry", nil, nil); code != http.StatusOK {
		t.Fatalf("using the new key = %d, want 200", code)
	}

	if code := f.do(t, http.MethodDelete, "/v1/api-keys/"+created.ID.String(), nil, nil); code != http.StatusNoContent {
		t.Fatalf("revoking = %d, want 204", code)
	}

	// Immediately, with no sleep: the handler drops the cache entry rather than
	// waiting out the TTL.
	var env apiError
	code := f.doWithKey(t, created.Key, http.MethodGet, "/v1/models/registry", nil, &env)
	if code != http.StatusUnauthorized {
		t.Fatalf("a revoked key returned %d, want 401", code)
	}
	if env.Error.Code != "credential_revoked" {
		t.Errorf("code = %q, want credential_revoked", env.Error.Code)
	}

	// Revoking twice is idempotent: an operator retrying after a timeout must not get
	// an error.
	if code := f.do(t, http.MethodDelete, "/v1/api-keys/"+created.ID.String(), nil, nil); code != http.StatusNoContent {
		t.Errorf("revoking twice = %d, want 204", code)
	}

	// And the revoked key is still listed, with revoked_at set: a key that vanishes
	// from the list cannot be audited.
	var page struct {
		Data []struct {
			ID        uuid.UUID  `json:"id"`
			RevokedAt *time.Time `json:"revoked_at"`
		} `json:"data"`
	}
	f.do(t, http.MethodGet, "/v1/api-keys", nil, &page)
	found := false
	for _, k := range page.Data {
		if k.ID == created.ID {
			found = true
			if k.RevokedAt == nil {
				t.Error("the revoked key has no revoked_at")
			}
		}
	}
	if !found {
		t.Error("the revoked key is missing from the listing")
	}
}

// An expired key is refused, and the reason is reported separately from revocation so
// an operator can tell a rotation failure from a deliberate kill.
func TestExpiredKeyIsRefused(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	ctx := dbtest.Context(t)

	var created struct {
		ID  uuid.UUID `json:"id"`
		Key string    `json:"key"`
	}
	if code := f.do(t, http.MethodPost, "/v1/api-keys", map[string]any{
		"name":       "expiring",
		"scopes":     []string{"models:read"},
		"expires_at": time.Now().Add(time.Hour).Format(time.RFC3339),
	}, &created); code != http.StatusCreated {
		t.Fatalf("creating a key = %d", code)
	}

	// Move the expiry into the past directly, since the API refuses to create one
	// already expired — which is itself worth asserting.
	if _, err := f.Pool.Exec(ctx,
		`UPDATE api_keys SET expires_at = now() - interval '1 minute' WHERE id = $1`,
		created.ID); err != nil {
		t.Fatalf("ageing the key: %v", err)
	}

	var env apiError
	if code := f.doWithKey(t, created.Key, http.MethodGet, "/v1/models/registry", nil, &env); code != http.StatusUnauthorized {
		t.Fatalf("an expired key returned %d, want 401", code)
	}
	if env.Error.Code != "credential_expired" {
		t.Errorf("code = %q, want credential_expired", env.Error.Code)
	}

	env = f.expectError(t, http.StatusBadRequest, http.MethodPost, "/v1/api-keys", map[string]any{
		"name":       "already-expired",
		"expires_at": time.Now().Add(-time.Hour).Format(time.RFC3339),
	})
	if env.Error.Param != "expires_at" {
		t.Errorf("param = %q, want expires_at", env.Error.Param)
	}
}

// A credential cannot grant a scope it does not hold. Without this rule admin is a
// privilege-escalation primitive and scoping is decorative.
func TestScopeEscalationIsRefused(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	// A limited key that can administer but cannot write deployments.
	var limited struct {
		ID  uuid.UUID `json:"id"`
		Key string    `json:"key"`
	}
	if code := f.do(t, http.MethodPost, "/v1/api-keys", map[string]any{
		"name":   "limited admin",
		"scopes": []string{"admin", "models:read"},
	}, &limited); code != http.StatusCreated {
		t.Fatalf("creating the limited key = %d", code)
	}

	// It may mint a key with its own scopes.
	var narrow struct {
		Scopes []string `json:"scopes"`
	}
	if code := f.doWithKey(t, limited.Key, http.MethodPost, "/v1/api-keys", map[string]any{
		"name":   "narrower",
		"scopes": []string{"models:read"},
	}, &narrow); code != http.StatusCreated {
		t.Fatalf("minting a narrower key = %d, want 201", code)
	}

	// It may not mint one carrying authority it lacks.
	var env apiError
	code := f.doWithKey(t, limited.Key, http.MethodPost, "/v1/api-keys", map[string]any{
		"name":   "wider",
		"scopes": []string{"deployments:write"},
	}, &env)
	if code != http.StatusForbidden {
		t.Fatalf("minting a wider key = %d, want 403", code)
	}
	if env.Error.Code != "scope_escalation" {
		t.Errorf("code = %q, want scope_escalation", env.Error.Code)
	}
	if !strings.Contains(env.Error.Message, "deployments:write") {
		t.Errorf("the refusal does not name the offending scope: %q", env.Error.Message)
	}

	// An unknown scope is rejected rather than dropped.
	env = f.expectError(t, http.StatusBadRequest, http.MethodPost, "/v1/api-keys", map[string]any{
		"name":   "typo",
		"scopes": []string{"models:delete"},
	})
	if env.Error.Param != "scopes" {
		t.Errorf("param = %q, want scopes", env.Error.Param)
	}
}

// Each endpoint enforces its scope. Checked against a real key per scope rather than
// by reading the route table, so the middleware wiring is what is under test.
func TestScopesAreEnforcedPerEndpoint(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	_, versionID := f.createReadyVersion(t, "scoped", "v1")
	deploymentID := f.createDeployment(t, "scoped-dep", versionID)

	// A read-only key: every read scope, no write scope, no admin.
	var reader struct {
		Key string `json:"key"`
	}
	if code := f.do(t, http.MethodPost, "/v1/api-keys", map[string]any{
		"name":   "reader",
		"scopes": []string{"models:read", "deployments:read", "usage:read"},
	}, &reader); code != http.StatusCreated {
		t.Fatalf("creating the reader key = %d", code)
	}

	allowed := []struct {
		method, path string
	}{
		{http.MethodGet, "/v1/me"},
		{http.MethodGet, "/v1/lifecycle/deployment-states"},
		{http.MethodGet, "/v1/models/registry"},
		{http.MethodGet, "/v1/deployments"},
		{http.MethodGet, "/v1/deployments/" + deploymentID.String()},
		{http.MethodGet, "/v1/deployments/" + deploymentID.String() + "/status"},
		{http.MethodGet, "/v1/deployments/" + deploymentID.String() + "/revisions"},
		{http.MethodGet, "/v1/deployments/" + deploymentID.String() + "/transitions"},
	}
	for _, c := range allowed {
		if code := f.doWithKey(t, reader.Key, c.method, c.path, nil, nil); code != http.StatusOK {
			t.Errorf("%s %s with a read key = %d, want 200", c.method, c.path, code)
		}
	}

	refused := []struct {
		method, path string
		body         any
	}{
		{http.MethodGet, "/v1/users", nil},
		{http.MethodGet, "/v1/api-keys", nil},
		{http.MethodGet, "/v1/audit-logs", nil},
		{http.MethodPost, "/v1/models", map[string]any{"name": "nope", "task": "chat"}},
		{http.MethodPost, "/v1/deployments", map[string]any{"name": "nope", "model_version_id": versionID}},
		{http.MethodDelete, "/v1/deployments/" + deploymentID.String(), nil},
		{http.MethodPost, "/v1/deployments/" + deploymentID.String() + "/scale",
			map[string]any{"replicas": 1}},
		{http.MethodPost, "/v1/deployments/" + deploymentID.String() + "/stop", nil},
		{http.MethodPost, "/v1/deployments/" + deploymentID.String() + "/transition",
			map[string]any{"from": "pending", "to": "stopping", "reason": "StopRequested"}},
	}
	for _, c := range refused {
		var env apiError
		code := f.doWithKey(t, reader.Key, c.method, c.path, c.body, &env)
		if code != http.StatusForbidden {
			t.Errorf("%s %s with a read key = %d, want 403", c.method, c.path, code)
			continue
		}
		if env.Error.Code != "insufficient_scope" {
			t.Errorf("%s %s: code = %q, want insufficient_scope", c.method, c.path, env.Error.Code)
		}
	}

	// Nothing the read key was refused actually happened.
	var check struct {
		Status struct {
			State string `json:"state"`
		} `json:"status"`
		Spec struct {
			DesiredReplicas int32 `json:"desired_replicas"`
		} `json:"spec"`
	}
	f.do(t, http.MethodGet, "/v1/deployments/"+deploymentID.String(), nil, &check)
	if check.Status.State != string(models.DeploymentPending) {
		t.Errorf("state = %q after refused writes, want pending", check.Status.State)
	}
	if check.Spec.DesiredReplicas != 2 {
		t.Errorf("desired_replicas = %d after a refused scale, want 2", check.Spec.DesiredReplicas)
	}
}

// /v1/me needs no scope, because a caller that has just been refused must be able to
// discover what its credential actually carries.
func TestWhoAmINeedsNoScope(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	// A key with no scopes at all is still allowed to describe itself.
	var scopeless struct {
		Key string `json:"key"`
	}
	if code := f.do(t, http.MethodPost, "/v1/api-keys", map[string]any{
		"name":   "scopeless",
		"scopes": []string{},
	}, &scopeless); code != http.StatusCreated {
		// An empty scope list falls back to the role's defaults, which an owner key has.
		t.Fatalf("creating a key with an empty scope list = %d", code)
	}

	var me struct {
		OrgID      uuid.UUID `json:"org_id"`
		ActorType  string    `json:"actor_type"`
		ActorID    uuid.UUID `json:"actor_id"`
		ActorLabel string    `json:"actor_label"`
		Role       string    `json:"role"`
		Scopes     []string  `json:"scopes"`
		Priority   string    `json:"priority"`
	}
	if code := f.do(t, http.MethodGet, "/v1/me", nil, &me); code != http.StatusOK {
		t.Fatalf("GET /v1/me = %d", code)
	}

	if me.OrgID != f.OrgID {
		t.Errorf("org_id = %v, want %v", me.OrgID, f.OrgID)
	}
	if me.ActorType != string(models.ActorAPIKey) {
		t.Errorf("actor_type = %q, want api_key", me.ActorType)
	}
	if me.ActorID != f.KeyID {
		t.Errorf("actor_id = %v, want %v", me.ActorID, f.KeyID)
	}
	// The label is the key prefix, which is safe to display.
	if !strings.HasPrefix(me.ActorLabel, auth.KeyPrefix) {
		t.Errorf("actor_label = %q, want the key prefix", me.ActorLabel)
	}
	if strings.Contains(f.Key, me.ActorLabel) && len(me.ActorLabel) >= len(f.Key) {
		t.Error("actor_label is the whole key rather than its prefix")
	}
	if me.Role != string(models.RoleOwner) {
		t.Errorf("role = %q, want owner", me.Role)
	}
	if len(me.Scopes) == 0 {
		t.Error("no scopes were reported, so a 403 would be unexplainable")
	}
	if me.Priority != string(models.PriorityNormal) {
		t.Errorf("priority = %q, want NORMAL", me.Priority)
	}
}

// A key belongs to exactly one tenant and can never see another's resources, and the
// tenant is never taken from the request.
func TestTenantIsolationAcrossTheAPI(t *testing.T) {
	t.Parallel()

	pool := dbtest.Migrated(t)
	alice := newFixtureOn(t, pool)
	bob := newFixtureOn(t, pool)

	_, aliceVersion := alice.createReadyVersion(t, "alice-iso", "v1")
	aliceDeployment := alice.createDeployment(t, "alice-dep", aliceVersion)

	_, bobVersion := bob.createReadyVersion(t, "bob-iso", "v1")
	bob.createDeployment(t, "bob-dep", bobVersion)

	// Bob's listings contain only Bob's rows.
	var list struct {
		Data []struct {
			ID   uuid.UUID `json:"id"`
			Name string    `json:"name"`
		} `json:"data"`
	}
	if code := bob.do(t, http.MethodGet, "/v1/deployments", nil, &list); code != http.StatusOK {
		t.Fatalf("GET /v1/deployments = %d", code)
	}
	if len(list.Data) != 1 {
		t.Fatalf("Bob sees %d deployments, want 1", len(list.Data))
	}
	if list.Data[0].Name != "bob-dep" {
		t.Errorf("Bob sees %q", list.Data[0].Name)
	}

	// Every operation on Alice's deployment is a 404 for Bob, including the writes:
	// a 403 would confirm the id exists.
	paths := []struct {
		method, path string
		body         any
	}{
		{http.MethodGet, "/v1/deployments/" + aliceDeployment.String(), nil},
		{http.MethodGet, "/v1/deployments/" + aliceDeployment.String() + "/status", nil},
		{http.MethodGet, "/v1/deployments/" + aliceDeployment.String() + "/revisions", nil},
		{http.MethodGet, "/v1/deployments/" + aliceDeployment.String() + "/transitions", nil},
		{http.MethodDelete, "/v1/deployments/" + aliceDeployment.String(), nil},
		{http.MethodPatch, "/v1/deployments/" + aliceDeployment.String(),
			map[string]any{"generation": 1, "replicas": 9}},
		{http.MethodPost, "/v1/deployments/" + aliceDeployment.String() + "/scale",
			map[string]any{"replicas": 9}},
		{http.MethodPost, "/v1/deployments/" + aliceDeployment.String() + "/stop", nil},
		{http.MethodPost, "/v1/deployments/" + aliceDeployment.String() + "/transition",
			map[string]any{"from": "pending", "to": "stopping", "reason": "StopRequested"}},
	}
	for _, c := range paths {
		if code := bob.do(t, c.method, c.path, c.body, nil); code != http.StatusNotFound {
			t.Errorf("%s %s as the wrong tenant = %d, want 404", c.method, c.path, code)
		}
	}

	// Alice's deployment is untouched.
	var check struct {
		Generation int64 `json:"generation"`
		Spec       struct {
			DesiredReplicas int32 `json:"desired_replicas"`
		} `json:"spec"`
		Status struct {
			State string `json:"state"`
		} `json:"status"`
	}
	alice.do(t, http.MethodGet, "/v1/deployments/"+aliceDeployment.String(), nil, &check)
	if check.Spec.DesiredReplicas != 2 || check.Generation != 1 ||
		check.Status.State != string(models.DeploymentPending) {
		t.Errorf("Alice's deployment was modified: generation %d, replicas %d, state %s",
			check.Generation, check.Spec.DesiredReplicas, check.Status.State)
	}

	// Bob cannot deploy Alice's version either, which is the check that matters most:
	// it would otherwise run her weights under his tenancy.
	if code := bob.do(t, http.MethodPost, "/v1/deployments", map[string]any{
		"name":             "stolen",
		"model_version_id": aliceVersion,
	}, nil); code != http.StatusNotFound {
		t.Errorf("deploying another tenant's version = %d, want 404", code)
	}
}

// Only an owner may create or change an owner: otherwise an admin could demote every
// owner and take the organization.
func TestOwnerRoleChangesRequireAnOwner(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	// An admin user, and a key attributed to them so the identity carries their role.
	var adminUser struct {
		ID uuid.UUID `json:"id"`
	}
	if code := f.do(t, http.MethodPost, "/v1/users", map[string]any{
		"email": "admin@example.test",
		"role":  "admin",
	}, &adminUser); code != http.StatusCreated {
		t.Fatalf("creating the admin user = %d", code)
	}

	var adminKey struct {
		Key string `json:"key"`
	}
	if code := f.do(t, http.MethodPost, "/v1/api-keys", map[string]any{
		"name":    "admin key",
		"user_id": adminUser.ID,
		"scopes":  []string{"admin"},
	}, &adminKey); code != http.StatusCreated {
		t.Fatalf("creating the admin key = %d", code)
	}

	// The admin may create ordinary users.
	var developer struct {
		ID uuid.UUID `json:"id"`
	}
	if code := f.doWithKey(t, adminKey.Key, http.MethodPost, "/v1/users", map[string]any{
		"email": "dev@example.test",
		"role":  "developer",
	}, &developer); code != http.StatusCreated {
		t.Fatalf("admin creating a developer = %d, want 201", code)
	}

	// But not an owner.
	var env apiError
	if code := f.doWithKey(t, adminKey.Key, http.MethodPost, "/v1/users", map[string]any{
		"email": "usurper@example.test",
		"role":  "owner",
	}, &env); code != http.StatusForbidden {
		t.Errorf("admin creating an owner = %d, want 403", code)
	} else if env.Error.Code != "insufficient_role" {
		t.Errorf("code = %q, want insufficient_role", env.Error.Code)
	}

	// Nor promote one.
	if code := f.doWithKey(t, adminKey.Key, http.MethodPatch,
		"/v1/users/"+developer.ID.String(), map[string]any{"role": "owner"}, nil); code != http.StatusForbidden {
		t.Errorf("admin promoting to owner = %d, want 403", code)
	}

	// Nor demote the existing owner.
	if code := f.doWithKey(t, adminKey.Key, http.MethodPatch,
		"/v1/users/"+f.UserID.String(), map[string]any{"role": "viewer"}, nil); code != http.StatusForbidden {
		t.Errorf("admin demoting an owner = %d, want 403", code)
	}

	// The owner may do all of it.
	if code := f.do(t, http.MethodPatch,
		"/v1/users/"+developer.ID.String(), map[string]any{"role": "owner"}, nil); code != http.StatusOK {
		t.Errorf("owner promoting to owner = %d, want 200", code)
	}

	// And the last owner cannot be removed, or the organization becomes
	// unadministrable.
	if code := f.do(t, http.MethodDelete, "/v1/users/"+developer.ID.String(), nil, nil); code != http.StatusNoContent {
		t.Fatalf("removing one of two owners = %d, want 204", code)
	}
	if code := f.do(t, http.MethodDelete, "/v1/users/"+f.UserID.String(), nil, nil); code != http.StatusConflict {
		t.Errorf("removing the last owner = %d, want 409", code)
	}
}

// decodeError reads an error envelope from a raw response.
func decodeError(t *testing.T, resp *http.Response) apiError {
	t.Helper()
	var env apiError
	if err := decodeJSONBody(resp, &env); err != nil {
		t.Fatalf("decoding the error envelope: %v", err)
	}
	return env
}
