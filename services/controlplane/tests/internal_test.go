package controlplane_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/adityasatwar321/nebula/packages/auth"
	"github.com/adityasatwar321/nebula/packages/db"
	"github.com/adityasatwar321/nebula/packages/db/models"
	"github.com/adityasatwar321/nebula/packages/testsupport/dbtest"
	"github.com/adityasatwar321/nebula/services/controlplane/internal/store"
)

// testInternalSecret is the secret the gateway and the control plane share in
// these tests (docs/security-boundaries.md §2, B2).
const testInternalSecret = "integration-internal-secret-0123456789abcdef"

func signer(t *testing.T, maxAge time.Duration, now func() time.Time) *auth.ContextSigner {
	t.Helper()
	s, err := auth.NewContextSigner(testInternalSecret, maxAge, now)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// send performs a request carrying a signed context and no bearer token, as the
// gateway's admin proxy does.
func (f *fixture) send(t *testing.T, method, path, signedContext string, out any) (int, http.Header) {
	t.Helper()
	req, err := http.NewRequestWithContext(dbtest.Context(t), method, f.Server.URL+path, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	if signedContext != "" {
		req.Header.Set(auth.HeaderAuthContext, signedContext)
	}
	resp, err := f.Server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if out != nil && resp.StatusCode != http.StatusNoContent {
		_ = json.NewDecoder(resp.Body).Decode(out)
	}
	return resp.StatusCode, resp.Header
}

// identity is what the gateway would sign for this fixture's key.
func (f *fixture) identity() auth.Identity {
	return auth.Identity{
		OrgID: f.OrgID, ActorType: models.ActorAPIKey, ActorID: f.KeyID, UserID: &f.UserID,
		ActorLabel: f.Key[:auth.PrefixLen], Scopes: auth.AllScopes(), Priority: models.PriorityNormal,
	}
}

// The signed context authenticates in place of a key, and the identity inside it
// is the one the control plane acts on — including for the audit trail.
func TestSignedContextAuthenticates(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	h, err := signer(t, 10*time.Second, nil).Sign(f.identity(), "req-signed", auth.AudienceControlPlane, "nebula-gateway")
	if err != nil {
		t.Fatal(err)
	}
	var me struct {
		OrgID   uuid.UUID `json:"org_id"`
		ActorID uuid.UUID `json:"actor_id"`
	}
	if status, _ := f.send(t, "GET", "/v1/me", h, &me); status != 200 {
		t.Fatalf("status %d", status)
	}
	if me.OrgID != f.OrgID || me.ActorID != f.KeyID {
		t.Errorf("identity: %+v", me)
	}

	// A mutation through a signed context is audited under the signed actor.
	var created struct {
		ID uuid.UUID `json:"id"`
	}
	req, _ := http.NewRequestWithContext(dbtest.Context(t), "POST", f.Server.URL+"/v1/models",
		jsonBody(t, map[string]any{"name": "signed-model", "task": "chat"}))
	req.Header.Set(auth.HeaderAuthContext, h)
	req.Header.Set("Content-Type", "application/json")
	resp, err := f.Server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = json.NewDecoder(resp.Body).Decode(&created)
	resp.Body.Close()
	if resp.StatusCode != 201 {
		t.Fatalf("create through a signed context: %d", resp.StatusCode)
	}
	var actorID uuid.UUID
	if err := f.Pool.QueryRow(dbtest.Context(t),
		`SELECT actor_id FROM audit_logs WHERE resource_id = $1`, created.ID).Scan(&actorID); err != nil {
		t.Fatal(err)
	}
	if actorID != f.KeyID {
		t.Errorf("audit actor %s, want the signed key %s", actorID, f.KeyID)
	}
}

func TestSignedContextRefusals(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	old := signer(t, 10*time.Second, func() time.Time { return time.Now().Add(-time.Minute) })
	expired, _ := old.Sign(f.identity(), "", auth.AudienceControlPlane, "gw")

	wrongSecret, _ := auth.NewContextSigner(testInternalSecret+"-other", 10*time.Second, nil)
	forged, _ := wrongSecret.Sign(f.identity(), "", auth.AudienceControlPlane, "gw")

	service, _ := signer(t, 10*time.Second, nil).Sign(auth.ServiceIdentity("nebula-gateway"), "", auth.AudienceControlPlane, "gw")

	for name, tc := range map[string]struct {
		header string
		status int
		code   string
	}{
		"expired":           {expired, 401, "invalid_internal_context"},
		"wrong secret":      {forged, 401, "invalid_internal_context"},
		"garbage":           {"not.a-context", 401, "invalid_internal_context"},
		"service on public": {service, 403, "service_identity"},
	} {
		var env apiError
		status, _ := f.send(t, "GET", "/v1/me", tc.header, &env)
		if status != tc.status || env.Error.Code != tc.code {
			t.Errorf("%s: got %d %q, want %d %q", name, status, env.Error.Code, tc.status, tc.code)
		}
	}
}

func TestInternalCredentialLookup(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	s := signer(t, 10*time.Second, nil)
	service, _ := s.Sign(auth.ServiceIdentity("nebula-gateway"), "", auth.AudienceControlPlane, "nebula-gateway")
	tenant, _ := s.Sign(f.identity(), "", auth.AudienceControlPlane, "nebula-gateway")
	prefix := f.Key[:auth.PrefixLen]

	// A policy bound to the key travels with the lookup.
	policyID := db.MustNewID()
	rpm := int32(42)
	f.mustInTx(t, func(q store.Querier) error {
		if _, err := q.Exec(dbtest.Context(t), `INSERT INTO rate_limit_policies (id, org_id, name, requests_per_minute)
			VALUES ($1, $2, 'tight', $3)`, policyID, f.OrgID, rpm); err != nil {
			return err
		}
		_, err := q.Exec(dbtest.Context(t), `UPDATE api_keys SET rate_limit_policy_id = $1 WHERE id = $2`, policyID, f.KeyID)
		return err
	})

	var cred struct {
		KeyID           uuid.UUID `json:"key_id"`
		Prefix          string    `json:"prefix"`
		KeyHash         []byte    `json:"key_hash"`
		OrgID           uuid.UUID `json:"org_id"`
		OrgSlug         string    `json:"org_slug"`
		Scopes          []string  `json:"scopes"`
		RevokedAt       *string   `json:"revoked_at"`
		RateLimitPolicy *struct {
			Name              string `json:"name"`
			RequestsPerMinute *int32 `json:"requests_per_minute"`
			TokensPerMinute   *int32 `json:"tokens_per_minute"`
		} `json:"rate_limit_policy"`
	}
	status, _ := f.send(t, "GET", "/internal/v1/credentials/"+prefix, service, &cred)
	if status != 200 {
		t.Fatalf("lookup: %d", status)
	}
	if cred.KeyID != f.KeyID || cred.OrgID != f.OrgID || cred.Prefix != prefix || cred.OrgSlug == "" ||
		len(cred.Scopes) == 0 || cred.RevokedAt != nil {
		t.Errorf("credential: %+v", cred)
	}
	if !f.Hasher.Verify(f.Key, cred.KeyHash) {
		t.Error("the returned hash must verify the key under the shared pepper")
	}
	if cred.RateLimitPolicy == nil || cred.RateLimitPolicy.Name != "tight" || *cred.RateLimitPolicy.RequestsPerMinute != 42 ||
		cred.RateLimitPolicy.TokensPerMinute != nil {
		t.Errorf("policy: %+v", cred.RateLimitPolicy)
	}

	// Only a service identity may call the internal surface.
	var env apiError
	if status, _ := f.send(t, "GET", "/internal/v1/credentials/"+prefix, tenant, &env); status != 403 || env.Error.Code != "service_only" {
		t.Errorf("tenant identity on the internal surface: %d %q", status, env.Error.Code)
	}
	if status, _ := f.send(t, "GET", "/internal/v1/credentials/"+prefix, "", &env); status != 401 {
		t.Errorf("no identity: %d", status)
	}
	// A bearer key is not a service identity either.
	if status := f.doWithKey(t, f.Key, "GET", "/internal/v1/credentials/"+prefix, nil, nil); status != 401 {
		t.Errorf("a bearer key on the internal surface: %d", status)
	}

	if status, _ := f.send(t, "GET", "/internal/v1/credentials/nbk_Zzzzzzz", service, nil); status != 404 {
		t.Errorf("unknown prefix: %d", status)
	}
	if status, _ := f.send(t, "GET", "/internal/v1/credentials/bogus", service, nil); status != 400 {
		t.Errorf("malformed prefix: %d", status)
	}

	// A revoked key is returned WITH its revocation, not hidden: the gateway must
	// answer credential_revoked, not invalid_credential.
	var revokeHeaders http.Header
	req, _ := http.NewRequestWithContext(dbtest.Context(t), "DELETE", f.Server.URL+"/v1/api-keys/"+f.KeyID.String(), http.NoBody)
	req.Header.Set(auth.HeaderAuthContext, tenant)
	resp, err := f.Server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	revokeHeaders = resp.Header
	if resp.StatusCode != 204 || revokeHeaders.Get("X-Nebula-Revoked-Key-Prefix") != prefix {
		t.Fatalf("revoke: %d, prefix header %q", resp.StatusCode, revokeHeaders.Get("X-Nebula-Revoked-Key-Prefix"))
	}
	cred.RevokedAt = nil
	if status, _ := f.send(t, "GET", "/internal/v1/credentials/"+prefix, service, &cred); status != 200 || cred.RevokedAt == nil {
		t.Errorf("a revoked key must be returned with revoked_at: %d %+v", status, cred.RevokedAt)
	}
}

func jsonBody(t *testing.T, v any) *strings.Reader {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return strings.NewReader(string(b))
}
