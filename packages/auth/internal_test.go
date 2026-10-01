package auth_test

import (
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/adityasatwar321/nebula/packages/auth"
	"github.com/adityasatwar321/nebula/packages/db/models"
)

const testSecret = "0123456789abcdef0123456789abcdef-internal"

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func newSigner(t *testing.T, c *clock) *auth.ContextSigner {
	t.Helper()
	s, err := auth.NewContextSigner(testSecret, 10*time.Second, c.now)
	if err != nil {
		t.Fatalf("NewContextSigner: %v", err)
	}
	return s
}

func sampleIdentity() auth.Identity {
	user := uuid.New()
	policy := uuid.New()
	return auth.Identity{
		OrgID:             uuid.New(),
		OrgSlug:           "acme",
		ActorType:         models.ActorAPIKey,
		ActorID:           uuid.New(),
		UserID:            &user,
		ActorLabel:        "nbk_AbCdEfG",
		Scopes:            []auth.Scope{auth.ScopeInferenceInvoke, auth.ScopeModelsRead},
		Role:              models.RoleDeveloper,
		Priority:          models.PriorityHigh,
		RateLimitPolicyID: &policy,
	}
}

func TestContextRoundTrip(t *testing.T) {
	t.Parallel()
	c := &clock{t: time.Unix(1_800_000_000, 0)}
	s := newSigner(t, c)
	id := sampleIdentity()

	h, err := s.Sign(id, "req-1", auth.AudienceControlPlane, "nebula-gateway")
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.Verify(h, auth.AudienceControlPlane)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if got.RequestID != "req-1" || got.Issuer != "nebula-gateway" {
		t.Errorf("metadata: got %+v", got)
	}
	g := got.Identity
	if g.OrgID != id.OrgID || g.OrgSlug != id.OrgSlug || g.ActorID != id.ActorID ||
		g.ActorType != id.ActorType || g.ActorLabel != id.ActorLabel || g.Role != id.Role ||
		g.Priority != id.Priority || *g.UserID != *id.UserID || *g.RateLimitPolicyID != *id.RateLimitPolicyID {
		t.Errorf("identity did not round-trip:\n got %+v\nwant %+v", g, id)
	}
	if len(g.Scopes) != 2 || !g.Has(auth.ScopeInferenceInvoke) || !g.Has(auth.ScopeModelsRead) {
		t.Errorf("scopes: got %v", g.Scopes)
	}
}

// The replay window is the whole defence against a captured header, so both edges
// of it are pinned.
func TestContextAge(t *testing.T) {
	t.Parallel()
	c := &clock{t: time.Unix(1_800_000_000, 0)}
	s := newSigner(t, c)
	h, _ := s.Sign(sampleIdentity(), "", auth.AudienceControlPlane, "gw")

	c.t = c.t.Add(10 * time.Second)
	if _, err := s.Verify(h, auth.AudienceControlPlane); err != nil {
		t.Errorf("exactly max age must still verify, got %v", err)
	}
	c.t = c.t.Add(time.Millisecond)
	if _, err := s.Verify(h, auth.AudienceControlPlane); !errors.Is(err, auth.ErrContextExpired) {
		t.Errorf("past max age: got %v, want ErrContextExpired", err)
	}
}

func TestContextFromTheFutureIsRefused(t *testing.T) {
	t.Parallel()
	c := &clock{t: time.Unix(1_800_000_000, 0)}
	s := newSigner(t, c)
	h, _ := s.Sign(sampleIdentity(), "", auth.AudienceControlPlane, "gw")

	c.t = c.t.Add(-time.Second) // verifier clock one second behind: tolerated
	if _, err := s.Verify(h, auth.AudienceControlPlane); err != nil {
		t.Errorf("small skew must be tolerated, got %v", err)
	}
	c.t = c.t.Add(-5 * time.Second)
	if _, err := s.Verify(h, auth.AudienceControlPlane); !errors.Is(err, auth.ErrContextFuture) {
		t.Errorf("large skew: got %v, want ErrContextFuture", err)
	}
}

func TestContextTampering(t *testing.T) {
	t.Parallel()
	c := &clock{t: time.Unix(1_800_000_000, 0)}
	s := newSigner(t, c)
	id := sampleIdentity()
	h, _ := s.Sign(id, "", auth.AudienceControlPlane, "gw")
	payload, sig, _ := strings.Cut(h, ".")

	// Rewrite the org inside the payload and keep the old signature: the attack
	// the signature exists to stop.
	raw, _ := base64.RawURLEncoding.DecodeString(payload)
	forged := strings.Replace(string(raw), id.OrgID.String(), uuid.New().String(), 1)
	forgedHeader := base64.RawURLEncoding.EncodeToString([]byte(forged)) + "." + sig

	other, _ := auth.NewContextSigner(testSecret+"-different", 10*time.Second, c.now)
	otherHeader, _ := other.Sign(id, "", auth.AudienceControlPlane, "gw")

	cases := map[string]struct {
		header string
		want   error
	}{
		"forged org":        {forgedHeader, auth.ErrContextSignature},
		"other secret":      {otherHeader, auth.ErrContextSignature},
		"no signature":      {payload, auth.ErrContextMalformed},
		"empty":             {"", auth.ErrContextMalformed},
		"garbage signature": {payload + ".!!!", auth.ErrContextMalformed},
		"truncated sig":     {payload + "." + sig[:10], auth.ErrContextSignature},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := s.Verify(tc.header, auth.AudienceControlPlane); !errors.Is(err, tc.want) {
				t.Errorf("got %v, want %v", err, tc.want)
			}
		})
	}
}

func TestContextAudienceIsBound(t *testing.T) {
	t.Parallel()
	c := &clock{t: time.Unix(1_800_000_000, 0)}
	s := newSigner(t, c)
	h, _ := s.Sign(sampleIdentity(), "", "some-other-service", "gw")
	if _, err := s.Verify(h, auth.AudienceControlPlane); !errors.Is(err, auth.ErrContextSignature) {
		t.Errorf("a context minted for another audience must not verify, got %v", err)
	}
}

func TestServiceIdentityRoundTrips(t *testing.T) {
	t.Parallel()
	c := &clock{t: time.Unix(1_800_000_000, 0)}
	s := newSigner(t, c)
	h, err := s.Sign(auth.ServiceIdentity("nebula-gateway"), "", auth.AudienceControlPlane, "nebula-gateway")
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.Verify(h, auth.AudienceControlPlane)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if got.Identity.ActorType != auth.ActorService || len(got.Identity.Scopes) != 0 {
		t.Errorf("service identity: got %+v", got.Identity)
	}
}

func TestNewContextSignerRefusesWeakSecrets(t *testing.T) {
	t.Parallel()
	if _, err := auth.NewContextSigner("", time.Second, nil); !errors.Is(err, auth.ErrSecretMissing) {
		t.Errorf("empty secret: got %v", err)
	}
	if _, err := auth.NewContextSigner("short", time.Second, nil); err == nil {
		t.Error("a short secret must be refused")
	}
	if _, err := auth.NewContextSigner(testSecret, 0, nil); err == nil {
		t.Error("a zero max age must be refused")
	}
}
