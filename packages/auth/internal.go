package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/adityasatwar321/nebula/packages/db/models"
)

// HeaderAuthContext carries a signed identity from the gateway to the control
// plane (docs/security-boundaries.md §2, B2).
//
// The gateway authenticates the caller's API key, then attaches this header to the
// call it proxies. The control plane verifies the signature and uses the identity
// inside; it never re-derives identity from anything else the client sent. A
// client-supplied value of this header is stripped by the gateway, and a forged
// one fails verification without the shared secret.
const HeaderAuthContext = "X-Nebula-Auth-Context"

// ActorService identifies a NEBULA service acting on its own behalf rather than
// for a caller — the gateway resolving a credential, for example. It is never an
// identity a tenant request can carry: the control plane refuses it on every
// public route.
const ActorService models.ActorType = "service"

// contextVersion is bumped when the signed payload changes shape. A verifier
// refuses versions it does not know rather than guessing at fields.
const contextVersion = 1

// signingDomain separates these signatures from any other HMAC made with the same
// secret, so a signature minted for one purpose cannot be replayed as another.
const signingDomain = "nebula-auth-context/v1."

// maxFutureSkew tolerates clock disagreement between replicas. Beyond it a
// context "from the future" is refused, since accepting it would extend its
// replay window by the skew.
const maxFutureSkew = 2 * time.Second

// Errors returned by VerifyContext. All of them mean "not authenticated"; they are
// distinct so the refusal can be logged precisely.
var (
	ErrContextMalformed = errors.New("auth context is malformed")
	ErrContextSignature = errors.New("auth context signature does not verify")
	ErrContextExpired   = errors.New("auth context has expired")
	ErrContextFuture    = errors.New("auth context is issued in the future")
	ErrContextVersion   = errors.New("auth context version is not supported")
	ErrSecretMissing    = errors.New("internal auth secret is not configured")
)

// signedContext is the wire form of an identity. Field names are short but not
// cryptic: the header is logged by nothing, but it is read by humans debugging B2.
type signedContext struct {
	Version     int        `json:"v"`
	OrgID       uuid.UUID  `json:"org_id"`
	OrgSlug     string     `json:"org_slug,omitempty"`
	ActorType   string     `json:"actor_type"`
	ActorID     uuid.UUID  `json:"actor_id"`
	ActorLabel  string     `json:"actor_label,omitempty"`
	UserID      *uuid.UUID `json:"user_id,omitempty"`
	Scopes      []string   `json:"scopes"`
	Role        string     `json:"role,omitempty"`
	Priority    string     `json:"priority,omitempty"`
	RatePolicy  *uuid.UUID `json:"rate_limit_policy_id,omitempty"`
	RequestID   string     `json:"request_id,omitempty"`
	IssuedAtMS  int64      `json:"iat_ms"`
	Audience    string     `json:"aud"`
	IssuerLabel string     `json:"iss"`
}

// ContextSigner mints and verifies signed identities under one secret.
type ContextSigner struct {
	secret []byte
	maxAge time.Duration
	now    func() time.Time
}

// NewContextSigner returns a signer. The secret must be at least 32 bytes; a
// short secret is refused rather than weakened silently.
func NewContextSigner(secret string, maxAge time.Duration, now func() time.Time) (*ContextSigner, error) {
	if secret == "" {
		return nil, ErrSecretMissing
	}
	if len(secret) < 32 {
		return nil, fmt.Errorf("internal auth secret must be at least 32 bytes, got %d", len(secret))
	}
	if maxAge <= 0 {
		return nil, errors.New("internal auth max age must be positive")
	}
	if now == nil {
		now = time.Now
	}
	return &ContextSigner{secret: []byte(secret), maxAge: maxAge, now: now}, nil
}

// AudienceControlPlane is the audience of contexts minted for the control plane.
// A context minted for one audience does not verify at another, so a header lifted
// from a control-plane call cannot be presented elsewhere.
const AudienceControlPlane = "nebula-controlplane"

// Sign encodes an identity for one call. issuer names the signing service and is
// recorded for the audit trail of B2 itself.
func (s *ContextSigner) Sign(id Identity, requestID, audience, issuer string) (string, error) {
	payload := signedContext{
		Version:     contextVersion,
		OrgID:       id.OrgID,
		OrgSlug:     id.OrgSlug,
		ActorType:   string(id.ActorType),
		ActorID:     id.ActorID,
		ActorLabel:  id.ActorLabel,
		UserID:      id.UserID,
		Scopes:      Strings(id.Scopes),
		Role:        string(id.Role),
		Priority:    string(id.Priority),
		RatePolicy:  id.RateLimitPolicyID,
		RequestID:   requestID,
		IssuedAtMS:  s.now().UnixMilli(),
		Audience:    audience,
		IssuerLabel: issuer,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encoding auth context: %w", err)
	}
	encoded := base64.RawURLEncoding.EncodeToString(body)
	return encoded + "." + base64.RawURLEncoding.EncodeToString(s.mac(encoded)), nil
}

// Verified is a successfully verified context.
type Verified struct {
	Identity  Identity
	RequestID string
	Issuer    string
	IssuedAt  time.Time
}

// Verify checks a header value minted for audience and returns the identity in it.
//
// The order is signature first, then everything else: nothing in an unverified
// payload is trusted enough to be parsed into an identity, and an unverified
// timestamp must not even produce a distinguishable error.
func (s *ContextSigner) Verify(header, audience string) (Verified, error) {
	encoded, sig, ok := strings.Cut(header, ".")
	if !ok || encoded == "" || sig == "" {
		return Verified{}, ErrContextMalformed
	}
	presented, err := base64.RawURLEncoding.DecodeString(sig)
	if err != nil {
		return Verified{}, ErrContextMalformed
	}
	if !hmac.Equal(presented, s.mac(encoded)) {
		return Verified{}, ErrContextSignature
	}

	body, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return Verified{}, ErrContextMalformed
	}
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.DisallowUnknownFields()
	var p signedContext
	if err := dec.Decode(&p); err != nil {
		return Verified{}, ErrContextMalformed
	}
	if p.Version != contextVersion {
		return Verified{}, ErrContextVersion
	}
	if p.Audience != audience {
		// A valid signature for another audience is a replay across services.
		return Verified{}, ErrContextSignature
	}

	issued := time.UnixMilli(p.IssuedAtMS)
	now := s.now()
	if issued.After(now.Add(maxFutureSkew)) {
		return Verified{}, ErrContextFuture
	}
	if now.Sub(issued) > s.maxAge {
		return Verified{}, ErrContextExpired
	}

	scopes, err := ParseScopes(p.Scopes)
	if err != nil {
		return Verified{}, ErrContextMalformed
	}
	actorType := models.ActorType(p.ActorType)
	if !actorType.Valid() && actorType != ActorService {
		return Verified{}, ErrContextMalformed
	}
	if p.OrgID == uuid.Nil && actorType != ActorService {
		return Verified{}, ErrContextMalformed
	}

	return Verified{
		Identity: Identity{
			OrgID:             p.OrgID,
			OrgSlug:           p.OrgSlug,
			ActorType:         actorType,
			ActorID:           p.ActorID,
			UserID:            p.UserID,
			ActorLabel:        p.ActorLabel,
			Scopes:            scopes,
			Role:              models.UserRole(p.Role),
			Priority:          models.Priority(p.Priority),
			RateLimitPolicyID: p.RatePolicy,
		},
		RequestID: p.RequestID,
		Issuer:    p.IssuerLabel,
		IssuedAt:  issued,
	}, nil
}

func (s *ContextSigner) mac(encoded string) []byte {
	m := hmac.New(sha256.New, s.secret)
	m.Write([]byte(signingDomain))
	m.Write([]byte(encoded))
	return m.Sum(nil)
}

// ServiceIdentity is the identity a NEBULA service signs when it calls another on
// its own behalf. It carries no org and no scopes, so it can reach only endpoints
// that explicitly accept a service caller.
func ServiceIdentity(service string) Identity {
	return Identity{ActorType: ActorService, ActorLabel: service}
}
