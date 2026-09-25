// Package controlplane is the gateway's client for the control plane's internal
// surface. Every call carries a signed service identity
// (docs/security-boundaries.md §2, B2); the gateway holds no database credential and
// asks the control plane for everything that lives in PostgreSQL.
package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/google/uuid"

	"github.com/adityasatwar321/nebula/packages/auth"
	"github.com/adityasatwar321/nebula/packages/telemetry"
)

// ServiceName is how the gateway identifies itself in signed contexts.
const ServiceName = "nebula-gateway"

// ErrNotFound means the control plane answered authoritatively that the thing does
// not exist. Distinct from an outage, which is ErrUnavailable: the first may be
// cached as a negative, the second must never be.
var (
	ErrNotFound    = errors.New("not found at the control plane")
	ErrUnavailable = errors.New("control plane unavailable")
)

// RatePolicy is a key's rate-limit policy. A nil field means the policy does not
// limit that dimension and the gateway's default applies.
type RatePolicy struct {
	Name              string `json:"name"`
	RequestsPerMinute *int32 `json:"requests_per_minute,omitempty"`
	TokensPerMinute   *int32 `json:"tokens_per_minute,omitempty"`
	MaxConcurrency    *int32 `json:"max_concurrency,omitempty"`
	MaxQueueDepth     *int32 `json:"max_queue_depth,omitempty"`
}

// Credential is the control plane's record of one API key.
type Credential struct {
	KeyID             uuid.UUID   `json:"key_id"`
	Prefix            string      `json:"prefix"`
	KeyHash           []byte      `json:"key_hash"`
	OrgID             uuid.UUID   `json:"org_id"`
	OrgSlug           string      `json:"org_slug"`
	UserID            *uuid.UUID  `json:"user_id,omitempty"`
	Role              string      `json:"role,omitempty"`
	Scopes            []string    `json:"scopes"`
	Priority          string      `json:"priority"`
	ExpiresAt         *time.Time  `json:"expires_at,omitempty"`
	RevokedAt         *time.Time  `json:"revoked_at,omitempty"`
	RateLimitPolicyID *uuid.UUID  `json:"rate_limit_policy_id,omitempty"`
	RateLimitPolicy   *RatePolicy `json:"rate_limit_policy,omitempty"`
}

// Client calls the control plane.
type Client struct {
	base    *url.URL
	http    *http.Client
	signer  *auth.ContextSigner
	timeout time.Duration
}

// New builds a client.
func New(baseURL string, signer *auth.ContextSigner, timeout time.Duration, hc *http.Client) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("control plane URL: %w", err)
	}
	if hc == nil {
		hc = &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()}
	}
	return &Client{base: u, http: hc, signer: signer, timeout: timeout}, nil
}

// BaseURL is the control plane's base URL, for the admin proxy.
func (c *Client) BaseURL() *url.URL { return c.base }

// LookupCredential fetches a key's record by prefix.
func (c *Client) LookupCredential(ctx context.Context, prefix string) (*Credential, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	u := c.base.JoinPath("/internal/v1/credentials", prefix)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	requestID := telemetry.RequestID(ctx)
	signed, err := c.signer.Sign(auth.ServiceIdentity(ServiceName), requestID, auth.AudienceControlPlane, ServiceName)
	if err != nil {
		return nil, err
	}
	req.Header.Set(auth.HeaderAuthContext, signed)
	if requestID != "" {
		req.Header.Set("X-Request-Id", requestID)
	}
	if tc, ok := telemetry.Trace(ctx); ok {
		req.Header.Set(telemetry.HeaderTraceparent, tc.Header())
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		_ = resp.Body.Close()
	}()

	switch {
	case resp.StatusCode == http.StatusOK:
		var cred Credential
		if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&cred); err != nil {
			return nil, fmt.Errorf("%w: undecodable credential: %v", ErrUnavailable, err)
		}
		return &cred, nil
	case resp.StatusCode == http.StatusNotFound:
		return nil, ErrNotFound
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		// The control plane refused the gateway itself: a secret mismatch between
		// the two services. Reported as unavailability, not as the caller's fault,
		// and loudly, because every request will fail the same way.
		return nil, fmt.Errorf("%w: the control plane refused the gateway's service identity (status %d); "+
			"check NEBULA_INTERNAL_AUTH_SECRET matches on both services", ErrUnavailable, resp.StatusCode)
	default:
		return nil, fmt.Errorf("%w: status %d", ErrUnavailable, resp.StatusCode)
	}
}
