package controlplane

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/adityasatwar321/nebula/packages/auth"
)

// ErrNotModified means the routing table has not changed since the version the
// caller already holds.
var ErrNotModified = errors.New("routing table not modified")

// RoutingTable fetches the routing table. With etag set, an unchanged table is
// ErrNotModified and costs no decoding. The body is returned raw: the caller both
// decodes it and snapshots it, and re-encoding would be a second representation.
func (c *Client) RoutingTable(ctx context.Context, etag string) (body []byte, newETag string, err error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	u := c.base.JoinPath("/internal/v1/routing-table")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, "", err
	}
	signed, err := c.signer.Sign(auth.ServiceIdentity(ServiceName), "", auth.AudienceControlPlane, ServiceName)
	if err != nil {
		return nil, "", err
	}
	req.Header.Set(auth.HeaderAuthContext, signed)
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		_ = resp.Body.Close()
	}()
	switch resp.StatusCode {
	case http.StatusOK:
		b, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
		if err != nil {
			return nil, "", fmt.Errorf("%w: reading the routing table: %v", ErrUnavailable, err)
		}
		return b, resp.Header.Get("ETag"), nil
	case http.StatusNotModified:
		return nil, etag, ErrNotModified
	case http.StatusUnauthorized, http.StatusForbidden:
		return nil, "", fmt.Errorf("%w: the control plane refused the gateway's service identity (status %d); "+
			"check NEBULA_INTERNAL_AUTH_SECRET matches on both services", ErrUnavailable, resp.StatusCode)
	default:
		return nil, "", fmt.Errorf("%w: routing table: status %d", ErrUnavailable, resp.StatusCode)
	}
}
