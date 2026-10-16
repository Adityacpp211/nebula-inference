// Package adminproxy forwards the control API from the public edge to the control
// plane (docs/security-boundaries.md §2, B2).
//
// The control plane is never internet-reachable. The gateway authenticates the
// caller's API key, then forwards the request with the key REMOVED and a signed
// identity attached in its place, so the control plane acts on an identity it can
// verify came from the gateway rather than on anything the client sent. Three
// client-controlled inputs are dropped on the way through, each for a reason:
//
//   - Authorization: the key has done its job at the gateway; forwarding it would
//     put a live credential on one more network hop for no benefit.
//   - X-Nebula-Auth-Context: a client must never be able to supply the header that
//     carries identity. It is deleted before the signed one is set.
//   - X-Forwarded-*: replaced by the gateway's own view, so the audit trail records
//     the address the gateway saw rather than one a client wrote.
package adminproxy

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"github.com/adityasatwar321/nebula/packages/auth"
	"github.com/adityasatwar321/nebula/packages/httpx"
	"github.com/adityasatwar321/nebula/packages/telemetry"
)

// HeaderRevokedKeyPrefix mirrors the control plane's revocation signal. The
// gateway consumes it and never passes it to the client.
const HeaderRevokedKeyPrefix = "X-Nebula-Revoked-Key-Prefix"

// Options configures a Proxy.
type Options struct {
	Target *url.URL
	Signer *auth.ContextSigner
	Issuer string
	Logger *slog.Logger
	// Invalidate is called with the prefix of a key revoked through the proxy.
	Invalidate func(ctx context.Context, prefix string)
	// Transport overrides the upstream transport, for tests.
	Transport http.RoundTripper
}

// New builds the proxy handler. It must be mounted behind the gateway's
// authentication middleware: it signs whatever identity the context carries and
// refuses a request that carries none.
func New(o Options) http.Handler {
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	transport := o.Transport
	if transport == nil {
		t := http.DefaultTransport.(*http.Transport).Clone() //nolint:errcheck // the default transport is always *http.Transport
		t.Proxy = nil
		t.ResponseHeaderTimeout = 30 * time.Second
		transport = t
	}

	rp := &httputil.ReverseProxy{
		Transport: transport,
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(o.Target)
			pr.SetXForwarded()
			h := pr.Out.Header
			h.Del("Authorization")
			h.Del(auth.HeaderAuthContext)
			h.Del("Cookie")

			ctx := pr.In.Context()
			requestID := telemetry.RequestID(ctx)
			h.Set(httpx.HeaderRequestID, requestID)
			if tc, ok := telemetry.Trace(ctx); ok {
				if child, err := tc.Child(); err == nil {
					h.Set(telemetry.HeaderTraceparent, child.Header())
				}
			}
			// The identity is known to be present: ServeHTTP checked before handing
			// the request to the proxy.
			ident := auth.MustFromContext(ctx)
			signed, err := o.Signer.Sign(ident, requestID, auth.AudienceControlPlane, o.Issuer)
			if err == nil {
				h.Set(auth.HeaderAuthContext, signed)
			}
		},
		ModifyResponse: func(resp *http.Response) error {
			if prefix := resp.Header.Get(HeaderRevokedKeyPrefix); prefix != "" {
				resp.Header.Del(HeaderRevokedKeyPrefix)
				if o.Invalidate != nil {
					o.Invalidate(resp.Request.Context(), prefix)
				}
			}
			// The gateway's own middleware already set these on the response; the
			// upstream copies would appear twice.
			resp.Header.Del(httpx.HeaderRequestID)
			resp.Header.Del(httpx.HeaderAPIVersion)
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			if r.Context().Err() != nil {
				// The client went away; there is nobody to answer.
				return
			}
			w.Header().Set("Retry-After", "5")
			httpx.WriteError(w, r, (&httpx.APIError{
				Status:  http.StatusServiceUnavailable,
				Message: "the control API is temporarily unavailable; inference is unaffected",
				Type:    httpx.TypeServiceUnavailable,
				Code:    "control_plane_unavailable",
				Reason:  "control_plane_degraded",
			}).WithInternal(err), o.Logger)
		},
		// Control-plane responses are small JSON documents; the error log is routed
		// through slog so a proxy failure is a structured line like everything else.
		ErrorLog: slog.NewLogLogger(o.Logger.Handler(), slog.LevelWarn),
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := auth.FromContext(r.Context()); err != nil {
			httpx.WriteError(w, r, httpx.ErrInternal(err), o.Logger)
			return
		}
		rp.ServeHTTP(w, r)
	})
}
