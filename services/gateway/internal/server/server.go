// Package server wires nebula-gateway's HTTP surface: the OpenAI-compatible
// inference API, the admin proxy, probes and the specification.
//
// The request path follows the stages of docs/architecture.md §6, and the order is
// part of the design (docs/security-boundaries.md §2, B1): cheap refusals happen
// before expensive work, so an unauthenticated flood never reaches a worker and a
// malformed body never costs a rate-limit round trip.
//
//	ACCEPT → IDENTIFY → AUTHENTICATE → AUTHORIZE → VALIDATE → ADMIT → RESOLVE →
//	SELECT → DISPATCH → STREAM → FINALIZE
//
// Idempotency-Key (docs/api.md §1) is not honoured yet: replay storage needs the
// same Redis the limiter uses and lands with Phase 10's retry work. TODO(NEB-142).
// Prometheus metrics for the gateway arrive with the metric catalogue in Phase 8.
// TODO(NEB-141).
//
// QUEUE (stage 9) is absent in Phase 4: the gateway admission queue is Phase 7.
// A saturated worker answers 429 and the gateway passes that on with the worker's
// Retry-After, which is the correct behaviour without a queue rather than a stub
// of one.
package server

import (
	"context"
	"log/slog"
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/adityasatwar321/nebula/packages/api"
	"github.com/adityasatwar321/nebula/packages/auth"
	"github.com/adityasatwar321/nebula/packages/config"
	"github.com/adityasatwar321/nebula/packages/httpx"
	"github.com/adityasatwar321/nebula/packages/telemetry"
	"github.com/adityasatwar321/nebula/services/gateway/internal/admission"
	"github.com/adityasatwar321/nebula/services/gateway/internal/credentials"
	"github.com/adityasatwar321/nebula/services/gateway/internal/dispatch"
	"github.com/adityasatwar321/nebula/services/gateway/internal/ratelimit"
	"github.com/adityasatwar321/nebula/services/gateway/internal/router"
	"github.com/adityasatwar321/nebula/services/gateway/internal/usage"
)

// Authenticator resolves a presented API key. Satisfied by *credentials.Resolver.
type Authenticator interface {
	Authenticate(ctx context.Context, presented string) (credentials.Principal, error)
}

// Deps is everything the gateway's handlers need.
type Deps struct {
	Config  *config.Config
	Logger  *slog.Logger
	Probes  *telemetry.Probes
	Auth    Authenticator
	Limiter *ratelimit.Limiter
	// Router holds the route table and endpoint state and places every request.
	Router *router.Router
	// RouteSource is "controlplane" or "static", reported in /v1/models.
	RouteSource string
	// Queues are the per-deployment admission queues.
	Queues  *admission.Queues
	Workers *dispatch.Client
	// Proxy forwards the control API. Nil answers 503 for every admin path, which
	// is the shape of a gateway configured without a control plane.
	Proxy http.Handler
	Usage usage.Sink
	Now   func() time.Time
}

// Gateway holds the handlers.
type Gateway struct {
	d Deps
}

// Route is one mounted endpoint, for the specification drift test.
type Route struct {
	Method, Path string
	// Scope is the scope the caller needs, "" for none.
	Scope auth.Scope
}

// Routes lists the endpoints the gateway serves itself. Everything else under /v1
// is proxied to the control plane and documented there.
func Routes() []Route {
	return []Route{
		{Method: http.MethodPost, Path: "/v1/chat/completions", Scope: auth.ScopeInferenceInvoke},
		{Method: http.MethodPost, Path: "/v1/completions", Scope: auth.ScopeInferenceInvoke},
		{Method: http.MethodGet, Path: "/v1/models", Scope: auth.ScopeInferenceInvoke},
	}
}

// New builds the gateway's root handler.
//
// The middleware chain is the same as the control plane's, in the same order and
// for the same reasons (services/controlplane/internal/server). Authentication is
// per route, because probes and the specification must be reachable without a
// credential.
func New(d Deps) http.Handler {
	if d.Now == nil {
		d.Now = time.Now
	}
	if d.Usage == nil {
		d.Usage = usage.LogSink{Logger: d.Logger}
	}
	if d.Router == nil {
		d.Router = router.New(router.Options{Now: d.Now})
	}
	if d.RouteSource == "" {
		d.RouteSource = "static"
	}
	if d.Queues == nil {
		// Real time, so every waiter also keeps its own deadline timer; the sweeper
		// grants waiters when capacity returns. The process owns its lifetime.
		d.Queues = admission.New(admission.Options{MaxDepth: 128, AgingStep: 5 * time.Second,
			Capacity: d.Router.Capacity})
		go d.Queues.Run(context.Background())
	}
	g := &Gateway{d: d}

	mux := http.NewServeMux()
	d.Probes.Mount(mux)
	mux.Handle(api.SpecPath, api.Handler())

	invoke := func(h http.HandlerFunc) http.Handler {
		return g.authenticate(g.require(auth.ScopeInferenceInvoke, h))
	}
	mux.Handle("POST /v1/chat/completions", invoke(g.chatCompletions))
	mux.Handle("POST /v1/completions", invoke(g.completions))
	mux.Handle("GET /v1/models", invoke(g.listModels))
	// GET /v1/models/{x} is both OpenAI's "retrieve model" (x is a route name) and
	// the control plane's registry read (x is a model uuid). The segment decides:
	// a uuid can never be a route name that a person chose, so the split is exact.
	mux.Handle("GET /v1/models/{model}", g.authenticate(http.HandlerFunc(g.modelOrRegistry)))
	// The control plane's registry list shares the prefix. A literal segment is more
	// specific than a wildcard, so this pattern wins; without it "registry" would be
	// looked up as a route name and answered 404.
	mux.Handle("GET /v1/models/registry", g.authenticate(g.admitAdmin(http.HandlerFunc(g.proxy))))

	for _, p := range []string{"/v1/chat/completions", "/v1/completions"} {
		mux.HandleFunc(p, g.methodNotAllowed("POST"))
	}

	// Admission queue state, for load tests and development. Never in production:
	// Dev.DebugEndpoints is refused there (packages/config).
	if d.Config != nil && d.Config.Dev.DebugEndpoints {
		mux.HandleFunc("GET /debug/queues", func(w http.ResponseWriter, _ *http.Request) {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			_ = httpx.WriteJSON(w, http.StatusOK, map[string]any{
				"queues":           d.Queues.Stats(),
				"heap_alloc_bytes": m.HeapAlloc,
				"goroutines":       runtime.NumGoroutine(),
			})
		})
	}

	// The internal surface of the control plane is never reachable through the
	// public edge, whatever the path looks like.
	mux.HandleFunc("/internal/", g.notFound)

	// Everything else under /v1 is the control API.
	mux.Handle("/v1/", g.authenticate(g.admitAdmin(http.HandlerFunc(g.proxy))))
	mux.HandleFunc("/", g.notFound)

	chain := httpx.Chain(
		httpx.RequestID(),
		httpx.Trace(),
		httpx.WithLogger(d.Logger),
		httpx.APIVersion(),
		httpx.AccessLog(d.Logger, telemetry.PathLivez, telemetry.PathReadyz, telemetry.PathHealthz),
		httpx.Recover(d.Logger),
		httpx.MaxBody(d.Config.HTTP.MaxBodyBytes),
	)
	return chain(mux)
}

func (g *Gateway) notFound(w http.ResponseWriter, r *http.Request) {
	g.fail(w, r, httpx.ErrNotFound("no such endpoint"))
}

func (g *Gateway) methodNotAllowed(allow string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Allow", allow)
		g.fail(w, r, httpx.ErrMethodNotAllowed(r.Method+" is not allowed here; use "+allow))
	}
}

func (g *Gateway) proxy(w http.ResponseWriter, r *http.Request) {
	if g.d.Proxy == nil {
		g.fail(w, r, httpx.ErrServiceUnavailable("the control API is not configured on this gateway", "control_plane_degraded"))
		return
	}
	g.d.Proxy.ServeHTTP(w, r)
}

func (g *Gateway) modelOrRegistry(w http.ResponseWriter, r *http.Request) {
	if _, err := uuid.Parse(r.PathValue("model")); err == nil {
		g.admitAdmin(http.HandlerFunc(g.proxy)).ServeHTTP(w, r)
		return
	}
	g.require(auth.ScopeInferenceInvoke, g.retrieveModel).ServeHTTP(w, r)
}

func (g *Gateway) fail(w http.ResponseWriter, r *http.Request, err error) {
	httpx.WriteError(w, r, err, g.d.Logger)
}

func (g *Gateway) logger(ctx context.Context) *slog.Logger { return telemetry.Logger(ctx, g.d.Logger) }

// ---------------------------------------------------------------------------
// authentication and authorization
// ---------------------------------------------------------------------------

type principalKey struct{}

func principalFrom(ctx context.Context) credentials.Principal {
	p, _ := ctx.Value(principalKey{}).(credentials.Principal)
	return p
}

// authenticate resolves the bearer key and puts the identity on the context.
func (g *Gateway) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		presented, err := bearer(r)
		if err != nil {
			g.fail(w, r, err)
			return
		}
		p, err := g.d.Auth.Authenticate(r.Context(), presented)
		if err != nil {
			g.fail(w, r, err)
			return
		}
		ctx := auth.WithIdentity(r.Context(), p.Identity)
		ctx = context.WithValue(ctx, principalKey{}, p)
		ctx = telemetry.WithOrgID(ctx, p.Identity.OrgID.String())
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// bearer extracts the API key. Session tokens (JWT) arrive with the dashboard in
// Phase 15 and are refused with a specific code until then.
func bearer(r *http.Request) (string, error) {
	h := r.Header.Get("Authorization")
	if h == "" {
		return "", unauthenticated("an API key is required: send Authorization: Bearer nbk_...", "missing_credential")
	}
	scheme, value, ok := strings.Cut(h, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return "", unauthenticated("Authorization must use the Bearer scheme", "malformed_credential")
	}
	value = strings.TrimSpace(value)
	if !strings.HasPrefix(value, auth.KeyPrefix) {
		return "", unauthenticated("only API keys are accepted; session tokens arrive with the dashboard",
			"unsupported_credential")
	}
	return value, nil
}

// require refuses a caller lacking a scope. As in the control plane, admin does
// not imply the others: an admin key that cannot invoke inference is useful.
func (g *Gateway) require(scope auth.Scope, next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ident, err := auth.FromContext(r.Context())
		if err != nil {
			g.fail(w, r, httpx.ErrInternal(err))
			return
		}
		if !ident.Has(scope) {
			g.fail(w, r, forbidden("this API key does not carry the "+string(scope)+" scope", "insufficient_scope"))
			return
		}
		next(w, r)
	})
}

func unauthenticated(message, code string) *httpx.APIError {
	return &httpx.APIError{Status: http.StatusUnauthorized, Message: message, Type: httpx.TypeAuthentication, Code: code}
}

func forbidden(message, code string) *httpx.APIError {
	return &httpx.APIError{Status: http.StatusForbidden, Message: message, Type: httpx.TypePermission, Code: code}
}

// ---------------------------------------------------------------------------
// admission
// ---------------------------------------------------------------------------

// limitsFor resolves a principal's key and org limits. A key with a policy gets
// the policy's numbers, where a nil dimension means unlimited (models.RateLimitPolicy);
// a key without one gets the configured defaults.
func (g *Gateway) limitsFor(p credentials.Principal) (key, org ratelimit.Limits) {
	c := g.d.Config.Limits
	org = ratelimit.Limits{RPM: c.OrgRPM, TPM: c.OrgTPM, Concurrency: c.OrgConcurrency}
	if p.Policy == nil {
		return ratelimit.Limits{RPM: c.KeyRPM, TPM: c.KeyTPM, Concurrency: c.KeyConcurrency}, org
	}
	deref := func(v *int32) int {
		if v == nil {
			return 0
		}
		return int(*v)
	}
	return ratelimit.Limits{
		RPM:         deref(p.Policy.RequestsPerMinute),
		TPM:         deref(p.Policy.TokensPerMinute),
		Concurrency: deref(p.Policy.MaxConcurrency),
	}, org
}

// admit runs the limiter, writes the x-ratelimit-* headers, and returns the lease
// or a 429.
func (g *Gateway) admit(w http.ResponseWriter, r *http.Request, tokens int, leaseTTL time.Duration, admin bool) (*ratelimit.Lease, error) {
	if !g.d.Config.Limits.Enabled || g.d.Limiter == nil {
		return nil, nil
	}
	p := principalFrom(r.Context())
	key, org := g.limitsFor(p)
	if admin {
		// The control API is limited by request rate only. It does not generate
		// tokens, and an operator's admin call must not queue behind inference
		// concurrency.
		key = ratelimit.Limits{RPM: key.RPM}
		org = ratelimit.Limits{RPM: org.RPM}
	}
	d, lease := g.d.Limiter.Admit(r.Context(), ratelimit.Request{
		OrgID:    p.Identity.OrgID.String(),
		KeyID:    p.Identity.ActorID.String(),
		Key:      key,
		Org:      org,
		Tokens:   tokens,
		LeaseTTL: leaseTTL,
		LeaseID:  telemetry.RequestID(r.Context()),
	})
	setRateHeaders(w.Header(), d)
	if d.Allowed {
		return lease, nil
	}
	w.Header().Set("Retry-After", seconds(d.RetryAfter))
	who := "this API key"
	if d.Scope == ratelimit.ScopeOrg {
		who = "this organization"
	}
	var msg string
	switch d.Reason {
	case ratelimit.ReasonRPM:
		msg = who + " has exceeded its requests-per-minute limit"
	case ratelimit.ReasonTPM:
		msg = who + " has exceeded its tokens-per-minute limit"
	default:
		msg = who + " has too many requests in flight"
	}
	return nil, &httpx.APIError{
		Status:  http.StatusTooManyRequests,
		Message: msg + "; retry after the interval in Retry-After",
		Type:    httpx.TypeRateLimit,
		Code:    string(d.Reason),
		Reason:  string(d.Reason),
	}
}

// admitAdmin applies the request-rate limit to proxied control-API calls.
func (g *Gateway) admitAdmin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lease, err := g.admit(w, r, 0, time.Minute, true)
		if err != nil {
			g.fail(w, r, err)
			return
		}
		defer lease.Release(r.Context(), 0)
		next.ServeHTTP(w, r)
	})
}

// setRateHeaders writes OpenAI's rate-limit headers for the key's own buckets.
func setRateHeaders(h http.Header, d ratelimit.Decision) {
	if d.Requests.Limit > 0 {
		h.Set("x-ratelimit-limit-requests", itoa(d.Requests.Limit))
		h.Set("x-ratelimit-remaining-requests", itoa(max(0, d.Requests.Remaining)))
		h.Set("x-ratelimit-reset-requests", ratelimit.FormatReset(d.Requests.Reset))
	}
	if d.Tokens.Limit > 0 {
		h.Set("x-ratelimit-limit-tokens", itoa(d.Tokens.Limit))
		h.Set("x-ratelimit-remaining-tokens", itoa(max(0, d.Tokens.Remaining)))
		h.Set("x-ratelimit-reset-tokens", ratelimit.FormatReset(d.Tokens.Reset))
	}
}
