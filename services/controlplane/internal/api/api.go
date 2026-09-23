package api

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/adityasatwar321/nebula/packages/auth"
	"github.com/adityasatwar321/nebula/packages/config"
	"github.com/adityasatwar321/nebula/packages/telemetry"
	"github.com/adityasatwar321/nebula/services/controlplane/internal/store"
)

// API is the control plane's handler set.
type API struct {
	Config *config.Config
	Logger *slog.Logger
	Store  *store.Store
	Hasher *auth.Hasher

	keys *keyCache
}

// New builds the API.
//
// The hasher is required: a control plane that cannot verify a key cannot serve a
// single authenticated request, so failing here is better than failing on the
// first request with a confusing error.
func New(cfg *config.Config, logger *slog.Logger, st *store.Store) (*API, error) {
	if err := cfg.RequireAuth(); err != nil {
		return nil, err
	}
	hasher, err := auth.NewHasher(cfg.Auth.KeyPepper.Reveal())
	if err != nil {
		return nil, err
	}
	return &API{
		Config: cfg,
		Logger: logger,
		Store:  st,
		Hasher: hasher,
		keys:   newKeyCache(cfg.Auth.KeyCacheSize, nil),
	}, nil
}

// logger returns the request-correlated logger.
func (a *API) logger(r *http.Request) *slog.Logger {
	return telemetry.Logger(r.Context(), a.Logger)
}

// actor converts an identity into the store actor whose id the database triggers
// stamp onto state-transition history.
func actor(ident auth.Identity) store.Actor {
	return store.Actor{Type: string(ident.ActorType), ID: ident.ActorID}
}

// auditFor pre-fills the fields of an audit entry that come from the request
// rather than from the change: who, from where, and with which request id.
func auditFor(r *http.Request, action, resourceType string) store.Entry {
	ident := auth.MustFromContext(r.Context())
	label := ident.ActorLabel
	e := store.Entry{
		OrgID:        ident.OrgID,
		ActorType:    ident.ActorType,
		Action:       action,
		ResourceType: resourceType,
		IP:           clientIP(r),
	}
	if ident.ActorID != uuid.Nil {
		id := ident.ActorID
		e.ActorID = &id
	}
	if label != "" {
		e.ActorLabel = &label
	}
	if ua := r.UserAgent(); ua != "" {
		if len(ua) > 512 {
			ua = ua[:512]
		}
		e.UserAgent = &ua
	}
	return e
}

// Route is one mounted endpoint and the authority it requires.
type Route struct {
	// Pattern is a Go 1.22 ServeMux pattern: "METHOD /path/{param}".
	Pattern string
	// Method and Path are the pattern split, so a test or a generator does not have
	// to re-parse it.
	Method string
	Path   string
	// Scope is the scope the caller must carry, or "" for an authenticated endpoint
	// with no scope requirement.
	Scope auth.Scope

	handler func(http.ResponseWriter, *http.Request) error
}

// routes is the complete route table.
//
// A table rather than a sequence of mux.Handle calls, for two reasons: every
// endpoint's required authority is reviewable by reading one function instead of
// opening twelve handlers, and the table can be compared against
// packages/api/openapi.yaml by a test, so the specification and the service cannot
// drift (openapi_test.go asserts both directions).
//
// Routes are declared with Go's own method-and-wildcard patterns rather than a
// third-party router. The standard library covers what this API needs, and a router
// is a dependency whose failure modes would have to be understood at 3 a.m.
// (docs/components.md §1.1).
//
// The three probe endpoints and the specification are deliberately absent: they are
// mounted unauthenticated by the server, because a readiness probe cannot carry a
// credential and a client cannot construct a valid request without the contract.
func (a *API) routes() []Route {
	const (
		modelsRead  = auth.ScopeModelsRead
		modelsWrite = auth.ScopeModelsWrite
		depsRead    = auth.ScopeDeploymentsRead
		depsWrite   = auth.ScopeDeploymentsWrite
		adminScope  = auth.ScopeAdmin
		auditRead   = auth.ScopeAuditRead
		noScope     = auth.Scope("")
	)

	return build([]Route{
		// Who am I requires authentication but no scope: a caller must always be
		// able to discover what their own credential can do, or a 403 is
		// unexplainable.
		{Pattern: "GET /v1/me", Scope: noScope, handler: a.whoAmI},

		// The state machine as data, so the CLI and dashboard render the same graph
		// the database enforces instead of hard-coding their own copy.
		{Pattern: "GET /v1/lifecycle/deployment-states", Scope: noScope, handler: a.deploymentLifecycle},

		// Users and credentials.
		{Pattern: "GET /v1/users", Scope: adminScope, handler: a.listUsers},
		{Pattern: "POST /v1/users", Scope: adminScope, handler: a.createUser},
		{Pattern: "GET /v1/users/{user_id}", Scope: adminScope, handler: a.getUser},
		{Pattern: "PATCH /v1/users/{user_id}", Scope: adminScope, handler: a.updateUser},
		{Pattern: "DELETE /v1/users/{user_id}", Scope: adminScope, handler: a.deleteUser},

		{Pattern: "GET /v1/api-keys", Scope: adminScope, handler: a.listAPIKeys},
		{Pattern: "POST /v1/api-keys", Scope: adminScope, handler: a.createAPIKey},
		{Pattern: "DELETE /v1/api-keys/{key_id}", Scope: adminScope, handler: a.revokeAPIKey},

		// Registry. /v1/models/registry is matched before /v1/models/{model_id}
		// because a literal segment is more specific than a wildcard.
		{Pattern: "GET /v1/models/registry", Scope: modelsRead, handler: a.listModels},
		{Pattern: "POST /v1/models", Scope: modelsWrite, handler: a.createModel},
		{Pattern: "GET /v1/models/{model_id}", Scope: modelsRead, handler: a.getModel},
		{Pattern: "PATCH /v1/models/{model_id}", Scope: modelsWrite, handler: a.updateModel},
		{Pattern: "DELETE /v1/models/{model_id}", Scope: modelsWrite, handler: a.deleteModel},

		{Pattern: "GET /v1/models/{model_id}/versions", Scope: modelsRead, handler: a.listVersions},
		{Pattern: "POST /v1/models/{model_id}/versions", Scope: modelsWrite, handler: a.createVersion},
		{Pattern: "GET /v1/model-versions/{version_id}", Scope: modelsRead, handler: a.getVersion},
		{Pattern: "POST /v1/model-versions/{version_id}/finalize", Scope: modelsWrite, handler: a.finalizeVersion},
		{Pattern: "POST /v1/model-versions/{version_id}/fail", Scope: modelsWrite, handler: a.failVersion},
		{Pattern: "DELETE /v1/model-versions/{version_id}", Scope: modelsWrite, handler: a.archiveVersion},

		// Deployments.
		{Pattern: "GET /v1/deployments", Scope: depsRead, handler: a.listDeployments},
		{Pattern: "POST /v1/deployments", Scope: depsWrite, handler: a.createDeployment},
		{Pattern: "GET /v1/deployments/{deployment_id}", Scope: depsRead, handler: a.getDeployment},
		{Pattern: "PATCH /v1/deployments/{deployment_id}", Scope: depsWrite, handler: a.updateDeployment},
		{Pattern: "DELETE /v1/deployments/{deployment_id}", Scope: depsWrite, handler: a.deleteDeployment},
		{Pattern: "GET /v1/deployments/{deployment_id}/status", Scope: depsRead, handler: a.deploymentStatus},
		{Pattern: "GET /v1/deployments/{deployment_id}/revisions", Scope: depsRead, handler: a.listRevisions},
		{Pattern: "GET /v1/deployments/{deployment_id}/transitions", Scope: depsRead, handler: a.listTransitions},
		{Pattern: "POST /v1/deployments/{deployment_id}/scale", Scope: depsWrite, handler: a.scaleDeployment},
		{Pattern: "POST /v1/deployments/{deployment_id}/rollback", Scope: depsWrite, handler: a.rollbackDeployment},
		{Pattern: "POST /v1/deployments/{deployment_id}/stop", Scope: depsWrite, handler: a.stopDeployment},
		{Pattern: "POST /v1/deployments/{deployment_id}/start", Scope: depsWrite, handler: a.startDeployment},

		// A manual transition is an operator tool, not part of the normal lifecycle:
		// it requires admin and is audited like any other change. It exists because
		// desired state must be correctable during an incident, and because until the
		// Phase 5 controller runs, nothing else advances an in-flight state.
		{Pattern: "POST /v1/deployments/{deployment_id}/transition", Scope: adminScope, handler: a.transitionDeployment},

		{Pattern: "GET /v1/audit-logs", Scope: auditRead, handler: a.listAuditLogs},
	})
}

// build fills in Method and Path from each Pattern, so the two cannot disagree.
func build(in []Route) []Route {
	out := make([]Route, 0, len(in))
	for _, r := range in {
		method, path, ok := strings.Cut(r.Pattern, " ")
		if !ok {
			panic("api: route pattern must be \"METHOD /path\": " + r.Pattern)
		}
		r.Method, r.Path = method, path
		out = append(out, r)
	}
	return out
}

// Routes returns the route table, for the specification drift test and for anything
// that needs to reason about the API surface without serving it.
func (a *API) Routes() []Route { return a.routes() }

// Mount registers every control-plane route on a mux.
func (a *API) Mount(mux *http.ServeMux) {
	for _, route := range a.routes() {
		h := a.handler(route.handler)
		if route.Scope != "" {
			h = a.require(route.Scope)(h).ServeHTTP
		}
		mux.Handle(route.Pattern, a.authenticate(h))
	}
}

// whoAmI describes the calling credential. It is the endpoint a caller hits when
// a 403 surprises them.
func (a *API) whoAmI(w http.ResponseWriter, r *http.Request) error {
	ident := auth.MustFromContext(r.Context())

	org, err := a.Store.Organizations.Get(r.Context(), a.Store.Pool(), ident.OrgID)
	if err != nil {
		return storeError(err, "organization")
	}

	a.write(w, r, http.StatusOK, meResponse{
		OrgID:            ident.OrgID,
		OrgSlug:          org.Slug,
		OrgName:          org.Name,
		DefaultNamespace: org.DefaultNamespace,
		ActorType:        string(ident.ActorType),
		ActorID:          ident.ActorID,
		ActorLabel:       ident.ActorLabel,
		Role:             string(ident.Role),
		Scopes:           auth.Strings(ident.Scopes),
		Priority:         string(ident.Priority),
	})
	return nil
}

// deploymentLifecycle returns the deployment state machine.
func (a *API) deploymentLifecycle(w http.ResponseWriter, r *http.Request) error {
	a.write(w, r, http.StatusOK, newLifecycleResponse())
	return nil
}

// clientIP extracts the caller's address for the audit trail.
//
// X-Forwarded-For is trusted only for its LAST entry, which is the one the
// immediately upstream proxy appended: earlier entries are client-supplied and
// forgeable, so recording the first would let a caller write any address they like
// into the audit trail.
func clientIP(r *http.Request) net.IP {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		if ip := net.ParseIP(strings.TrimSpace(parts[len(parts)-1])); ip != nil {
			return ip
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	return net.ParseIP(host)
}

// ---------------------------------------------------------------------------
// shared transaction helper
// ---------------------------------------------------------------------------

// inTx runs fn in one tenant-scoped transaction, with the caller's actor recorded
// for the database triggers.
//
// Every mutating handler goes through this, which is what makes "the change and
// its audit record commit together" structural rather than a habit.
//
// The context is passed INTO fn rather than left for the closure to fetch from the
// request again. That is not a style preference: a closure that reaches back for
// r.Context() can be given a different context by a future caller (a retry with a
// shorter deadline, say) and silently keep using the original, so the deadline the
// caller set would not apply to the queries.
func (a *API) inTx(r *http.Request, fn func(ctx context.Context, q store.Querier) error) error {
	ctx := r.Context()
	ident := auth.MustFromContext(ctx)
	return a.Store.InTxForOrg(ctx, ident.OrgID, actor(ident), func(tx pgx.Tx) error {
		return fn(ctx, tx)
	})
}
