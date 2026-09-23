package api_test

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"
	"gopkg.in/yaml.v3"

	specpkg "github.com/adityasatwar321/nebula/packages/api"
	"github.com/adityasatwar321/nebula/packages/auth"
	"github.com/adityasatwar321/nebula/packages/config"
	"github.com/adityasatwar321/nebula/packages/lifecycle"
	"github.com/adityasatwar321/nebula/services/controlplane/internal/api"
)

// spec is the parsed OpenAPI document. Only the parts this test checks are
// modelled: a full OpenAPI type would be a second implementation of the standard,
// and the point here is drift detection, not validation.
type spec struct {
	Paths map[string]map[string]yaml.Node `yaml:"paths"`
}

var httpMethods = map[string]bool{
	"get": true, "put": true, "post": true, "delete": true,
	"options": true, "head": true, "patch": true, "trace": true,
}

func loadSpec(t *testing.T) spec {
	t.Helper()
	var s spec
	if err := yaml.Unmarshal(specpkg.Spec, &s); err != nil {
		t.Fatalf("the embedded openapi.yaml is not valid YAML: %v", err)
	}
	if len(s.Paths) == 0 {
		t.Fatal("the embedded openapi.yaml declares no paths")
	}
	return s
}

// newAPI builds a real API. No database is opened: this test only reads the route
// table, and requiring PostgreSQL to check that a document matches a slice would
// make the check skippable, which is how drift gets in.
func newAPI(t *testing.T) *api.API {
	t.Helper()

	cfg, err := config.Loader{
		Service: "nebula-controlplane",
		Getenv: func(k string) string {
			switch k {
			case "NEBULA_DATABASE_URL":
				return "postgres://nebula:not-a-real-secret@localhost:5432/nebula"
			case "NEBULA_AUTH_KEY_PEPPER":
				return "0123456789abcdef0123456789abcdef0123456789abcdef"
			}
			return ""
		},
	}.Load()
	if err != nil {
		t.Fatalf("loading config: %v", err)
	}

	a, err := api.New(cfg, slog.New(slog.DiscardHandler), nil)
	if err != nil {
		t.Fatalf("building api: %v", err)
	}
	return a
}

// Every mounted route must be documented and every documented operation must be
// mounted. Drift in the first direction is an undocumented endpoint; in the second
// it is a documented 404, which is worse because a client will call it.
func TestSpecMatchesMountedRoutes(t *testing.T) {
	t.Parallel()

	s := loadSpec(t)
	a := newAPI(t)

	documented := map[string]bool{}
	for path, ops := range s.Paths {
		for method := range ops {
			if !httpMethods[strings.ToLower(method)] {
				continue // "parameters", "summary" and friends
			}
			documented[strings.ToUpper(method)+" "+path] = true
		}
	}

	mounted := map[string]bool{}
	for _, r := range a.Routes() {
		mounted[r.Method+" "+r.Path] = true
	}

	for op := range mounted {
		if !documented[op] {
			t.Errorf("%s is mounted but not documented in packages/api/openapi.yaml", op)
		}
	}
	for op := range documented {
		if !mounted[op] {
			t.Errorf("%s is documented in packages/api/openapi.yaml but not mounted: "+
				"a client following the spec would get a 404", op)
		}
	}
}

// The wildcard names in a route pattern must match the parameter names in the
// document, or generated clients build URLs against parameters the handler never
// reads — and r.PathValue returns "" for a name that is not in the pattern, which
// this API turns into a 404 rather than an error.
func TestSpecPathParametersMatchPatterns(t *testing.T) {
	t.Parallel()

	s := loadSpec(t)
	a := newAPI(t)

	for _, r := range a.Routes() {
		wildcards := pathWildcards(r.Path)
		if len(wildcards) == 0 {
			continue
		}
		ops, ok := s.Paths[r.Path]
		if !ok {
			continue // reported by TestSpecMatchesMountedRoutes
		}

		var declared []string
		if node, ok := ops["parameters"]; ok {
			declared = append(declared, parameterNames(t, node)...)
		}
		if node, ok := ops[strings.ToLower(r.Method)]; ok {
			var op struct {
				Parameters yaml.Node `yaml:"parameters"`
			}
			if err := node.Decode(&op); err == nil && !op.Parameters.IsZero() {
				declared = append(declared, parameterNames(t, op.Parameters)...)
			}
		}

		for _, w := range wildcards {
			if !contains(declared, w) {
				t.Errorf("%s: path parameter {%s} is not declared in the specification (declared: %v)",
					r.Pattern, w, declared)
			}
		}
	}
}

// pathWildcards extracts {name} segments from a route path.
func pathWildcards(path string) []string {
	var out []string
	for _, segment := range strings.Split(path, "/") {
		if strings.HasPrefix(segment, "{") && strings.HasSuffix(segment, "}") {
			out = append(out, strings.TrimSuffix(strings.TrimPrefix(segment, "{"), "}"))
		}
	}
	return out
}

func parameterNames(t *testing.T, node yaml.Node) []string {
	t.Helper()
	var params []struct {
		Name string `yaml:"name"`
		In   string `yaml:"in"`
	}
	if err := node.Decode(&params); err != nil {
		return nil
	}
	var out []string
	for _, p := range params {
		if p.In == "path" {
			out = append(out, p.Name)
		}
	}
	return out
}

func contains(haystack []string, needle string) bool {
	for _, h := range haystack {
		if h == needle {
			return true
		}
	}
	return false
}

// A route table with a duplicate pattern would panic at mount time, inside
// http.ServeMux, when the process starts. Catching it here turns a crash-on-boot
// into a test failure.
func TestRouteTableIsWellFormed(t *testing.T) {
	t.Parallel()

	a := newAPI(t)
	routes := a.Routes()
	if len(routes) == 0 {
		t.Fatal("the route table is empty")
	}

	seen := map[string]bool{}
	for _, r := range routes {
		if seen[r.Pattern] {
			t.Errorf("route %q is registered twice; mounting it would panic", r.Pattern)
		}
		seen[r.Pattern] = true

		if r.Method == "" || r.Path == "" {
			t.Errorf("route %q did not split into a method and a path", r.Pattern)
		}
		if !httpMethods[strings.ToLower(r.Method)] {
			t.Errorf("route %q uses an unknown method %q", r.Pattern, r.Method)
		}
		if !strings.HasPrefix(r.Path, "/v1/") {
			t.Errorf("route %q is outside the /v1 prefix", r.Pattern)
		}
	}

	// Mounting must not panic, which is the real assertion: the pattern grammar is
	// the standard library's, so only the standard library can confirm it.
	mux := http.NewServeMux()
	a.Mount(mux)
}

// Only two endpoints may be reachable with a credential that carries no scope, and
// both are self-description. Anything else appearing here is an endpoint someone
// forgot to scope.
func TestOnlyDescriptiveEndpointsAreScopeless(t *testing.T) {
	t.Parallel()

	allowed := map[string]bool{
		"GET /v1/me":                          true,
		"GET /v1/lifecycle/deployment-states": true,
	}

	var scopeless []string
	for _, r := range newAPI(t).Routes() {
		if r.Scope == "" {
			scopeless = append(scopeless, r.Method+" "+r.Path)
		}
	}
	sort.Strings(scopeless)

	for _, op := range scopeless {
		if !allowed[op] {
			t.Errorf("%s requires no scope; every endpoint that touches tenant data must name one", op)
		}
	}
	if len(scopeless) != len(allowed) {
		t.Errorf("expected %d scopeless endpoints, found %d: %v", len(allowed), len(scopeless), scopeless)
	}
}

// Read endpoints must not require a write scope and write endpoints must not settle
// for a read scope. A GET behind :write means a read-only dashboard cannot render;
// a POST behind :read means a viewer can change production.
func TestScopesMatchMethodSafety(t *testing.T) {
	t.Parallel()

	for _, r := range newAPI(t).Routes() {
		scope := string(r.Scope)
		switch r.Method {
		case http.MethodGet, http.MethodHead:
			if strings.HasSuffix(scope, ":write") {
				t.Errorf("%s is a read but requires the write scope %s", r.Pattern, scope)
			}
		case http.MethodPost, http.MethodPatch, http.MethodPut, http.MethodDelete:
			if strings.HasSuffix(scope, ":read") {
				t.Errorf("%s changes state but only requires the read scope %s", r.Pattern, scope)
			}
		}
	}
}

func TestSpecIsServed(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	specpkg.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, specpkg.SpecPath, http.NoBody))

	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s = %d, want 200", specpkg.SpecPath, rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); !strings.Contains(got, "yaml") {
		t.Errorf("Content-Type = %q, want a YAML type", got)
	}
	if rec.Body.Len() == 0 {
		t.Error("the specification was served empty")
	}

	etag := rec.Header().Get("ETag")
	if etag == "" {
		t.Fatal("no ETag was served; a client cannot cache the spec")
	}

	// A conditional request must be answered 304, or every dashboard load
	// re-transfers the whole document.
	req := httptest.NewRequest(http.MethodGet, specpkg.SpecPath, http.NoBody)
	req.Header.Set("If-None-Match", etag)
	rec = httptest.NewRecorder()
	specpkg.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotModified {
		t.Errorf("conditional GET = %d, want 304", rec.Code)
	}

	// The spec describes the API's shape, not any tenant's data, so it is served
	// without a credential — deliberately, and a test says so.
	req = httptest.NewRequest(http.MethodPost, specpkg.SpecPath, http.NoBody)
	rec = httptest.NewRecorder()
	specpkg.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST %s = %d, want 405", specpkg.SpecPath, rec.Code)
	}
}

// The number of states and edges the lifecycle endpoint reports must match the
// package, and the package is compared against the migration by
// packages/lifecycle/deployment_test.go. This is the third link in that chain: it
// catches a response that filters or duplicates edges on the way out.
func TestLifecycleResponseMatchesThePackage(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/lifecycle/deployment-states", http.NoBody)
	req = req.WithContext(auth.WithIdentity(req.Context(), auth.Identity{
		OrgID:  uuid.New(),
		Scopes: auth.AllScopes(),
	}))

	mux := http.NewServeMux()
	newAPI(t).Mount(mux)

	// Mount wraps every route in authentication, which needs a database. The handler
	// is therefore called directly, with an identity already in the context: the
	// endpoint is pure and the authentication path has its own tests.
	if err := newAPI(t).DeploymentLifecycleForTest(rec, req); err != nil {
		t.Fatalf("handler: %v", err)
	}

	var got struct {
		States []struct {
			Name string   `json:"name"`
			Next []string `json:"next"`
		} `json:"states"`
		Transitions []struct {
			From string `json:"from"`
			To   string `json:"to"`
			Note string `json:"note"`
		} `json:"transitions"`
		Reasons []string `json:"reasons"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decoding the response: %v", err)
	}

	if len(got.States) != len(lifecycle.States()) {
		t.Errorf("reported %d states, the package has %d", len(got.States), len(lifecycle.States()))
	}
	if len(got.Transitions) != len(lifecycle.Edges()) {
		t.Errorf("reported %d transitions, the package has %d",
			len(got.Transitions), len(lifecycle.Edges()))
	}
	if len(got.Reasons) != len(lifecycle.Reasons()) {
		t.Errorf("reported %d reasons, the package has %d", len(got.Reasons), len(lifecycle.Reasons()))
	}
	for _, e := range got.Transitions {
		if e.Note == "" {
			t.Errorf("edge %s -> %s is reported with no explanation", e.From, e.To)
		}
		if !lifecycle.CanTransition(lifecycle.State(e.From), lifecycle.State(e.To)) {
			t.Errorf("reported edge %s -> %s is not in the package's graph", e.From, e.To)
		}
	}
}
