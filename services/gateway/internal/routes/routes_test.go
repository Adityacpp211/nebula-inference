package routes_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/adityasatwar321/nebula/services/gateway/internal/routes"
)

func target(name string, weight int, eps ...string) *routes.Target {
	return &routes.Target{Deployment: name, ModelVersion: name + ":v1", Weight: weight, Endpoints: eps}
}

func TestBuildReportsEveryProblem(t *testing.T) {
	t.Parallel()
	_, err := routes.Build([]*routes.Route{
		{Model: "bad name!", Org: "", ContextWindow: 0, Targets: nil},
		{Model: "ok", Org: "acme", ContextWindow: 10, Targets: []*routes.Target{
			target("a", 60, "http://w:1"), target("a", 30, "ftp://nope"),
		}},
		{Model: "ok", Org: "acme", ContextWindow: 10, Targets: []*routes.Target{target("b", 100, "http://w:1")}},
	}, time.Now())
	var le *routes.LoadError
	if !errors.As(err, &le) {
		t.Fatalf("want *LoadError, got %v", err)
	}
	joined := strings.Join(le.Problems, "\n")
	for _, want := range []string{
		"not a valid route name", "org is required", "context_window must be positive",
		"at least one target", "appears twice", "must be a worker base URL",
		"weights must sum to 100, got 90", `already has a route named "ok"`,
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("missing problem %q in:\n%s", want, joined)
		}
	}
}

// A route belongs to one org. Another org asking for the same name gets exactly
// what it would get for a name that does not exist.
func TestLookupIsTenantScoped(t *testing.T) {
	t.Parallel()
	tbl, err := routes.Build([]*routes.Route{
		{Model: "chat", Org: "acme", ContextWindow: 10, Targets: []*routes.Target{target("a", 100, "http://w:1")}},
		{Model: "chat", Org: "globex", ContextWindow: 10, Targets: []*routes.Target{target("g", 100, "http://w:2")}},
	}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	a, ok := tbl.Lookup("acme", "chat")
	if !ok || a.Targets[0].Deployment != "a" {
		t.Fatalf("acme/chat: %+v %v", a, ok)
	}
	if _, ok := tbl.Lookup("initech", "chat"); ok {
		t.Error("an org with no route must not see another org's")
	}
	if len(tbl.ForOrg("globex")) != 1 || len(tbl.ForOrg("initech")) != 0 {
		t.Error("ForOrg must list only the org's own routes")
	}
	if a.ID == "" || a.ID == mustLookup(t, tbl, "globex", "chat").ID {
		t.Errorf("route ids must be set and distinct per org: %q", a.ID)
	}
}

func mustLookup(t *testing.T, tbl *routes.Table, org, model string) *routes.Route {
	t.Helper()
	r, ok := tbl.Lookup(org, model)
	if !ok {
		t.Fatalf("%s/%s not found", org, model)
	}
	return r
}

func TestPinned(t *testing.T) {
	t.Parallel()
	tg1 := target("base", 50, "http://w:1")
	tg1.DeploymentID = "0192f3c1-0000-7000-8000-000000000001"
	tbl, _ := routes.Build([]*routes.Route{{Model: "chat", Org: "acme", ContextWindow: 10,
		Targets: []*routes.Target{tg1, target("canary", 50, "http://w:2")}}}, time.Now())
	r := mustLookup(t, tbl, "acme", "chat")
	for _, pin := range []string{"base", "0192F3C1-0000-7000-8000-000000000001"} {
		got, err := r.Pinned(pin)
		if err != nil || got.Deployment != "base" {
			t.Errorf("pin %q: got %v %v", pin, got, err)
		}
	}
	if _, err := r.Pinned("nope"); !errors.Is(err, routes.ErrNoTarget) {
		t.Errorf("unknown pin: %v", err)
	}
}

// A table from the control plane carries no endpoints but must name every
// deployment by id, because discovery keys on it.
func TestFromPayload(t *testing.T) {
	t.Parallel()
	tbl, err := routes.FromPayload(&routes.Payload{Version: "v7", Routes: []*routes.Route{{
		ID: "r1", Model: "chat", Org: "acme", ContextWindow: 4096,
		Targets: []*routes.Target{{Deployment: "a", DeploymentID: "ID-A", ModelVersion: "m:v1", Weight: 100}},
	}}}, time.Now())
	if err != nil || tbl.Version != "v7" {
		t.Fatalf("%v %v", tbl, err)
	}
	r := mustLookup(t, tbl, "acme", "chat")
	if r.Targets[0].Key(r) != "id-a" {
		t.Errorf("key %q", r.Targets[0].Key(r))
	}
	_, err = routes.FromPayload(&routes.Payload{Routes: []*routes.Route{{
		Model: "chat", Org: "acme", ContextWindow: 4096,
		Targets: []*routes.Target{{Deployment: "a", ModelVersion: "m:v1", Weight: 100}},
	}}}, time.Now())
	if err == nil || !strings.Contains(err.Error(), "deployment_id is required") {
		t.Fatalf("want a deployment_id problem, got %v", err)
	}
}

func TestLoadYAMLAndJSON(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	yml := filepath.Join(dir, "routes.yaml")
	if err := os.WriteFile(yml, []byte(`
routes:
  - model: tiny
    org: dev
    context_window: 4096
    chat_template: plain
    timeout: 30s
    capabilities: {streaming: true}
    targets:
      - {deployment: tiny-a, model_version: "nebula-tiny:fixture", weight: 100, endpoints: ["http://127.0.0.1:9101/"]}
`), 0o600); err != nil {
		t.Fatal(err)
	}
	tbl, err := routes.Load(yml, time.Unix(1_800_000_000, 0))
	if err != nil {
		t.Fatal(err)
	}
	r := mustLookup(t, tbl, "dev", "tiny")
	if r.Timeout != 30*time.Second || r.ChatTemplate != routes.TemplatePlain || !r.Capabilities.Streaming ||
		r.Targets[0].Endpoints[0] != "http://127.0.0.1:9101" || r.Created != 1_800_000_000 || r.Task != routes.TaskChat {
		t.Errorf("loaded route: %+v", r)
	}

	// An unknown key is a typo, not something to ignore.
	bad := filepath.Join(dir, "bad.json")
	_ = os.WriteFile(bad, []byte(`{"routes":[{"model":"x","org":"o","context_window":1,"wieght":5}]}`), 0o600)
	if _, err := routes.Load(bad, time.Now()); err == nil || !strings.Contains(err.Error(), "wieght") {
		t.Errorf("unknown JSON field must be refused, got %v", err)
	}
}
