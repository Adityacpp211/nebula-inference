package controlplane_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/adityasatwar321/nebula/packages/auth"
	"github.com/adityasatwar321/nebula/packages/testsupport/dbtest"
)

type routeBody struct {
	ID            uuid.UUID `json:"id"`
	ModelName     string    `json:"model_name"`
	RoutingPolicy *struct {
		Name     string `json:"name"`
		Strategy string `json:"strategy"`
	} `json:"routing_policy"`
	Targets []struct {
		DeploymentID uuid.UUID `json:"deployment_id"`
		Deployment   string    `json:"deployment"`
		ModelVersion string    `json:"model_version"`
		Weight       int       `json:"weight"`
		Label        *string   `json:"label"`
		State        string    `json:"state"`
	} `json:"targets"`
}

func target(dep string, weight int, label string) map[string]any {
	return map[string]any{"deployment": dep, "weight": weight, "label": label}
}

func TestRouteLifecycle(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	_, v1 := f.createReadyVersion(t, "chatty", "v1")
	_, v2 := f.createReadyVersion(t, "chatty", "v2")
	blue := f.createDeployment(t, "blue", v1)
	f.createDeployment(t, "green", v2)

	var rt routeBody
	code := f.do(t, "POST", "/v1/routes", map[string]any{
		"model_name": "chatty-chat",
		"targets":    []any{target("blue", 50, "baseline"), map[string]any{"deployment_id": f.deploymentID(t, "green"), "weight": 50}},
	}, &rt)
	if code != http.StatusCreated || rt.ModelName != "chatty-chat" || len(rt.Targets) != 2 || rt.RoutingPolicy != nil {
		t.Fatalf("create: %d %+v", code, rt)
	}
	if rt.Targets[0].ModelVersion == "" || rt.Targets[0].State != "pending" {
		t.Errorf("targets should carry version and state: %+v", rt.Targets[0])
	}

	// Weights are replaced as a set, and the policy can be named.
	var upd routeBody
	code = f.do(t, "PATCH", "/v1/routes/"+rt.ID.String(), map[string]any{
		"routing_policy": "round-robin",
		"targets":        []any{target("blue", 90, "baseline"), target("green", 10, "canary")},
	}, &upd)
	if code != 200 || upd.RoutingPolicy == nil || upd.RoutingPolicy.Strategy != "round_robin" ||
		upd.Targets[0].Weight != 90 || upd.Targets[1].Weight != 10 {
		t.Fatalf("update: %d %+v", code, upd)
	}
	// null returns the route to the default policy.
	code = f.do(t, "PATCH", "/v1/routes/"+rt.ID.String(), map[string]any{"routing_policy": nil}, &upd)
	if code != 200 || upd.RoutingPolicy != nil {
		t.Fatalf("policy reset: %d %+v", code, upd.RoutingPolicy)
	}

	var list struct {
		Data []routeBody `json:"data"`
	}
	if code := f.do(t, "GET", "/v1/routes", nil, &list); code != 200 || len(list.Data) != 1 {
		t.Fatalf("list: %d %d", code, len(list.Data))
	}

	// A routed deployment cannot be deleted out from under its route.
	f.do(t, "POST", "/v1/deployments/"+blue.String()+"/stop", map[string]any{}, nil)
	f.transition(t, blue, "stopping", "stopped", "Drained")
	var env apiError
	if code := f.do(t, "DELETE", "/v1/deployments/"+blue.String(), nil, &env); code != http.StatusConflict {
		t.Fatalf("deleting a routed deployment: %d %+v", code, env)
	}

	if code := f.do(t, "DELETE", "/v1/routes/"+rt.ID.String(), nil, nil); code != http.StatusNoContent {
		t.Fatalf("delete route: %d", code)
	}
	if code := f.do(t, "DELETE", "/v1/deployments/"+blue.String(), nil, nil); code != http.StatusNoContent {
		t.Fatalf("the deployment is deletable once unrouted: %d", code)
	}
	var n int
	_ = f.Pool.QueryRow(dbtest.Context(t),
		`SELECT count(*) FROM audit_logs WHERE resource_id = $1 AND action LIKE 'route.%'`, rt.ID).Scan(&n)
	if n != 4 {
		t.Errorf("%d route audit records, want 4 (create, 2 updates, delete)", n)
	}
}

func TestRouteRefusals(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	_, v := f.createReadyVersion(t, "refuse", "v1")
	f.createDeployment(t, "one", v)
	f.createDeployment(t, "two", v)

	for name, tc := range map[string]struct {
		body   map[string]any
		status int
		code   string
	}{
		"weights short of 100": {map[string]any{"model_name": "r1",
			"targets": []any{target("one", 50, ""), target("two", 40, "")}}, 400, "invalid_weights"},
		"no targets":  {map[string]any{"model_name": "r2", "targets": []any{}}, 400, "missing_field"},
		"bad name":    {map[string]any{"model_name": "-bad", "targets": []any{target("one", 100, "")}}, 400, "invalid_model_name"},
		"unknown dep": {map[string]any{"model_name": "r3", "targets": []any{target("nope", 100, "")}}, 422, "deployment_not_found"},
		"twice": {map[string]any{"model_name": "r4",
			"targets": []any{target("one", 50, ""), target("one", 50, "")}}, 400, "duplicate_target"},
		"unknown policy": {map[string]any{"model_name": "r5", "routing_policy": "psychic",
			"targets": []any{target("one", 100, "")}}, 422, "routing_policy_not_found"},
		"two baselines": {map[string]any{"model_name": "r6", "targets": []any{
			map[string]any{"deployment": "one", "weight": 50, "is_baseline": true},
			map[string]any{"deployment": "two", "weight": 50, "is_baseline": true}}}, 400, "multiple_baselines"},
	} {
		var env apiError
		if got := f.do(t, "POST", "/v1/routes", tc.body, &env); got != tc.status || env.Error.Code != tc.code {
			t.Errorf("%s: %d %q, want %d %q (%s)", name, got, env.Error.Code, tc.status, tc.code, env.Error.Message)
		}
	}

	if code := f.do(t, "POST", "/v1/routes", map[string]any{"model_name": "taken",
		"targets": []any{target("one", 100, "")}}, nil); code != 201 {
		t.Fatalf("create: %d", code)
	}
	var env apiError
	if code := f.do(t, "POST", "/v1/routes", map[string]any{"model_name": "taken",
		"targets": []any{target("two", 100, "")}}, &env); code != http.StatusConflict {
		t.Fatalf("duplicate name: %d %+v", code, env)
	}

	var policies struct {
		Data []struct {
			Name     string `json:"name"`
			Strategy string `json:"strategy"`
			BuiltIn  bool   `json:"built_in"`
		} `json:"data"`
	}
	if code := f.do(t, "GET", "/v1/policies/routing", nil, &policies); code != 200 || len(policies.Data) < 6 {
		t.Fatalf("policies: %d %+v", code, policies)
	}
}

// The gateway's view: every route of every org, versioned, with 304 for no change.
func TestInternalRoutingTable(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	_, v := f.createReadyVersion(t, "tbl", "v1")
	id := f.createDeployment(t, "tbl-a", v)
	if code := f.do(t, "POST", "/v1/routes", map[string]any{"model_name": "tbl-route",
		"targets": []any{target("tbl-a", 100, "baseline")}}, nil); code != 201 {
		t.Fatalf("create: %d", code)
	}

	service, _ := signer(t, 10*time.Second, nil).Sign(auth.ServiceIdentity("nebula-gateway"), "",
		auth.AudienceControlPlane, "nebula-gateway")
	var table struct {
		Version string `json:"version"`
		Routes  []struct {
			Org           string `json:"org"`
			Model         string `json:"model"`
			Task          string `json:"task"`
			ContextWindow int    `json:"context_window"`
			ChatTemplate  string `json:"chat_template"`
			Policy        struct {
				Strategy string `json:"strategy"`
			} `json:"policy"`
			Targets []struct {
				DeploymentID uuid.UUID `json:"deployment_id"`
				Weight       int       `json:"weight"`
				ModelVersion string    `json:"model_version"`
			} `json:"targets"`
		} `json:"routes"`
	}
	status, h := f.send(t, "GET", "/internal/v1/routing-table", service, &table)
	if status != 200 || table.Version == "" || h.Get("ETag") == "" {
		t.Fatalf("%d %q", status, table.Version)
	}
	found := false
	for _, rt := range table.Routes {
		if rt.Model == "tbl-route" {
			found = true
			if rt.Task != "chat" || rt.ContextWindow <= 0 || rt.ChatTemplate != "chatml" ||
				rt.Policy.Strategy != "least_loaded" || len(rt.Targets) != 1 ||
				rt.Targets[0].DeploymentID != id || rt.Targets[0].Weight != 100 {
				t.Errorf("route: %+v", rt)
			}
		}
	}
	if !found {
		t.Fatal("the route is missing from the table")
	}

	// Unchanged content is a 304.
	req, _ := http.NewRequestWithContext(dbtest.Context(t), "GET", f.Server.URL+"/internal/v1/routing-table", nil)
	req.Header.Set(auth.HeaderAuthContext, service)
	req.Header.Set("If-None-Match", h.Get("ETag"))
	resp, err := f.Server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotModified {
		t.Fatalf("conditional GET: %d", resp.StatusCode)
	}

	// A tenant identity never reads it.
	tenant, _ := signer(t, 10*time.Second, nil).Sign(f.identity(), "", auth.AudienceControlPlane, "nebula-gateway")
	if status, _ := f.send(t, "GET", "/internal/v1/routing-table", tenant, nil); status != http.StatusForbidden {
		t.Fatalf("tenant: %d", status)
	}
}

// deploymentID looks a deployment up by name.
func (f *fixture) deploymentID(t *testing.T, name string) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	if err := f.Pool.QueryRow(dbtest.Context(t),
		`SELECT id FROM deployments WHERE org_id = $1 AND name = $2 AND deleted_at IS NULL`, f.OrgID, name).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}
