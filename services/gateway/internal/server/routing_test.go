package server_test

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/adityasatwar321/nebula/packages/routing"
	"github.com/adityasatwar321/nebula/services/gateway/internal/routes"
)

// setRoutes replaces the harness's table. FromPayload accepts both shapes: a
// target with endpoints is served from them, one without waits for discovery.
func (h *harness) setRoutes(rs ...*routes.Route) {
	h.t.Helper()
	tbl, err := routes.FromPayload(&routes.Payload{Version: "test", Routes: rs}, time.Unix(1_800_000_000, 0))
	if err != nil {
		h.t.Fatal(err)
	}
	h.router.SetTable(tbl)
}

type nebulaBlock struct {
	Nebula struct {
		Deployment string `json:"deployment"`
		Attempts   int    `json:"attempts"`
		Degraded   string `json:"degraded"`
	} `json:"nebula"`
}

func readNebula(t *testing.T, resp *http.Response) nebulaBlock {
	t.Helper()
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status %d: %s", resp.StatusCode, b)
	}
	var out nebulaBlock
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// A replica that cannot be reached is skipped before any work starts: the request
// is placed on another replica and succeeds, and says it took two attempts.
func TestUnreachableReplicaIsSkipped(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	dead := newWorker(t)
	deadURL := dead.srv.URL
	dead.srv.Close()

	h.setRoutes(&routes.Route{ID: "r-tiny", Model: "tiny", Org: "acme", ContextWindow: 512,
		Capabilities: routes.Capabilities{Streaming: true},
		Targets: []*routes.Target{{Deployment: "tiny-a", DeploymentID: "dep-a", ModelVersion: "tiny:v1", Weight: 100,
			Endpoints: []string{deadURL, h.worker.srv.URL}}}})

	sawRetry := false
	for i := 0; i < 4; i++ {
		nb := readNebula(t, h.do("POST", "/v1/chat/completions", keyAcme, chatBody))
		if nb.Nebula.Attempts == 2 {
			sawRetry = true
		}
		if nb.Nebula.Attempts < 1 || nb.Nebula.Attempts > 2 {
			t.Fatalf("attempts %d", nb.Nebula.Attempts)
		}
	}
	if !sawRetry {
		t.Log("the dead replica was never chosen first; the retry path was not exercised this run")
	}

	// Streaming takes the same path, before the first byte.
	resp := h.do("POST", "/v1/chat/completions", keyAcme,
		`{"model":"tiny","messages":[{"role":"user","content":"x"}],"stream":true}`)
	if resp.StatusCode != 200 {
		t.Fatalf("stream: %d", resp.StatusCode)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}

// Nothing eligible is a 503 that says so and says when to retry, and nothing is
// charged to the caller's limits.
func TestNoEligibleEndpointIs503(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.setRoutes(&routes.Route{ID: "r-tiny", Model: "tiny", Org: "acme", ContextWindow: 512,
		Targets: []*routes.Target{{Deployment: "tiny-a", DeploymentID: "dep-a", ModelVersion: "tiny:v1", Weight: 100}}})
	resp := h.do("POST", "/v1/chat/completions", keyAcme, chatBody)
	e := errorOf(t, resp)
	if resp.StatusCode != 503 || e.Code != "no_healthy_endpoint" || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("%d %+v", resp.StatusCode, e)
	}
	if n := len(h.usage.Records()); n != 0 {
		t.Errorf("%d usage records for a request that was never admitted", n)
	}
}

// A failover policy serves from its fallback route when the primary has nothing
// eligible, and the response says it was degraded.
func TestFailoverPolicy(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	cfg, _ := json.Marshal(map[string]string{"primary": "least_loaded", "fallback_model": "backup"})
	h.setRoutes(
		&routes.Route{ID: "r-primary", Model: "primary", Org: "acme", ContextWindow: 512,
			Policy:  routing.Policy{Strategy: routing.StrategyFailover, Config: cfg},
			Targets: []*routes.Target{{Deployment: "p", DeploymentID: "dep-p", ModelVersion: "p:v1", Weight: 100}}},
		&routes.Route{ID: "r-backup", Model: "backup", Org: "acme", ContextWindow: 512,
			Targets: []*routes.Target{{Deployment: "b", DeploymentID: "dep-b", ModelVersion: "b:v1", Weight: 100,
				Endpoints: []string{h.worker.srv.URL}}}},
	)
	resp := h.do("POST", "/v1/chat/completions", keyAcme,
		`{"model":"primary","messages":[{"role":"user","content":"hi"}],"max_tokens":4}`)
	if got := resp.Header.Get("X-Nebula-Degraded"); got != "failover:backup" {
		t.Errorf("X-Nebula-Degraded %q", got)
	}
	nb := readNebula(t, resp)
	if nb.Nebula.Deployment != "b" || nb.Nebula.Degraded != "failover:backup" {
		t.Fatalf("%+v", nb.Nebula)
	}
}
