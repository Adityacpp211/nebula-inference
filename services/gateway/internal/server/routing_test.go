package server_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/adityasatwar321/nebula/packages/config"
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
		nb := readNebula(t, h.do("POST", "/v1/chat/completions", keyAcme, chatBody)) //nolint:bodyclose // readNebula closes it
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

// Backpressure end to end: a worker answering 429 is marked saturated, the request
// goes back to the admission queue instead of failing, and is served once the
// Retry-After has passed.
func TestSaturatedWorkerIsWaitedOut(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	var calls atomic.Int32
	ok := replyJSON("hello", "stop", 7, 2)
	h.worker.handle = func(w http.ResponseWriter, r *http.Request, req workerReq) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.Header().Set("X-Nebula-Reason", "worker_saturated")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = io.WriteString(w, `{"error":{"message":"full","type":"rate_limit_error","code":"worker_saturated"}}`)
			return
		}
		ok(w, r, req)
	}
	started := time.Now()
	resp := h.do("POST", "/v1/chat/completions", keyAcme, chatBody)
	var body struct {
		Nebula struct {
			Attempts       int    `json:"attempts"`
			GatewayQueueMS *int64 `json:"gateway_queue_ms"`
		} `json:"nebula"`
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("%d %s", resp.StatusCode, b)
	}
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body.Nebula.Attempts != 2 || body.Nebula.GatewayQueueMS == nil || *body.Nebula.GatewayQueueMS < 900 {
		t.Fatalf("want 2 attempts and about a second queued, got %+v (queued %v)", body.Nebula, body.Nebula.GatewayQueueMS)
	}
	if d := time.Since(started); d > 5*time.Second {
		t.Fatalf("took %v", d)
	}
}

// The metrics a dashboard reads exist after one ordinary request, with the
// documented labels (docs/observability.md §2.1, §2.2).
func TestRequestRecordsMetrics(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	if resp := h.do("POST", "/v1/chat/completions", keyAcme, chatBody); resp.StatusCode != 200 {
		t.Fatalf("%d", resp.StatusCode)
	} else {
		resp.Body.Close()
	}
	h.do("POST", "/v1/chat/completions", keyAcme, `{"model":"nope","messages":[{"role":"user","content":"x"}]}`).Body.Close()

	rec := httptest.NewRecorder()
	h.metrics.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", http.NoBody))
	text := rec.Body.String()
	for _, want := range []string{
		`nebula_requests_total{deployment="tiny-a",deployment_id="0192f3c1-0000-7000-8000-00000000000a",endpoint="chat.completions",error_class="",model_version="tiny:v1",route="tiny",status_class="2xx",variant="baseline"} 1`,
		`nebula_requests_total{deployment="",deployment_id="",endpoint="chat.completions",error_class="not_found_error",model_version="",route="",status_class="4xx",variant=""} 1`,
		`nebula_request_duration_seconds_count{deployment="tiny-a",deployment_id="0192f3c1-0000-7000-8000-00000000000a",endpoint="chat.completions",route="tiny",streamed="false"} 1`,
		`nebula_tokens_total{deployment="tiny-a",direction="completion",model_version="tiny:v1"} 2`,
		`nebula_route_decisions_total{outcome="selected",route="tiny",strategy="least_loaded"} 1`,
		`nebula_request_attempts_total{deployment="tiny-a",outcome="succeeded"} 1`,
		`nebula_inflight_requests{deployment="tiny-a"} 0`,
		`nebula_endpoints{deployment="tiny-a",state="ready"} 1`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("missing %s", want)
		}
	}
	if t.Failed() {
		t.Log(text)
	}
}

// Under a total outage — every replica answers 503 before doing any work — the
// retry budget, not the attempt cap, bounds the extra load: failovers stay at
// the budget's ratio of requests, and the refusals are counted.
func TestRetryBudgetCapsAmplificationUnderOutage(t *testing.T) {
	t.Parallel()
	h := newHarness(t, func(c *config.Config) {
		c.Gateway.RetryBudgetRatio, c.Gateway.RetryBudgetMinPerSecond = 0.1, 0
	})
	var calls atomic.Int64
	var urls []string
	for range 3 {
		down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			calls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Nebula-Reason", "model_not_ready")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, `{"error":{"type":"service_unavailable","code":"model_not_ready","message":"loading"}}`)
		}))
		t.Cleanup(down.Close)
		urls = append(urls, down.URL)
	}
	h.setRoutes(&routes.Route{ID: "r-out", Model: "tiny", Org: "acme", ContextWindow: 512,
		Capabilities: routes.Capabilities{Streaming: true},
		Targets: []*routes.Target{{Deployment: "tiny-out", DeploymentID: "dep-out", ModelVersion: "tiny:v1",
			Weight: 100, Endpoints: urls}}})

	const n = 60
	for range n {
		resp := h.do("POST", "/v1/chat/completions", keyAcme, chatBody)
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode < 500 {
			t.Fatalf("status %d during a total outage", resp.StatusCode)
		}
	}
	retries := testutil.ToFloat64(h.metrics.Counter("nebula_retries_total").WithLabelValues("tiny-out", "unavailable"))
	refused := testutil.ToFloat64(h.metrics.Counter("nebula_retries_rejected_budget_total").WithLabelValues("tiny-out"))
	// Requests that reached a replica earn 0.1 retries each.
	dispatched := float64(calls.Load()) - retries
	if retries > 0.1*dispatched+1 {
		t.Errorf("%v failovers for %v dispatched requests: the budget allows %v", retries, dispatched, 0.1*dispatched)
	}
	if refused == 0 {
		t.Error("no failover was refused: the budget was never exercised")
	}
	if amp := float64(calls.Load()) / dispatched; amp > 1.15 {
		t.Errorf("amplification %.2f under a total outage, want <= 1.15", amp)
	}
	t.Logf("%d requests: %v dispatched, %v failovers, %v refused by the budget", n, dispatched, retries, refused)
}
