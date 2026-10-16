package signals_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/adityasatwar321/nebula/packages/autoscale"
	"github.com/adityasatwar321/nebula/services/autoscaler/internal/signals"
)

var (
	depA = uuid.MustParse("11111111-1111-4111-8111-111111111111")
	depB = uuid.MustParse("22222222-2222-4222-8222-222222222222")
)

// fakeProm answers each query by the metric it names.
func fakeProm(t *testing.T, answers map[string][]any, fail bool) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/query" {
			http.NotFound(w, r)
			return
		}
		if fail {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "error", "errorType": "unavailable", "error": "down"})
			return
		}
		q := r.URL.Query().Get("query")
		var result []any
		for metric, rows := range answers {
			if strings.Contains(q, metric) {
				result = rows
			}
		}
		if result == nil {
			result = []any{}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "success",
			"data": map[string]any{"resultType": "vector", "result": result}})
	}))
}

func sample(id uuid.UUID, v string) map[string]any {
	return map[string]any{"metric": map[string]string{"deployment_id": id.String()}, "value": []any{1.7e9, v}}
}

func TestReadAndBuildSignals(t *testing.T) {
	t.Parallel()
	srv := fakeProm(t, map[string][]any{
		"nebula_worker_in_flight":   {sample(depA, "6"), sample(depB, "0")},
		"nebula_queue_depth":        {sample(depA, "10")},
		"nebula_worker_slots_total": {sample(depA, "4"), sample(depB, "4")},
		// A quantile over no requests is NaN: an absence, not a zero.
		"nebula_queue_wait_seconds_bucket": {sample(depA, "1250"), sample(depB, "NaN"),
			map[string]any{"metric": map[string]string{"deployment_id": "not-a-uuid"}, "value": []any{1.7e9, "5"}}},
	}, false)
	defer srv.Close()
	p := &signals.Prometheus{BaseURL: srv.URL, Window: time.Minute}
	r := p.Read(context.Background())
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	s, _ := autoscale.ParseSettings(json.RawMessage(`{"enabled":true}`))

	a := signals.For(r, depA, 2, s)
	if a[0].Stale || a[0].Observed != 8 || a[0].Target != 4 {
		t.Errorf("concurrency: (6 at workers + 10 queued at gateways) / 2 replicas = 8 vs 4 slots: %+v", a[0])
	}
	if a[1].Stale || a[1].Observed != 1250 || a[1].Target != 500 {
		t.Errorf("queue wait: %+v", a[1])
	}

	b := signals.For(r, depB, 3, s)
	if b[0].Stale || b[0].Observed != 0 {
		t.Errorf("idle workers and no gateway queue are a real zero: %+v", b[0])
	}
	if !b[1].Stale {
		t.Errorf("NaN must be missing: %+v", b[1])
	}

	// A deployment Prometheus knows nothing about is missing, not idle.
	c := signals.For(r, uuid.New(), 2, s)
	if !c[0].Stale || !c[1].Stale {
		t.Errorf("unknown deployment: %+v", c)
	}

	// An explicit concurrency target wins over the workers' slots.
	s.TargetConcurrency = 2
	if a := signals.For(r, depA, 2, s); a[0].Target != 2 {
		t.Errorf("target: %+v", a[0])
	}
}

func TestPrometheusDownMeansEverySignalMissing(t *testing.T) {
	t.Parallel()
	srv := fakeProm(t, nil, true)
	defer srv.Close()
	r := (&signals.Prometheus{BaseURL: srv.URL}).Read(context.Background())
	if r.Err == nil {
		t.Fatal("an error response must fail the reading")
	}
	s, _ := autoscale.ParseSettings(json.RawMessage(`{"enabled":true}`))
	for _, sig := range signals.For(r, depA, 2, s) {
		if !sig.Stale {
			t.Errorf("%s is not stale", sig.Name)
		}
	}

	unreachable := (&signals.Prometheus{BaseURL: "http://127.0.0.1:1"}).Read(context.Background())
	if unreachable.Err == nil {
		t.Fatal("an unreachable Prometheus must fail the reading")
	}
}

func TestQueriesUseTheWindowAndJoinOnDeploymentID(t *testing.T) {
	t.Parallel()
	q := (&signals.Prometheus{Window: 90 * time.Second}).Queries()
	if !strings.Contains(q["queue_wait_ms"], "[90s]") {
		t.Errorf("window: %s", q["queue_wait_ms"])
	}
	for name, query := range q {
		if !strings.Contains(query, "by (deployment_id") {
			t.Errorf("%s does not group by deployment_id: %s", name, query)
		}
	}
}
