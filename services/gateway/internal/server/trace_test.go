package server_test

import (
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// spans records every span the gateway emits during this package's tests. The
// provider is global, so it is installed once, before any test runs; tests find
// their own spans by trace id.
var spans = tracetest.NewSpanRecorder()

func TestMain(m *testing.M) {
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans)))
	os.Exit(m.Run())
}

// The span tree of docs/observability.md §3.1 for one request, and the worker
// handed the dispatch attempt as its parent — the link that makes the worker's
// own spans children of the gateway's.
func TestOneRequestProducesTheDocumentedSpanTree(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	resp := h.do("POST", "/v1/chat/completions", keyAcme, chatBody,
		"traceparent", "00-"+traceID+"-00f067aa0ba902b7-01", "X-Request-Id", "req-trace-1")
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("%d", resp.StatusCode)
	}

	byName := map[string]sdktrace.ReadOnlySpan{}
	for _, s := range spans.Ended() {
		if s.SpanContext().TraceID().String() == traceID {
			byName[s.Name()] = s
		}
	}
	root, ok := byName["gateway.request"]
	if !ok {
		t.Fatalf("no gateway.request span in the trace; got %v", names(byName))
	}
	if root.Parent().SpanID().String() != "00f067aa0ba902b7" {
		t.Errorf("the root span does not continue the client's trace: parent %s", root.Parent().SpanID())
	}
	attrs := map[string]string{}
	for _, kv := range root.Attributes() {
		attrs[string(kv.Key)] = kv.Value.Emit()
	}
	if attrs["route"] != "tiny" || attrs["request_id"] != "req-trace-1" || attrs["http.response.status_code"] != "200" {
		t.Errorf("root attributes: %v", attrs)
	}
	for _, child := range []string{"gateway.authenticate", "gateway.validate", "router.resolve", "router.select",
		"gateway.ratelimit", "dispatch.attempt"} {
		s, ok := byName[child]
		if !ok {
			t.Errorf("missing span %s (have %v)", child, names(byName))
			continue
		}
		if s.Parent().SpanID() != root.SpanContext().SpanID() {
			t.Errorf("%s is not a child of gateway.request", child)
		}
	}
	if _, queued := byName["queue.wait"]; queued {
		t.Error("queue.wait must appear only when the request actually waited")
	}

	// The worker's parent is the attempt span.
	attempt := byName["dispatch.attempt"]
	h.worker.mu.Lock()
	defer h.worker.mu.Unlock()
	var tp string
	for _, r := range h.worker.requests {
		if r.Header.Get("X-Request-Id") == "req-trace-1" {
			tp = r.Header.Get("traceparent")
		}
	}
	if !strings.Contains(tp, traceID) || !strings.Contains(tp, attempt.SpanContext().SpanID().String()) {
		t.Errorf("worker traceparent %q is not a child of dispatch.attempt %s", tp, attempt.SpanContext().SpanID())
	}
}

func names(m map[string]sdktrace.ReadOnlySpan) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// The log contract (docs/observability.md §4): every line a request produces is
// JSON carrying request_id and trace_id, and the prompt never appears, at any
// level, even on failure.
func TestRequestLogsCarryCorrelationAndNoPrompt(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	secret := "my bank password is hunter2"
	body := `{"model":"tiny","messages":[{"role":"user","content":"` + secret + `"}],"max_tokens":4}`
	h.do("POST", "/v1/chat/completions", keyAcme, body, "X-Request-Id", "req-log-1").Body.Close()
	// A failing request logs more, and must not leak either.
	h.worker.handle = func(w http.ResponseWriter, _ *http.Request, _ workerReq) { w.WriteHeader(500) }
	h.do("POST", "/v1/chat/completions", keyAcme, body, "X-Request-Id", "req-log-2").Body.Close()

	logs := h.logs.String()
	if strings.Contains(logs, "hunter2") {
		t.Fatal("prompt content reached the logs")
	}
	n := 0
	for _, line := range strings.Split(strings.TrimSpace(logs), "\n") {
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("not JSON: %s", line)
		}
		rid, _ := rec["request_id"].(string)
		if rid != "req-log-1" && rid != "req-log-2" {
			continue
		}
		n++
		for _, field := range []string{"ts", "level", "msg", "service", "version", "trace_id"} {
			if _, ok := rec[field]; !ok {
				t.Errorf("a request log line lacks %s: %s", field, line)
			}
		}
	}
	if n == 0 {
		t.Fatal("no log lines carried the request ids")
	}
}
