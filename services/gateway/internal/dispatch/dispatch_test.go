package dispatch_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/adityasatwar321/nebula/packages/testsupport/netx"
	"github.com/adityasatwar321/nebula/services/gateway/internal/dispatch"
)

func call(endpoint string) dispatch.Call {
	return dispatch.Call{
		Endpoint: endpoint, RequestID: "req-1",
		Traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		Deadline:    time.UnixMilli(1_900_000_000_123), Priority: "HIGH", ModelVersion: "m:v1",
		Body: dispatch.Body{Prompt: "hi", MaxTokens: 5, Temperature: 0.5, TopP: 1, Stop: []string{"x"}},
	}
}

// The five required headers of docs/api.md §6 are always sent, and nothing that
// identifies the caller is (B3).
func TestRequestCarriesTheProtocolHeaders(t *testing.T) {
	t.Parallel()
	var got http.Header
	var body map[string]any
	srv := netx.NewServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		_ = json.NewDecoder(r.Body).Decode(&body)
		_, _ = w.Write([]byte(`{"text":"ok","finish_reason":"stop","usage":{"prompt_tokens":1,"completion_tokens":1}}`))
	}))
	res, err := dispatch.New(dispatch.Options{}).Generate(context.Background(), call(srv.URL))
	if err != nil {
		t.Fatal(err)
	}
	if res.Text != "ok" || res.Usage.CompletionTokens != 1 {
		t.Errorf("result: %+v", res)
	}
	for h, want := range map[string]string{
		"X-Request-Id": "req-1", "Traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"X-Nebula-Deadline": "1900000000123", "X-Nebula-Priority": "HIGH",
		"X-Nebula-Model-Version": "m:v1", "X-Nebula-Worker-Protocol": "1",
	} {
		if got.Get(h) != want {
			t.Errorf("%s = %q, want %q", h, got.Get(h), want)
		}
	}
	if got.Get("Authorization") != "" || got.Get("X-Nebula-Auth-Context") != "" {
		t.Error("a credential crossed B3")
	}
	if body["prompt"] != "hi" || body["max_tokens"].(float64) != 5 {
		t.Errorf("body: %v", body)
	}
}

func TestWorkerErrorEnvelope(t *testing.T) {
	t.Parallel()
	srv := netx.NewServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "3")
		w.Header().Set("X-Nebula-Reason", "worker_saturated")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"message":"full","type":"rate_limit_error","code":"worker_saturated"}}`))
	}))
	_, err := dispatch.New(dispatch.Options{}).Generate(context.Background(), call(srv.URL))
	var we *dispatch.Error
	if !errors.As(err, &we) {
		t.Fatalf("want *dispatch.Error, got %v", err)
	}
	if we.Status != 429 || we.Code != "worker_saturated" || we.Reason != "worker_saturated" || we.RetryAfter != 3*time.Second {
		t.Errorf("error: %+v", we)
	}
}

func TestTransportError(t *testing.T) {
	t.Parallel()
	ln := netx.Listen(t)
	addr := ln.Addr().String()
	_ = ln.Close() // nothing listens here now
	_, err := dispatch.New(dispatch.Options{ConnectTimeout: time.Second}).Generate(context.Background(), call("http://"+addr))
	var we *dispatch.Error
	if !errors.As(err, &we) || we.Transport == nil {
		t.Fatalf("want a transport error, got %v", err)
	}
}

func sse(events ...string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		for _, e := range events {
			_, _ = io.WriteString(w, e)
			w.(http.Flusher).Flush()
		}
	}
}

func TestStreamParsing(t *testing.T) {
	t.Parallel()
	srv := netx.NewServer(t, sse(
		": comment\n\n",
		`data: {"content":"He","index":0,"stop":false}`+"\n\n",
		// Split across writes, with CRLF line endings: both legal SSE.
		"data: {\"content\":\"llo\",\"index\":1,", "\"stop\":false}\r\n\r\n",
		`data: {"content":"","index":2,"stop":true,"finish_reason":"stop","usage":{"prompt_tokens":3,"completion_tokens":2},"timing":{"ttft_ms":5,"queue_ms":1,"prefill_ms":2,"decode_ms":9,"total_ms":12},"runtime":{"name":"mock","version":"1","model_version":"m:v1","slot":0}}`+"\n\n",
		"data: [DONE]\n\n",
	))
	st, err := dispatch.New(dispatch.Options{}).Stream(context.Background(), call(srv.URL))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var text strings.Builder
	var final dispatch.Chunk
	for {
		ch, err := st.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		text.WriteString(ch.Content)
		if ch.Stop {
			final = ch
		}
	}
	if text.String() != "Hello" || final.FinishReason != "stop" || final.Usage.PromptTokens != 3 ||
		*final.Timing.TTFTMS != 5 || final.Runtime.Name != "mock" {
		t.Errorf("text=%q final=%+v", text.String(), final)
	}
}

func TestStreamTruncatedAndErrorEvents(t *testing.T) {
	t.Parallel()
	cut := netx.NewServer(t, sse(`data: {"content":"a","index":0,"stop":false}`+"\n\n"))
	st, err := dispatch.New(dispatch.Options{}).Stream(context.Background(), call(cut.URL))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Next(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Next(); !errors.Is(err, dispatch.ErrStreamTruncated) {
		t.Errorf("a stream that ends without [DONE] must be truncated, got %v", err)
	}
	_ = st.Close()

	failing := netx.NewServer(t, sse(`data: {"error":{"message":"boom","type":"upstream_error","code":"engine_died"}}`+"\n\n", "data: [DONE]\n\n"))
	st, _ = dispatch.New(dispatch.Options{}).Stream(context.Background(), call(failing.URL))
	_, err = st.Next()
	var we *dispatch.Error
	if !errors.As(err, &we) || !we.MidStream || we.Code != "engine_died" {
		t.Errorf("mid-stream error event: %v", err)
	}
	_ = st.Close()
}

func TestStreamRefusedBeforeStart(t *testing.T) {
	t.Parallel()
	srv := netx.NewServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":{"message":"wrong version","type":"invalid_request_error","code":"model_version_mismatch"}}`))
	}))
	_, err := dispatch.New(dispatch.Options{}).Stream(context.Background(), call(srv.URL))
	var we *dispatch.Error
	if !errors.As(err, &we) || we.Status != 409 || we.MidStream {
		t.Errorf("want a pre-stream 409, got %v", err)
	}
}
