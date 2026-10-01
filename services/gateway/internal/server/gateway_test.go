package server_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/adityasatwar321/nebula/packages/auth"
	"github.com/adityasatwar321/nebula/packages/config"
	"github.com/adityasatwar321/nebula/services/gateway/internal/dispatch"
	"github.com/adityasatwar321/nebula/services/gateway/internal/openai"
	"github.com/adityasatwar321/nebula/services/gateway/internal/usage"
)

func mustURL(t *testing.T, s string) *url.URL {
	t.Helper()
	u, err := url.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

const chatBody = `{"model":"tiny","messages":[{"role":"system","content":"S"},{"role":"user","content":"hi"}],"max_tokens":16,"temperature":0}`

func TestChatCompletionNonStreaming(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	resp := h.do("POST", "/v1/chat/completions", keyAcme, chatBody, "X-Request-Id", "req-chat-1")
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d: %v", resp.StatusCode, errorOf(t, resp))
	}
	var out openai.ChatCompletion
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.ID != "chatcmpl-req-chat-1" || out.Object != "chat.completion" || out.Model != "tiny" ||
		len(out.Choices) != 1 || out.Choices[0].Message.Content != "hello" || out.Choices[0].Message.Role != "assistant" ||
		out.Choices[0].FinishReason != "stop" {
		t.Errorf("response: %+v", out)
	}
	if out.Usage.PromptTokens != 7 || out.Usage.CompletionTokens != 2 || out.Usage.TotalTokens != 9 {
		t.Errorf("usage must be the runtime's count: %+v", out.Usage)
	}
	m := out.Nebula
	if m == nil || m.RequestID != "req-chat-1" || m.Deployment != "tiny-a" || m.ModelVersion != "tiny:v1" ||
		m.Variant != "baseline" || *m.QueueWaitMS != 3 || *m.TTFTMS != 12 || m.Attempts != 1 ||
		m.Runtime != "mock" || *m.TokensPerSecond != 20 {
		t.Errorf("nebula block: %+v", m)
	}

	// What the worker received: the rendered template, the protocol headers, and
	// no identity (B3).
	got := h.worker.received()
	if len(got) != 1 {
		t.Fatalf("worker calls: %d", len(got))
	}
	w := got[0]
	if w.Body.Prompt != "<|im_start|>system\nS<|im_end|>\n<|im_start|>user\nhi<|im_end|>\n<|im_start|>assistant\n" {
		t.Errorf("prompt: %q", w.Body.Prompt)
	}
	if w.Body.MaxTokens != 16 || w.Body.Temperature != 0 || w.Body.TopP != 1 ||
		!contains(w.Body.Stop, "<|im_end|>") {
		t.Errorf("body: %+v", w.Body)
	}
	if w.Header.Get("X-Request-Id") != "req-chat-1" || w.Header.Get("X-Nebula-Priority") != "HIGH" ||
		w.Header.Get("X-Nebula-Model-Version") != "tiny:v1" || w.Header.Get("Traceparent") == "" {
		t.Errorf("headers: %v", w.Header)
	}
	dl, _ := strconv.ParseInt(w.Header.Get("X-Nebula-Deadline"), 10, 64)
	if until := time.Until(time.UnixMilli(dl)); until < 50*time.Second || until > 61*time.Second {
		t.Errorf("deadline must be ~60s out (the default timeout), is %s", until)
	}
	if w.Header.Get("Authorization") != "" {
		t.Error("the API key crossed B3 to the worker")
	}

	rec := h.usage.Wait(1, 2*time.Second)
	if len(rec) != 1 {
		t.Fatalf("usage records: %d", len(rec))
	}
	r := rec[0]
	if r.Outcome != usage.OutcomeCompleted || r.StatusCode != 200 || *r.PromptTokens != 7 || *r.CompletionTokens != 2 ||
		r.TokenSource != "runtime" || r.RequestID != "req-chat-1" || r.OrgID != acmeOrg.String() ||
		r.Endpoint != "chat.completions" || r.Streamed || r.Deployment != "tiny-a" || r.Runtime != "mock" {
		t.Errorf("usage record: %+v", r)
	}
	if resp.Header.Get("x-ratelimit-limit-requests") == "" || resp.Header.Get("x-ratelimit-remaining-tokens") == "" {
		t.Errorf("rate-limit headers missing: %v", resp.Header)
	}
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

func TestTextCompletion(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	resp := h.do("POST", "/v1/completions", keyAcme, `{"model":"tiny-text","prompt":"once","max_tokens":4}`)
	defer resp.Body.Close()
	var out openai.TextCompletion
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if resp.StatusCode != 200 || out.Object != "text_completion" || out.Choices[0].Text != "hello" {
		t.Fatalf("%d %+v", resp.StatusCode, out)
	}
	if p := h.worker.received()[0].Body.Prompt; p != "once" {
		t.Errorf("a text completion's prompt must be sent verbatim, got %q", p)
	}
	// A completion-only route refuses the chat endpoint.
	e := errorOf(t, h.do("POST", "/v1/chat/completions", keyAcme,
		`{"model":"tiny-text","messages":[{"role":"user","content":"x"}]}`))
	if e.Code != "unsupported_endpoint" {
		t.Errorf("chat on a text route: %+v", e)
	}
}

// The frame order docs/api.md §2 specifies, end to end.
func TestStreamingFrameOrder(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.worker.handle = sseTokens([]string{"Hel", "lo", "!"}, 0, "stop", 5)
	body := `{"model":"tiny","messages":[{"role":"user","content":"hi"}],"stream":true,"stream_options":{"include_usage":true}}`
	resp := h.do("POST", "/v1/chat/completions", keyAcme, body, "X-Request-Id", "req-stream-1")
	defer resp.Body.Close()
	if resp.StatusCode != 200 || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") ||
		resp.Header.Get("Cache-Control") != "no-cache" || resp.Header.Get("X-Accel-Buffering") != "no" {
		t.Fatalf("status %d headers %v", resp.StatusCode, resp.Header)
	}
	frames := readFrames(t, resp.Body)
	if len(frames) != 8 {
		t.Fatalf("want 8 frames (meta, role, 3 content, finish, usage, DONE), got %d: %+v", len(frames), frames)
	}

	// The routing context is a COMMENT, which every SSE parser ignores, never a
	// named event: current OpenAI SDKs yield named events as chunks (ADR-0029).
	if frames[0].Event != "" || frames[0].Data != "" || !strings.HasPrefix(frames[0].Comment, "nebula.meta ") {
		t.Errorf("first frame must be the nebula.meta comment: %+v", frames[0])
	}
	var meta openai.Meta
	_ = json.Unmarshal([]byte(strings.TrimPrefix(frames[0].Comment, "nebula.meta ")), &meta)
	if meta.RequestID != "req-stream-1" || meta.Deployment != "tiny-a" || meta.ModelVersion != "tiny:v1" {
		t.Errorf("meta: %+v", meta)
	}
	for h, want := range map[string]string{
		"X-Nebula-Route": "tiny", "X-Nebula-Deployment": "tiny-a",
		"X-Nebula-Model-Version": "tiny:v1", "X-Nebula-Variant": "baseline",
	} {
		if got := resp.Header.Get(h); got != want {
			t.Errorf("%s = %q, want %q", h, got, want)
		}
	}
	for _, f := range frames {
		if f.Event != "" {
			t.Errorf("no frame may be a named event: %+v", f)
		}
	}

	chunk := func(i int) openai.ChatChunk {
		var c openai.ChatChunk
		if err := json.Unmarshal([]byte(frames[i].Data), &c); err != nil {
			t.Fatalf("frame %d: %v: %s", i, err, frames[i].Data)
		}
		return c
	}
	role := chunk(1)
	if role.Choices[0].Delta.Role != "assistant" || *role.Choices[0].Delta.Content != "" || role.Choices[0].FinishReason != nil {
		t.Errorf("role chunk: %s", frames[1].Data)
	}
	var text strings.Builder
	for i := 2; i <= 4; i++ {
		c := chunk(i)
		if c.ID != "chatcmpl-req-stream-1" || c.Object != "chat.completion.chunk" || c.Choices[0].FinishReason != nil {
			t.Errorf("content chunk %d: %s", i, frames[i].Data)
		}
		text.WriteString(*c.Choices[0].Delta.Content)
	}
	if text.String() != "Hello!" {
		t.Errorf("streamed text %q", text.String())
	}
	fin := chunk(5)
	if fin.Choices[0].FinishReason == nil || *fin.Choices[0].FinishReason != "stop" || fin.Choices[0].Delta.Content != nil {
		t.Errorf("finish chunk: %s", frames[5].Data)
	}
	u := chunk(6)
	if len(u.Choices) != 0 || u.Usage == nil || u.Usage.PromptTokens != 5 || u.Usage.CompletionTokens != 3 || u.Usage.TotalTokens != 8 {
		t.Errorf("usage chunk: %s", frames[6].Data)
	}
	if frames[7].Data != "[DONE]" {
		t.Errorf("last frame: %+v", frames[7])
	}

	rec := h.usage.Wait(1, 2*time.Second)
	if len(rec) != 1 || rec[0].Outcome != usage.OutcomeCompleted || !rec[0].Streamed || *rec[0].CompletionTokens != 3 ||
		rec[0].FinishReason != "stop" {
		t.Errorf("usage: %+v", rec)
	}
}

// Without include_usage there is no usage chunk, matching OpenAI.
func TestStreamingWithoutUsageChunk(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.worker.handle = sseTokens([]string{"a"}, 0, "length", 1)
	resp := h.do("POST", "/v1/chat/completions", keyAcme, `{"model":"tiny","messages":[{"role":"user","content":"x"}],"stream":true}`)
	frames := readFrames(t, resp.Body)
	resp.Body.Close()
	for _, f := range frames {
		if strings.Contains(f.Data, `"usage"`) {
			t.Errorf("unexpected usage frame: %s", f.Data)
		}
	}
	if !strings.Contains(frames[len(frames)-2].Data, `"finish_reason":"length"`) {
		t.Errorf("finish frame: %+v", frames[len(frames)-2])
	}
}

func TestKeepAliveDuringSilence(t *testing.T) {
	t.Parallel()
	h := newHarness(t, func(c *config.Config) { c.Gateway.KeepAliveInterval = config.Duration(40 * time.Millisecond) })
	h.worker.handle = sseTokens([]string{"a", "b"}, 200*time.Millisecond, "stop", 1)
	resp := h.do("POST", "/v1/chat/completions", keyAcme, `{"model":"tiny","messages":[{"role":"user","content":"x"}],"stream":true}`)
	frames := readFrames(t, resp.Body)
	resp.Body.Close()
	var keepAlives, afterA int
	sawA := false
	for _, f := range frames {
		if f.Comment == "keep-alive" {
			keepAlives++
			if sawA {
				afterA++
			}
		}
		if strings.Contains(f.Data, `"content":"a"`) {
			sawA = true
		}
	}
	if afterA < 2 {
		t.Errorf("a 200ms silence with a 40ms keep-alive must produce keep-alives; got %d (%d total)", afterA, keepAlives)
	}
	if frames[len(frames)-1].Data != "[DONE]" {
		t.Error("stream must still complete")
	}
}

// A worker that dies mid-stream: terminal error frame, [DONE], and never a
// finish_reason — so the client can tell it from a completed response.
func TestStreamInterruptedMidway(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.worker.handle = func(w http.ResponseWriter, _ *http.Request, _ workerReq) {
		w.Header().Set("Content-Type", "text/event-stream")
		writeEvent(w, dispatch.Chunk{Content: "partial"})
		w.(http.Flusher).Flush()
		// return without a final chunk or [DONE]: the connection closes
	}
	resp := h.do("POST", "/v1/chat/completions", keyAcme, `{"model":"tiny","messages":[{"role":"user","content":"x"}],"stream":true}`)
	frames := readFrames(t, resp.Body)
	resp.Body.Close()
	last2 := frames[len(frames)-2:]
	var se openai.StreamError
	if err := json.Unmarshal([]byte(last2[0].Data), &se); err != nil || se.Error.Code != "stream_interrupted" ||
		se.Error.Type != "upstream_error" || se.Error.RequestID == "" {
		t.Errorf("error frame: %+v", last2[0])
	}
	if last2[1].Data != "[DONE]" {
		t.Errorf("an error frame must be followed by [DONE]: %+v", last2[1])
	}
	for _, f := range frames {
		if strings.Contains(f.Data, "finish_reason\":\"") {
			t.Errorf("an interrupted stream must never carry a finish_reason: %s", f.Data)
		}
	}
	rec := h.usage.Wait(1, 2*time.Second)
	if len(rec) != 1 || rec[0].Outcome != usage.OutcomeStreamInterrupted || rec[0].TokenSource != usage.TokenSourceUnavailable {
		t.Errorf("usage: %+v", rec)
	}
}

// Client disconnect mid-stream: the worker is told to stop, the gateway reads the
// final frame anyway, and the partial usage is recorded from the runtime's count.
func TestClientDisconnectCancelsWorkerAndRecordsPartialUsage(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.worker.handle = func(w http.ResponseWriter, r *http.Request, req workerReq) {
		id := req.Header.Get("X-Request-Id")
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		emitted := 0
		for {
			select {
			case <-h.worker.cancelCh(id):
				writeEvent(w, dispatch.Chunk{Stop: true, FinishReason: "cancel",
					Usage: &dispatch.Usage{PromptTokens: 4, CompletionTokens: emitted}})
				_, _ = io.WriteString(w, "data: [DONE]\n\n")
				fl.Flush()
				return
			case <-r.Context().Done():
				return
			case <-time.After(20 * time.Millisecond):
				writeEvent(w, dispatch.Chunk{Content: "t", Index: emitted})
				fl.Flush()
				emitted++
			}
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, "POST", h.gw.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"tiny","messages":[{"role":"user","content":"x"}],"stream":true}`))
	req.Header.Set("Authorization", "Bearer "+keyAcme)
	req.Header.Set("X-Request-Id", "req-disconnect")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4096)
	seen := 0
	for seen < 3 {
		n, err := resp.Body.Read(buf)
		seen += strings.Count(string(buf[:n]), `"content":"t"`)
		if err != nil {
			t.Fatal(err)
		}
	}
	cancel()
	resp.Body.Close()

	rec := h.usage.Wait(1, 5*time.Second)
	if len(rec) != 1 {
		t.Fatalf("no usage record after disconnect")
	}
	r := rec[0]
	if r.Outcome != usage.OutcomeClientCancelled || r.StatusCode != 499 {
		t.Errorf("outcome %q status %d", r.Outcome, r.StatusCode)
	}
	if r.TokenSource != "runtime" || r.CompletionTokens == nil || *r.CompletionTokens < 3 || r.FinishReason != "cancel" {
		t.Errorf("partial usage must come from the runtime's final frame: %+v", r)
	}
	if c := h.worker.cancelsReceived(); len(c) != 1 || c[0] != "req-disconnect" {
		t.Errorf("the worker was not told to cancel: %v", c)
	}
}

// Non-streamed disconnect: same obligations.
func TestClientDisconnectNonStreaming(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.worker.handle = func(w http.ResponseWriter, r *http.Request, req workerReq) {
		select {
		case <-h.worker.cancelCh(req.Header.Get("X-Request-Id")):
			replyJSON("par", "cancel", 4, 1)(w, r, req)
		case <-time.After(5 * time.Second):
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "POST", h.gw.URL+"/v1/chat/completions", strings.NewReader(chatBody))
	req.Header.Set("Authorization", "Bearer "+keyAcme)
	if _, err := http.DefaultClient.Do(req); err == nil {
		t.Fatal("expected the client to time out")
	}
	rec := h.usage.Wait(1, 5*time.Second)
	if len(rec) != 1 || rec[0].Outcome != usage.OutcomeClientCancelled || rec[0].CompletionTokens == nil || *rec[0].CompletionTokens != 1 {
		t.Fatalf("usage: %+v", rec)
	}
}

func TestDeadlineFinishIsAGatewayTimeout(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.worker.handle = replyJSON("par", "deadline", 4, 3)
	resp := h.do("POST", "/v1/chat/completions", keyAcme,
		`{"model":"tiny","messages":[{"role":"user","content":"x"}],"nebula":{"timeout_ms":1500}}`)
	e := errorOf(t, resp)
	if resp.StatusCode != 504 || e.Type != "timeout_error" || e.Code != "deadline_exceeded" {
		t.Errorf("%d %+v", resp.StatusCode, e)
	}
	dl, _ := strconv.ParseInt(h.worker.received()[0].Header.Get("X-Nebula-Deadline"), 10, 64)
	if until := time.Until(time.UnixMilli(dl)); until > 1500*time.Millisecond {
		t.Errorf("nebula.timeout_ms must set the deadline; it is %s out", until)
	}
	rec := h.usage.Wait(1, 2*time.Second)
	if rec[0].Outcome != usage.OutcomeDeadlineExceeded || *rec[0].CompletionTokens != 3 {
		t.Errorf("a timed-out request still records the tokens it consumed: %+v", rec[0])
	}
}

// Each worker failure before the first byte maps onto the public error the
// contract names — and never passes the worker's own message through.
func TestWorkerErrorMapping(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		status     int
		headers    map[string]string
		wantStatus int
		wantCode   string
		wantReason string
	}{
		{"saturated", 429, map[string]string{"Retry-After": "7", "X-Nebula-Reason": "worker_saturated"}, 429, "worker_saturated", "queue_full"},
		{"wrong version", 409, nil, 502, "model_version_mismatch", ""},
		{"loading", 503, nil, 503, "model_loading", "model_loading"},
		{"deadline", 504, nil, 504, "deadline_exceeded", ""},
		{"engine crash", 500, nil, 502, "upstream_error", ""},
		{"protocol bug", 422, nil, 502, "upstream_error", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.worker.handle = func(w http.ResponseWriter, _ *http.Request, _ workerReq) {
				for k, v := range tc.headers {
					w.Header().Set(k, v)
				}
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, `{"error":{"message":"SECRET internal worker detail pod-7f9","type":"x","code":"y"}}`)
			}
			for _, stream := range []bool{false, true} {
				body := `{"model":"tiny","messages":[{"role":"user","content":"x"}],"stream":` + strconv.FormatBool(stream) + `}`
				resp := h.do("POST", "/v1/chat/completions", keyAcme, body)
				e := errorOf(t, resp)
				if resp.StatusCode != tc.wantStatus || e.Code != tc.wantCode || resp.Header.Get("X-Nebula-Reason") != tc.wantReason {
					t.Errorf("stream=%v: got %d %q reason %q", stream, resp.StatusCode, e.Code, resp.Header.Get("X-Nebula-Reason"))
				}
				if strings.Contains(e.Message, "SECRET") || strings.Contains(e.Message, "pod-") {
					t.Errorf("worker detail crossed B1 outward: %q", e.Message)
				}
				if tc.status == 429 && resp.Header.Get("Retry-After") == "" {
					t.Error("a saturated worker's 429 must carry Retry-After")
				}
			}
		})
	}
}

func TestWorkerUnreachable(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.worker.srv.Close()
	resp := h.do("POST", "/v1/chat/completions", keyAcme, chatBody)
	e := errorOf(t, resp)
	if resp.StatusCode != 503 || e.Code != "no_healthy_endpoint" || resp.Header.Get("X-Nebula-Reason") != "no_healthy_endpoint" {
		t.Errorf("%d %+v", resp.StatusCode, e)
	}
}

func TestAuthenticationAndAuthorization(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	cases := []struct {
		name   string
		key    string
		path   string
		body   string
		status int
		code   string
	}{
		{"no key", "", "/v1/chat/completions", chatBody, 401, "missing_credential"},
		{"bad key", "nbk_WRONG00aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "/v1/chat/completions", chatBody, 401, "invalid_credential"},
		{"no invoke scope", keyAdmin, "/v1/chat/completions", chatBody, 403, "insufficient_scope"},
		{"pin without scope", keyAcme, "/v1/chat/completions",
			`{"model":"tiny","messages":[{"role":"user","content":"x"}],"nebula":{"deployment_id":"tiny-b"}}`, 403, "insufficient_scope"},
		// Another org's model is indistinguishable from one that does not exist.
		{"other org's model", keyAcme, "/v1/chat/completions",
			`{"model":"secret","messages":[{"role":"user","content":"x"}]}`, 404, "model_not_found"},
		{"no such model", keyGlobex, "/v1/chat/completions",
			`{"model":"tiny","messages":[{"role":"user","content":"x"}]}`, 404, "model_not_found"},
		{"unsupported param", keyAcme, "/v1/chat/completions",
			`{"model":"tiny","messages":[{"role":"user","content":"x"}],"n":3}`, 400, "unsupported_parameter"},
		{"over the window", keyAcme, "/v1/chat/completions",
			`{"model":"tiny","messages":[{"role":"user","content":"x"}],"max_tokens":9999}`, 400, "context_length_exceeded"},
	}
	for _, tc := range cases {
		resp := h.do("POST", tc.path, tc.key, tc.body)
		e := errorOf(t, resp)
		if resp.StatusCode != tc.status || e.Code != tc.code || e.RequestID == "" {
			t.Errorf("%s: got %d %q (request_id %q), want %d %q", tc.name, resp.StatusCode, e.Code, e.RequestID, tc.status, tc.code)
		}
	}
	if n := len(h.worker.received()); n != 0 {
		t.Errorf("refused requests reached the worker %d times", n)
	}
}

func TestPinWithScope(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	for _, pin := range []string{"tiny-b", "0192f3c1-0000-7000-8000-00000000000a"} {
		resp := h.do("POST", "/v1/chat/completions", keyAcmePin,
			`{"model":"tiny","messages":[{"role":"user","content":"x"}],"nebula":{"deployment_id":"`+pin+`"}}`)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("pin %s: %d", pin, resp.StatusCode)
		}
	}
	got := h.worker.received()
	if got[0].Header.Get("X-Nebula-Model-Version") != "tiny:v2" || got[1].Header.Get("X-Nebula-Model-Version") != "tiny:v1" {
		t.Errorf("pinning must select the deployment and assert its version: %s, %s",
			got[0].Header.Get("X-Nebula-Model-Version"), got[1].Header.Get("X-Nebula-Model-Version"))
	}
}

func TestRateLimitRefusal(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	first := h.do("POST", "/v1/chat/completions", keyTight, chatBody)
	first.Body.Close()
	if first.StatusCode != 200 || first.Header.Get("x-ratelimit-limit-requests") != "1" ||
		first.Header.Get("x-ratelimit-remaining-requests") != "0" {
		t.Fatalf("first: %d %v", first.StatusCode, first.Header)
	}
	resp := h.do("POST", "/v1/chat/completions", keyTight, chatBody)
	e := errorOf(t, resp)
	if resp.StatusCode != 429 || e.Type != "rate_limit_error" || e.Code != "rate_limited_rpm" ||
		resp.Header.Get("X-Nebula-Reason") != "rate_limited_rpm" || resp.Header.Get("Retry-After") != "60" {
		t.Errorf("%d %+v headers %v", resp.StatusCode, e, resp.Header)
	}
	if n := len(h.worker.received()); n != 1 {
		t.Errorf("a refused request reached the worker; calls=%d", n)
	}
}

func TestModelsList(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	resp := h.do("GET", "/v1/models", keyAcme, "")
	defer resp.Body.Close()
	var list openai.ModelList
	_ = json.NewDecoder(resp.Body).Decode(&list)
	if resp.StatusCode != 200 || list.Object != "list" || len(list.Data) != 2 {
		t.Fatalf("%d %+v", resp.StatusCode, list)
	}
	m := list.Data[0]
	if m.ID != "tiny" || m.Object != "model" || m.OwnedBy != "acme" || m.Created != 1_800_000_000 ||
		m.Nebula.ContextWindow != 512 || len(m.Nebula.Targets) != 2 || m.Nebula.Source != "static" || !m.Nebula.Streaming {
		t.Errorf("model: %+v %+v", m, m.Nebula)
	}
	for _, d := range list.Data {
		if d.ID == "secret" {
			t.Error("another org's route was listed")
		}
	}

	one := h.do("GET", "/v1/models/tiny", keyAcme, "")
	one.Body.Close()
	if one.StatusCode != 200 {
		t.Errorf("retrieve by route name: %d", one.StatusCode)
	}
	if e := errorOf(t, h.do("GET", "/v1/models/secret", keyAcme, "")); e.Code != "model_not_found" {
		t.Errorf("retrieve another org's route: %+v", e)
	}
	if n := len(h.cp.received()); n != 0 {
		t.Errorf("route reads must not touch the control plane; calls=%d", n)
	}
}

// B2: the admin proxy removes the caller's key and any client-supplied context,
// and attaches a signed identity the control plane can verify.
func TestAdminProxySignsIdentity(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	forged, _ := h.signer.Sign(auth.Identity{OrgID: acmeOrg, ActorType: "api_key", Scopes: []auth.Scope{auth.ScopeAdmin}},
		"", "some-other-audience", "attacker")
	resp := h.do("GET", "/v1/deployments?limit=5", keyAdmin, "",
		auth.HeaderAuthContext, forged, "X-Forwarded-For", "6.6.6.6")
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(string(body), `"data"`) {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	calls := h.cp.received()
	if len(calls) != 1 {
		t.Fatalf("control plane calls: %d", len(calls))
	}
	c := calls[0]
	if c.VerifyErr != nil {
		t.Fatalf("the proxied context must verify: %v", c.VerifyErr)
	}
	if c.Identity.OrgID != acmeOrg || !c.Identity.Has(auth.ScopeAdmin) || c.Path != "/v1/deployments" {
		t.Errorf("identity %+v path %s", c.Identity, c.Path)
	}
	if c.Header.Get("Authorization") != "" {
		t.Error("the API key was forwarded to the control plane")
	}
	if xff := c.Header.Get("X-Forwarded-For"); strings.Contains(xff, "6.6.6.6") {
		t.Errorf("a client-supplied X-Forwarded-For reached the control plane: %q", xff)
	}
	if got := resp.Header.Values("X-Request-Id"); len(got) != 1 || got[0] == "upstream-copy" {
		t.Errorf("X-Request-Id must be the gateway's, once: %v", got)
	}
}

func TestRevocationThroughTheProxyInvalidates(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	resp := h.do("DELETE", "/v1/api-keys/0192f3c1-0000-7000-8000-000000000123", keyAdmin, "")
	resp.Body.Close()
	if resp.StatusCode != 204 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if resp.Header.Get("X-Nebula-Revoked-Key-Prefix") != "" {
		t.Error("the revocation signal must not reach the client")
	}
	if len(h.invalidated) != 1 || h.invalidated[0] != "nbk_REVOKED" {
		t.Errorf("invalidated: %v", h.invalidated)
	}
}

func TestInternalPathsAreNeverProxied(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	for _, p := range []string{"/internal/v1/credentials/nbk_AAAAAAA", "/internal/v1/anything"} {
		resp := h.do("GET", p, keyAdmin, "")
		resp.Body.Close()
		if resp.StatusCode != 404 {
			t.Errorf("%s: %d", p, resp.StatusCode)
		}
	}
	if n := len(h.cp.received()); n != 0 {
		t.Errorf("an internal path reached the control plane %d times", n)
	}
}

func TestRegistryReadByUUIDIsProxied(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	resp := h.do("GET", "/v1/models/0192f3c1-0000-7000-8000-0000000000ff", keyAdmin, "")
	resp.Body.Close()
	calls := h.cp.received()
	if len(calls) != 1 || calls[0].Path != "/v1/models/0192f3c1-0000-7000-8000-0000000000ff" {
		t.Errorf("a uuid model path must reach the registry: %+v", calls)
	}
}

// Found on kind: the registry list must reach the control plane, not be read as a
// route called "registry".
func TestRegistryListIsProxied(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	resp := h.do("GET", "/v1/models/registry?limit=5", keyAdmin, "")
	resp.Body.Close()
	calls := h.cp.received()
	if resp.StatusCode != 200 || len(calls) != 1 || calls[0].Path != "/v1/models/registry" {
		t.Errorf("status %d, control plane calls %+v", resp.StatusCode, calls)
	}
}

func TestMethodNotAllowed(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	resp := h.do("GET", "/v1/chat/completions", keyAcme, "")
	resp.Body.Close()
	if resp.StatusCode != 405 || resp.Header.Get("Allow") != "POST" {
		t.Errorf("%d allow=%q", resp.StatusCode, resp.Header.Get("Allow"))
	}
}
