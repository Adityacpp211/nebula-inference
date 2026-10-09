package server_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/adityasatwar321/nebula/packages/auth"
	"github.com/adityasatwar321/nebula/packages/config"
	"github.com/adityasatwar321/nebula/packages/db/models"
	"github.com/adityasatwar321/nebula/packages/httpx"
	"github.com/adityasatwar321/nebula/packages/routing"
	"github.com/adityasatwar321/nebula/packages/telemetry"
	"github.com/adityasatwar321/nebula/packages/testsupport/netx"
	"github.com/adityasatwar321/nebula/packages/version"
	"github.com/adityasatwar321/nebula/services/gateway/internal/adminproxy"
	"github.com/adityasatwar321/nebula/services/gateway/internal/controlplane"
	"github.com/adityasatwar321/nebula/services/gateway/internal/credentials"
	"github.com/adityasatwar321/nebula/services/gateway/internal/dispatch"
	"github.com/adityasatwar321/nebula/services/gateway/internal/ratelimit"
	"github.com/adityasatwar321/nebula/services/gateway/internal/router"
	"github.com/adityasatwar321/nebula/services/gateway/internal/routes"
	"github.com/adityasatwar321/nebula/services/gateway/internal/server"
	"github.com/adityasatwar321/nebula/services/gateway/internal/usage"
)

const internalSecret = "internal-secret-0123456789abcdef0123456789"

// ---------------------------------------------------------------------------
// fake worker
// ---------------------------------------------------------------------------

// workerReq is what the fake worker received.
type workerReq struct {
	Path   string
	Header http.Header
	Body   dispatch.Body
}

type fakeWorker struct {
	t   *testing.T
	srv *httptest.Server

	mu       sync.Mutex
	requests []workerReq
	cancels  []string
	// cancelled is closed per request id when a cancel arrives.
	cancelled map[string]chan struct{}

	// handle serves /internal/v1/generate[/stream]. Set per test.
	handle func(w http.ResponseWriter, r *http.Request, req workerReq)
}

func newWorker(t *testing.T) *fakeWorker {
	fw := &fakeWorker{t: t, cancelled: map[string]chan struct{}{}}
	fw.srv = netx.NewServer(t, http.HandlerFunc(fw.serve))
	return fw
}

func (fw *fakeWorker) cancelCh(id string) chan struct{} {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	ch, ok := fw.cancelled[id]
	if !ok {
		ch = make(chan struct{})
		fw.cancelled[id] = ch
	}
	return ch
}

func (fw *fakeWorker) serve(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/internal/v1/cancel" {
		var b struct {
			RequestID string `json:"request_id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&b)
		fw.mu.Lock()
		fw.cancels = append(fw.cancels, b.RequestID)
		fw.mu.Unlock()
		ch := fw.cancelCh(b.RequestID)
		select {
		case <-ch:
		default:
			close(ch)
		}
		_, _ = w.Write([]byte(`{"cancelled":true}`))
		return
	}
	var body dispatch.Body
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields() // the real worker refuses unknown fields too
	if err := dec.Decode(&body); err != nil {
		w.WriteHeader(422)
		_, _ = fmt.Fprintf(w, `{"detail":%q}`, err.Error())
		return
	}
	req := workerReq{Path: r.URL.Path, Header: r.Header.Clone(), Body: body}
	fw.mu.Lock()
	fw.requests = append(fw.requests, req)
	h := fw.handle
	fw.mu.Unlock()
	h(w, r, req)
}

func (fw *fakeWorker) received() []workerReq {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	return append([]workerReq(nil), fw.requests...)
}

func (fw *fakeWorker) cancelsReceived() []string {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	return append([]string(nil), fw.cancels...)
}

func i64(n int64) *int64 { return &n }

// replyJSON answers a non-streamed generate.
func replyJSON(text, finish string, prompt, completion int) func(http.ResponseWriter, *http.Request, workerReq) {
	return func(w http.ResponseWriter, _ *http.Request, _ workerReq) {
		_ = json.NewEncoder(w).Encode(dispatch.Result{
			Text: text, FinishReason: finish,
			Usage:   &dispatch.Usage{PromptTokens: prompt, CompletionTokens: completion},
			Timing:  &dispatch.Timing{QueueMS: i64(3), PrefillMS: i64(10), DecodeMS: i64(100), TTFTMS: i64(12), TotalMS: i64(113)},
			Runtime: &dispatch.Runtime{Name: "mock", Version: "0.3.0", ModelVersion: "tiny:v1"},
		})
	}
}

// sseTokens streams tokens with an optional gap, then a final chunk.
func sseTokens(tokens []string, gap time.Duration, finish string, prompt int) func(http.ResponseWriter, *http.Request, workerReq) {
	return func(w http.ResponseWriter, _ *http.Request, _ workerReq) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		for i, tok := range tokens {
			if i > 0 && gap > 0 {
				time.Sleep(gap)
			}
			writeEvent(w, dispatch.Chunk{Content: tok, Index: i})
			fl.Flush()
		}
		writeEvent(w, dispatch.Chunk{Index: len(tokens), Stop: true, FinishReason: finish,
			Usage:   &dispatch.Usage{PromptTokens: prompt, CompletionTokens: len(tokens)},
			Timing:  &dispatch.Timing{QueueMS: i64(1), PrefillMS: i64(2), DecodeMS: i64(50), TTFTMS: i64(4), TotalMS: i64(53)},
			Runtime: &dispatch.Runtime{Name: "mock", Version: "0.3.0", ModelVersion: "tiny:v1"}})
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		fl.Flush()
	}
}

func writeEvent(w io.Writer, v any) {
	b, _ := json.Marshal(v)
	_, _ = fmt.Fprintf(w, "data: %s\n\n", b)
}

// ---------------------------------------------------------------------------
// fake authentication
// ---------------------------------------------------------------------------

type fakeAuth map[string]credentials.Principal

func (f fakeAuth) Authenticate(_ context.Context, presented string) (credentials.Principal, error) {
	p, ok := f[presented]
	if !ok {
		return credentials.Principal{}, &httpx.APIError{Status: 401, Type: httpx.TypeAuthentication,
			Code: "invalid_credential", Message: "invalid API key"}
	}
	return p, nil
}

// Keys used by the tests. They only have to look like keys; fakeAuth decides.
const (
	keyAcme    = "nbk_ACME000aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	keyAcmePin = "nbk_ACMEPINaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	keyAdmin   = "nbk_ADMIN00aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	keyGlobex  = "nbk_GLOBEX0aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	keyTight   = "nbk_TIGHT00aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
)

var acmeOrg = uuid.MustParse("0192f3b0-1111-7000-8000-000000000001")

func principal(org uuid.UUID, slug string, scopes ...auth.Scope) credentials.Principal {
	return credentials.Principal{Identity: auth.Identity{
		OrgID: org, OrgSlug: slug, ActorType: models.ActorAPIKey, ActorID: uuid.New(),
		ActorLabel: "nbk_TEST000", Scopes: scopes, Priority: models.PriorityHigh,
	}}
}

// ---------------------------------------------------------------------------
// the gateway under test
// ---------------------------------------------------------------------------

type harness struct {
	t           *testing.T
	gw          *httptest.Server
	worker      *fakeWorker
	usage       *usage.MemorySink
	cfg         *config.Config
	cp          *fakeControlPlane
	signer      *auth.ContextSigner
	auth        fakeAuth
	invalidated []string
	router      *router.Router
	metrics     *telemetry.Metrics
	logs        *syncBuffer
}

// syncBuffer collects log output from concurrent handlers.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type option func(*config.Config)

func newHarness(t *testing.T, opts ...option) *harness {
	t.Helper()
	cfg, err := config.Loader{Service: config.ServiceGateway, Getenv: func(k string) string {
		return map[string]string{
			"NEBULA_AUTH_KEY_PEPPER":      "0123456789abcdef0123456789abcdef-pepper",
			"NEBULA_INTERNAL_AUTH_SECRET": internalSecret,
		}[k]
	}}.Load()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Limits.FallbackFraction = 1
	cfg.Gateway.KeepAliveInterval = config.Duration(15 * time.Second)
	cfg.Gateway.CancelDrainTimeout = config.Duration(2 * time.Second)
	for _, o := range opts {
		o(cfg)
	}

	h := &harness{t: t, cfg: cfg, usage: usage.NewMemorySink(), worker: newWorker(t)}
	h.worker.handle = replyJSON("hello", "stop", 7, 2)

	tbl, err := routes.Build([]*routes.Route{
		{Model: "tiny", Org: "acme", ContextWindow: 512, ChatTemplate: routes.TemplateChatML,
			Capabilities: routes.Capabilities{Streaming: true},
			Targets: []*routes.Target{
				{Deployment: "tiny-a", DeploymentID: "0192f3c1-0000-7000-8000-00000000000a",
					ModelVersion: "tiny:v1", Weight: 100, Label: "baseline", Endpoints: []string{h.worker.srv.URL}},
				{Deployment: "tiny-b", ModelVersion: "tiny:v2", Weight: 0, Label: "canary", Endpoints: []string{h.worker.srv.URL}},
			}},
		{Model: "tiny-text", Org: "acme", Task: routes.TaskCompletion, ContextWindow: 512,
			Capabilities: routes.Capabilities{Streaming: true},
			Targets:      []*routes.Target{{Deployment: "tt", ModelVersion: "tt:v1", Weight: 100, Endpoints: []string{h.worker.srv.URL}}}},
		{Model: "secret", Org: "globex", ContextWindow: 512,
			Targets: []*routes.Target{{Deployment: "g", ModelVersion: "g:v1", Weight: 100, Endpoints: []string{h.worker.srv.URL}}}},
	}, time.Unix(1_800_000_000, 0))
	if err != nil {
		t.Fatal(err)
	}

	tight := principal(acmeOrg, "acme", auth.ScopeInferenceInvoke)
	one := int32(1)
	tight.Policy = &controlplane.RatePolicy{Name: "tight", RequestsPerMinute: &one}
	h.auth = fakeAuth{
		keyAcme:    principal(acmeOrg, "acme", auth.ScopeInferenceInvoke),
		keyAcmePin: principal(acmeOrg, "acme", auth.ScopeInferenceInvoke, auth.ScopeInferencePin),
		keyAdmin:   principal(acmeOrg, "acme", auth.ScopeAdmin, auth.ScopeDeploymentsRead),
		keyGlobex:  principal(uuid.New(), "globex", auth.ScopeInferenceInvoke),
		keyTight:   tight,
	}

	h.router = router.New(router.Options{Breaker: routing.BreakerConfig{Threshold: 1000, Cooldown: time.Millisecond}})
	h.router.SetTable(tbl)
	h.metrics = telemetry.NewMetrics()

	h.signer, _ = auth.NewContextSigner(internalSecret, 10*time.Second, nil)
	h.cp = newFakeControlPlane(t, h.signer)
	h.logs = &syncBuffer{}
	logger, err := telemetry.NewLogger(h.logs, config.LogConfig{Level: "debug", Format: "json"},
		version.Get("nebula-gateway"), "test")
	if err != nil {
		t.Fatal(err)
	}
	proxy := adminproxy.New(adminproxy.Options{
		Target: mustURL(t, h.cp.srv.URL), Signer: h.signer, Issuer: "nebula-gateway", Logger: logger,
		Invalidate: func(_ context.Context, prefix string) { h.invalidated = append(h.invalidated, prefix) },
	})

	probes := telemetry.NewProbes(telemetry.ProbesOptions{Info: version.Get("nebula-gateway"), Logger: logger})
	probes.MarkReady()
	handler := server.New(server.Deps{
		Config: cfg, Logger: logger, Probes: probes, Auth: h.auth,
		Limiter: ratelimit.New(ratelimit.Options{FallbackFraction: 1}),
		Router:  h.router, Metrics: h.metrics, Workers: dispatch.New(dispatch.Options{}), Proxy: proxy, Usage: h.usage,
	})
	h.gw = netx.NewServer(t, handler)
	return h
}

func (h *harness) do(method, path, key, body string, hdr ...string) *http.Response {
	h.t.Helper()
	req, err := http.NewRequest(method, h.gw.URL+path, strings.NewReader(body))
	if err != nil {
		h.t.Fatal(err)
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	req.Header.Set("Content-Type", "application/json")
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	return resp
}

// errorOf decodes the standard error envelope.
func errorOf(t *testing.T, resp *http.Response) httpx.APIError {
	t.Helper()
	defer resp.Body.Close()
	var env struct {
		Error httpx.APIError `json:"error"`
	}
	b, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(b, &env); err != nil {
		t.Fatalf("not an error envelope (%d): %s", resp.StatusCode, b)
	}
	return env.Error
}

// frame is one parsed SSE frame from the gateway.
type frame struct {
	Event   string
	Data    string
	Comment string
}

func readFrames(t *testing.T, r io.Reader) []frame {
	t.Helper()
	var out []frame
	sc := bufio.NewScanner(r)
	cur := frame{}
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			if cur != (frame{}) {
				out = append(out, cur)
			}
			cur = frame{}
		case strings.HasPrefix(line, ":"):
			cur.Comment = strings.TrimSpace(strings.TrimPrefix(line, ":"))
		case strings.HasPrefix(line, "event: "):
			cur.Event = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			cur.Data = strings.TrimPrefix(line, "data: ")
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// fake control plane
// ---------------------------------------------------------------------------

type proxied struct {
	Method, Path string
	Header       http.Header
	Identity     auth.Identity
	VerifyErr    error
}

type fakeControlPlane struct {
	srv    *httptest.Server
	mu     sync.Mutex
	calls  []proxied
	signer *auth.ContextSigner
}

func newFakeControlPlane(t *testing.T, signer *auth.ContextSigner) *fakeControlPlane {
	cp := &fakeControlPlane{signer: signer}
	cp.srv = netx.NewServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v, err := signer.Verify(r.Header.Get(auth.HeaderAuthContext), auth.AudienceControlPlane)
		cp.mu.Lock()
		cp.calls = append(cp.calls, proxied{Method: r.Method, Path: r.URL.Path, Header: r.Header.Clone(),
			Identity: v.Identity, VerifyErr: err})
		cp.mu.Unlock()
		w.Header().Set("X-Request-Id", "upstream-copy")
		if r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/v1/api-keys/") {
			w.Header().Set(adminproxy.HeaderRevokedKeyPrefix, "nbk_REVOKED")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		_, _ = w.Write([]byte(`{"data":[],"has_more":false}`))
	}))
	return cp
}

func (cp *fakeControlPlane) received() []proxied {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	return append([]proxied(nil), cp.calls...)
}
