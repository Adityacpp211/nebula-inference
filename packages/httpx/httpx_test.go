package httpx_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/adityasatwar321/nebula/packages/config"
	"github.com/adityasatwar321/nebula/packages/httpx"
	"github.com/adityasatwar321/nebula/packages/telemetry"
	"github.com/adityasatwar321/nebula/packages/testsupport/netx"
	"github.com/adityasatwar321/nebula/packages/version"
)

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
}

func TestRequestIDMintedAndEchoed(t *testing.T) {
	t.Parallel()

	var seen string
	h := httpx.RequestID()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = telemetry.RequestID(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", http.NoBody))

	if seen == "" {
		t.Fatal("no request ID placed on the context")
	}
	if got := rec.Header().Get(httpx.HeaderRequestID); got != seen {
		t.Errorf("echoed %q, context has %q; they must match", got, seen)
	}
}

// A client-supplied ID reaches log lines and response headers, so it must be
// validated. Anything that could break a header or a JSON log line is replaced.
func TestRequestIDValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		supplied string
		adopted  bool
	}{
		{"uuid adopted", "0192f3c1-7b5a-7c3e-9a11-0e5d2b8f4a21", true},
		{"simple token adopted", "ci-run_42.1", true},
		{"empty rejected", "", false},
		{"header injection rejected", "abc\r\nX-Evil: 1", false},
		{"newline rejected", "abc\ndef", false},
		{"space rejected", "abc def", false},
		{"quote rejected", `abc"def`, false},
		{"too long rejected", strings.Repeat("a", 65), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var seen string
			h := httpx.RequestID()(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				seen = telemetry.RequestID(r.Context())
			}))
			req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
			if tt.supplied != "" {
				// Set directly on the map: http.Header.Set would reject some of
				// these, and we are testing our own validation, not net/http's.
				req.Header[http.CanonicalHeaderKey(httpx.HeaderRequestID)] = []string{tt.supplied}
			}
			httptest.NewRecorder()
			h.ServeHTTP(httptest.NewRecorder(), req)

			if tt.adopted && seen != tt.supplied {
				t.Errorf("request ID = %q, want the supplied %q", seen, tt.supplied)
			}
			if !tt.adopted {
				if seen == tt.supplied && tt.supplied != "" {
					t.Errorf("adopted an unsafe request ID %q", tt.supplied)
				}
				if seen == "" {
					t.Error("no replacement request ID was minted")
				}
			}
		})
	}
}

func TestTraceContinuedOrStarted(t *testing.T) {
	t.Parallel()

	const incoming = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

	tests := []struct {
		name        string
		header      string
		wantTraceID string // "" means "any freshly generated one"
	}{
		{"continues a valid trace", incoming, "4bf92f3577b34da6a3ce929d0e0e4736"},
		{"starts a trace when absent", "", ""},
		{"starts a trace when malformed", "garbage", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var tc telemetry.TraceContext
			var ok bool
			h := httpx.Trace()(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				tc, ok = telemetry.Trace(r.Context())
			}))
			req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
			if tt.header != "" {
				req.Header.Set(telemetry.HeaderTraceparent, tt.header)
			}
			h.ServeHTTP(httptest.NewRecorder(), req)

			if !ok {
				t.Fatal("no trace context placed on the request")
			}
			if !tc.Valid() {
				t.Errorf("trace context is not valid: %+v", tc)
			}
			if tt.wantTraceID != "" && tc.TraceID != tt.wantTraceID {
				t.Errorf("trace_id = %s, want the incoming %s", tc.TraceID, tt.wantTraceID)
			}
			if tt.wantTraceID == "" && tc.TraceID == "4bf92f3577b34da6a3ce929d0e0e4736" {
				t.Error("propagated a trace ID from a header we could not parse")
			}
		})
	}
}

// A panic in one request must not take down a replica serving others.
func TestRecoverConvertsPanicToEnvelope(t *testing.T) {
	t.Parallel()

	var logBuf bytes.Buffer
	logger, err := telemetry.NewLogger(&logBuf, config.LogConfig{Level: "error", Format: "json"},
		version.Info{Service: "t", Version: "1"}, "i")
	if err != nil {
		t.Fatalf("NewLogger() error = %v", err)
	}

	// Recover must sit INSIDE RequestID, otherwise the recovered panic has no
	// correlation identifier to log or to return.
	h := httpx.Chain(
		httpx.RequestID(),
		httpx.Recover(logger),
	)(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		panic("something went very wrong")
	}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/boom", http.NoBody))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}

	var env struct {
		Error struct {
			Message   string `json:"message"`
			Type      string `json:"type"`
			RequestID string `json:"request_id"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("response is not the standard envelope: %v\n%s", err, rec.Body.String())
	}
	if env.Error.Type != string(httpx.TypeInternal) {
		t.Errorf("error.type = %q, want %q", env.Error.Type, httpx.TypeInternal)
	}
	if env.Error.RequestID == "" {
		t.Error("error envelope carries no request_id, so the panic cannot be correlated")
	}
	// The panic message must not reach the client.
	if strings.Contains(rec.Body.String(), "something went very wrong") {
		t.Errorf("panic detail leaked to the client: %s", rec.Body.String())
	}
	// ...but it must reach the logs.
	if !strings.Contains(logBuf.String(), "something went very wrong") {
		t.Errorf("panic detail was not logged: %s", logBuf.String())
	}
	if !strings.Contains(logBuf.String(), "stack") {
		t.Error("panic log carries no stack trace")
	}
}

// Locks in the ordering constraint: AccessLog must sit outside Recover so a
// recovered panic is still recorded as a 500, rather than vanishing from the
// access log entirely.
func TestAccessLogRecordsRecoveredPanic(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger, _ := telemetry.NewLogger(&buf, config.LogConfig{Level: "info", Format: "json"},
		version.Info{Service: "t", Version: "1"}, "i")

	h := httpx.Chain(
		httpx.RequestID(),
		httpx.AccessLog(logger),
		httpx.Recover(telemetry.Discard()),
	)(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		panic("kaboom")
	}))

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/boom", http.NoBody))

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	var found bool
	for _, line := range lines {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			continue
		}
		if m[telemetry.FieldHTTPPath] == "/boom" {
			found = true
			if got, ok := m[telemetry.FieldHTTPStatus].(float64); !ok || int(got) != http.StatusInternalServerError {
				t.Errorf("access log http_status = %v, want 500", m[telemetry.FieldHTTPStatus])
			}
		}
	}
	if !found {
		t.Errorf("recovered panic produced no access log line:\n%s", buf.String())
	}
}

func TestErrorEnvelopeShape(t *testing.T) {
	t.Parallel()

	h := httpx.Chain(httpx.RequestID())(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteError(w, r, httpx.ErrInvalidRequest("max_tokens is too large", "context_length_exceeded", "max_tokens"), nil)
	}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", http.NoBody))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type = %q, want JSON", ct)
	}

	var env map[string]map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("not valid JSON: %v", err)
	}
	e, ok := env["error"]
	if !ok {
		t.Fatalf(`response has no "error" key: %s`, rec.Body.String())
	}
	for k, want := range map[string]any{
		"message": "max_tokens is too large",
		"type":    "invalid_request_error",
		"code":    "context_length_exceeded",
		"param":   "max_tokens",
	} {
		if e[k] != want {
			t.Errorf("error.%s = %v, want %v", k, e[k], want)
		}
	}
	if e["request_id"] == "" || e["request_id"] == nil {
		t.Error("error.request_id is empty")
	}
}

// A raw error must never have its text rendered to a client: it could carry SQL
// detail, hostnames or upstream bodies.
func TestWriteErrorHidesNonAPIErrors(t *testing.T) {
	t.Parallel()

	h := httpx.Chain(httpx.RequestID())(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteError(w, r, errors.New(`pq: relation "api_keys" does not exist on host db-primary-7`), nil)
	}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", http.NoBody))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	for _, leak := range []string{"api_keys", "db-primary-7", "relation"} {
		if strings.Contains(rec.Body.String(), leak) {
			t.Errorf("internal detail %q leaked to the client: %s", leak, rec.Body.String())
		}
	}
}

func TestAPIVersionHeader(t *testing.T) {
	t.Parallel()

	h := httpx.APIVersion()(okHandler())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", http.NoBody))

	if got := rec.Header().Get(httpx.HeaderAPIVersion); got != version.Contract {
		t.Errorf("%s = %q, want %q", httpx.HeaderAPIVersion, got, version.Contract)
	}
}

func TestMaxBodyRejectsOversizedRequest(t *testing.T) {
	t.Parallel()

	h := httpx.MaxBody(16)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.ReadAll(r.Body); err != nil {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(strings.Repeat("x", 1024)))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want 413", rec.Code)
	}
}

func TestAccessLogSkipsProbes(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger, _ := telemetry.NewLogger(&buf, config.LogConfig{Level: "info", Format: "json"},
		version.Info{Service: "t", Version: "1"}, "i")

	h := httpx.Chain(
		httpx.RequestID(),
		httpx.AccessLog(logger, telemetry.PathLivez, telemetry.PathReadyz),
	)(okHandler())

	for _, p := range []string{telemetry.PathLivez, telemetry.PathReadyz} {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, p, http.NoBody))
	}
	if buf.Len() != 0 {
		t.Errorf("probe requests were logged; kubelet polls would bury real events:\n%s", buf.String())
	}

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/real", http.NoBody))
	if buf.Len() == 0 {
		t.Fatal("a real request was not logged")
	}

	var m map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &m); err != nil {
		t.Fatalf("access log line is not valid JSON: %v", err)
	}
	for _, f := range []string{
		telemetry.FieldHTTPMethod, telemetry.FieldHTTPPath, telemetry.FieldHTTPStatus,
		telemetry.FieldDurationMS, telemetry.FieldRequestID, telemetry.FieldRemoteIP,
	} {
		if _, ok := m[f]; !ok {
			t.Errorf("access log is missing %q: %v", f, m)
		}
	}
	if m[telemetry.FieldHTTPPath] != "/real" {
		t.Errorf("http_path = %v, want /real", m[telemetry.FieldHTTPPath])
	}
}

func TestAccessLogLevelsByStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		status    int
		wantLevel string
	}{
		{http.StatusOK, "info"},
		{http.StatusBadRequest, "warn"},
		{http.StatusInternalServerError, "error"},
	}

	for _, tt := range tests {
		t.Run(http.StatusText(tt.status), func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			logger, _ := telemetry.NewLogger(&buf, config.LogConfig{Level: "debug", Format: "json"},
				version.Info{Service: "t", Version: "1"}, "i")
			h := httpx.AccessLog(logger)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.status)
			}))
			h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", http.NoBody))

			var m map[string]any
			if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &m); err != nil {
				t.Fatalf("not valid JSON: %v", err)
			}
			if m[telemetry.FieldLevel] != tt.wantLevel {
				t.Errorf("level for %d = %v, want %v", tt.status, m[telemetry.FieldLevel], tt.wantLevel)
			}
		})
	}
}

// X-Forwarded-For is caller-controlled. Trusting it without a configured
// trusted-proxy list turns rate limiting and audit logs into fiction.
func TestClientIPIgnoresForwardedHeaders(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger, _ := telemetry.NewLogger(&buf, config.LogConfig{Level: "info", Format: "json"},
		version.Info{Service: "t", Version: "1"}, "i")

	h := httpx.AccessLog(logger)(okHandler())
	req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	req.RemoteAddr = "10.1.2.3:54321"
	req.Header.Set("X-Forwarded-For", "203.0.113.9")
	h.ServeHTTP(httptest.NewRecorder(), req)

	var m map[string]any
	_ = json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &m)
	if m[telemetry.FieldRemoteIP] != "10.1.2.3" {
		t.Errorf("remote_ip = %v, want the real peer 10.1.2.3, not the client-supplied header",
			m[telemetry.FieldRemoteIP])
	}
}

// --- server -----------------------------------------------------------------

func testHTTPConfig() config.HTTPConfig {
	return config.HTTPConfig{
		Addr:              "127.0.0.1:0",
		ReadHeaderTimeout: config.Duration(2 * time.Second),
		ReadTimeout:       config.Duration(5 * time.Second),
		WriteTimeout:      config.Duration(5 * time.Second),
		IdleTimeout:       config.Duration(5 * time.Second),
		ShutdownGrace:     config.Duration(2 * time.Second),
		DrainDelay:        config.Duration(10 * time.Millisecond),
		MaxHeaderBytes:    1 << 20,
		MaxBodyBytes:      1 << 20,
	}
}

type fakeDrainer struct{ called chan struct{} }

func (f *fakeDrainer) MarkDraining() {
	select {
	case f.called <- struct{}{}:
	default:
	}
}

// Shutdown must fail readiness BEFORE closing the listener, so load balancers
// stop sending work before the server stops accepting it.
func TestServerDrainsBeforeShutdown(t *testing.T) {
	t.Parallel()

	ln := netx.Listen(t)

	drainer := &fakeDrainer{called: make(chan struct{}, 1)}
	srv := httpx.NewServer(httpx.ServerOptions{
		Config:   testHTTPConfig(),
		Handler:  okHandler(),
		Logger:   telemetry.Discard(),
		Drainer:  drainer,
		Listener: ln,
	})

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Run(ctx) }()

	// The server is serving.
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://" + ln.Addr().String() + "/")
	if err != nil {
		t.Fatalf("request to running server failed: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	cancel()

	select {
	case <-drainer.called:
	case <-time.After(2 * time.Second):
		t.Fatal("MarkDraining was never called; readiness would not fail before shutdown")
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run() returned %v, want a clean shutdown", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not shut down within 5s")
	}
}

// An in-flight request must be allowed to finish during the grace period.
func TestServerFinishesInFlightRequests(t *testing.T) {
	t.Parallel()

	ln := netx.Listen(t)

	released := make(chan struct{})
	started := make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		<-released
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("finished"))
	})

	srv := httpx.NewServer(httpx.ServerOptions{
		Config:   testHTTPConfig(),
		Handler:  handler,
		Logger:   telemetry.Discard(),
		Listener: ln,
	})

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- srv.Run(ctx) }()

	type result struct {
		body string
		err  error
	}
	resCh := make(chan result, 1)
	go func() {
		resp, err := (&http.Client{Timeout: 5 * time.Second}).Get("http://" + ln.Addr().String() + "/slow")
		if err != nil {
			resCh <- result{err: err}
			return
		}
		defer func() { _ = resp.Body.Close() }()
		b, err := io.ReadAll(resp.Body)
		resCh <- result{body: string(b), err: err}
	}()

	<-started
	cancel()                          // shutdown requested while a request is in flight
	time.Sleep(50 * time.Millisecond) // past the drain delay, into graceful shutdown
	close(released)

	select {
	case r := <-resCh:
		if r.err != nil {
			t.Fatalf("in-flight request failed during shutdown: %v", r.err)
		}
		if r.body != "finished" {
			t.Errorf("body = %q, want %q", r.body, "finished")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight request never completed")
	}

	if err := <-runErr; err != nil {
		t.Errorf("Run() = %v, want nil", err)
	}
}
