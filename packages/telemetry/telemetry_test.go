package telemetry_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/adityasatwar321/nebula/packages/config"
	"github.com/adityasatwar321/nebula/packages/telemetry"
	"github.com/adityasatwar321/nebula/packages/version"
)

func testInfo() version.Info {
	return version.Info{
		Service: "nebula-test", Version: "1.2.3", Commit: "abc1234",
		BuildTime: "2026-09-22T00:00:00Z", Contract: "v1", GoVersion: "go1.24",
	}
}

// decode reads the last JSON log line written to buf.
func decode(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) == 0 || lines[0] == "" {
		t.Fatal("no log output")
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &m); err != nil {
		t.Fatalf("log line is not valid JSON: %v\nline: %s", err, lines[len(lines)-1])
	}
	return m
}

// The log schema is a contract: dashboards and Loki queries depend on the exact
// field names (docs/observability.md §4.1).
func TestLoggerEmitsMandatoryFields(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger, err := telemetry.NewLogger(&buf, config.LogConfig{Level: "info", Format: "json"}, testInfo(), "pod-xyz")
	if err != nil {
		t.Fatalf("NewLogger() error = %v", err)
	}
	logger.Info("hello")

	m := decode(t, &buf)
	for _, f := range []string{
		telemetry.FieldTime, telemetry.FieldLevel, telemetry.FieldMessage,
		telemetry.FieldService, telemetry.FieldVersion, telemetry.FieldInstance,
	} {
		if _, ok := m[f]; !ok {
			t.Errorf("log line is missing mandatory field %q: %v", f, m)
		}
	}
	if m[telemetry.FieldService] != "nebula-test" {
		t.Errorf("service = %v, want nebula-test", m[telemetry.FieldService])
	}
	if m[telemetry.FieldInstance] != "pod-xyz" {
		t.Errorf("instance = %v, want pod-xyz", m[telemetry.FieldInstance])
	}
	if m[telemetry.FieldLevel] != "info" {
		t.Errorf("level = %v, want lowercase %q", m[telemetry.FieldLevel], "info")
	}
	// slog's default "time" key is renamed; the old one must be gone.
	if _, ok := m["time"]; ok {
		t.Errorf(`log line still uses "time"; it must be renamed to %q`, telemetry.FieldTime)
	}
}

func TestLoggerRejectsBadLevelAndFormat(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	if _, err := telemetry.NewLogger(&buf, config.LogConfig{Level: "screaming", Format: "json"}, testInfo(), "i"); err == nil {
		t.Error("NewLogger() accepted an unknown level")
	}
	if _, err := telemetry.NewLogger(&buf, config.LogConfig{Level: "info", Format: "xml"}, testInfo(), "i"); err == nil {
		t.Error("NewLogger() accepted an unknown format")
	}
}

// Correlation identifiers must reach every log line without the call site
// remembering to add them (axiom A5).
func TestLoggerFromContextCarriesCorrelation(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	base, err := telemetry.NewLogger(&buf, config.LogConfig{Level: "info", Format: "json"}, testInfo(), "i")
	if err != nil {
		t.Fatalf("NewLogger() error = %v", err)
	}

	tc, err := telemetry.NewTraceContext(true)
	if err != nil {
		t.Fatalf("NewTraceContext() error = %v", err)
	}
	ctx := telemetry.WithOrgID(
		telemetry.WithTrace(
			telemetry.WithRequestID(context.Background(), "req-123"), tc),
		"org-456")

	telemetry.Logger(ctx, base).Info("correlated")

	m := decode(t, &buf)
	if m[telemetry.FieldRequestID] != "req-123" {
		t.Errorf("request_id = %v, want req-123", m[telemetry.FieldRequestID])
	}
	if m[telemetry.FieldTraceID] != tc.TraceID {
		t.Errorf("trace_id = %v, want %s", m[telemetry.FieldTraceID], tc.TraceID)
	}
	if m[telemetry.FieldSpanID] != tc.SpanID {
		t.Errorf("span_id = %v, want %s", m[telemetry.FieldSpanID], tc.SpanID)
	}
	if m[telemetry.FieldOrgID] != "org-456" {
		t.Errorf("org_id = %v, want org-456", m[telemetry.FieldOrgID])
	}
}

func TestLoggerWithoutCorrelationOmitsFields(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	base, _ := telemetry.NewLogger(&buf, config.LogConfig{Level: "info", Format: "json"}, testInfo(), "i")
	telemetry.Logger(context.Background(), base).Info("plain")

	m := decode(t, &buf)
	for _, f := range []string{telemetry.FieldRequestID, telemetry.FieldTraceID, telemetry.FieldOrgID} {
		if _, ok := m[f]; ok {
			t.Errorf("field %q present without correlation context; it should be omitted, not empty", f)
		}
	}
}

func TestNewRequestIDIsUniqueAndTimeOrdered(t *testing.T) {
	t.Parallel()

	const n = 200
	seen := make(map[string]bool, n)
	prev := ""
	for i := range n {
		id := telemetry.NewRequestID()
		if seen[id] {
			t.Fatalf("duplicate request ID at iteration %d: %s", i, id)
		}
		seen[id] = true
		// UUIDv7 is time-ordered, so lexical order tracks creation order.
		if prev != "" && id < prev {
			t.Errorf("request ID %s sorts before the previously generated %s; UUIDv7 must be time-ordered", id, prev)
		}
		prev = id
	}
}

func TestParseTraceparent(t *testing.T) {
	t.Parallel()

	const (
		traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
		spanID  = "00f067aa0ba902b7"
	)

	tests := []struct {
		name        string
		in          string
		wantErr     bool
		wantSampled bool
	}{
		{name: "sampled", in: "00-" + traceID + "-" + spanID + "-01", wantSampled: true},
		{name: "not sampled", in: "00-" + traceID + "-" + spanID + "-00"},
		{name: "empty", in: "", wantErr: true},
		{name: "too few fields", in: "00-" + traceID + "-" + spanID, wantErr: true},
		{name: "unsupported version", in: "99-" + traceID + "-" + spanID + "-01", wantErr: true},
		{name: "short trace id", in: "00-abc-" + spanID + "-01", wantErr: true},
		{name: "uppercase trace id", in: "00-" + strings.ToUpper(traceID) + "-" + spanID + "-01", wantErr: true},
		{name: "zero trace id", in: "00-" + strings.Repeat("0", 32) + "-" + spanID + "-01", wantErr: true},
		{name: "zero span id", in: "00-" + traceID + "-" + strings.Repeat("0", 16) + "-01", wantErr: true},
		{name: "bad flags", in: "00-" + traceID + "-" + spanID + "-zz", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tc, err := telemetry.ParseTraceparent(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ParseTraceparent(%q) succeeded, want error", tt.in)
				}
				if !errors.Is(err, telemetry.ErrInvalidTraceparent) {
					t.Errorf("error does not wrap ErrInvalidTraceparent: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseTraceparent(%q) error = %v", tt.in, err)
			}
			if tc.TraceID != traceID || tc.SpanID != spanID {
				t.Errorf("parsed %s/%s, want %s/%s", tc.TraceID, tc.SpanID, traceID, spanID)
			}
			if tc.Sampled != tt.wantSampled {
				t.Errorf("Sampled = %v, want %v", tc.Sampled, tt.wantSampled)
			}
			if got := tc.Header(); got != tt.in {
				t.Errorf("Header() = %q, want a round trip of %q", got, tt.in)
			}
		})
	}
}

func TestTraceContextChildKeepsTrace(t *testing.T) {
	t.Parallel()

	parent, err := telemetry.NewTraceContext(true)
	if err != nil {
		t.Fatalf("NewTraceContext() error = %v", err)
	}
	child, err := parent.Child()
	if err != nil {
		t.Fatalf("Child() error = %v", err)
	}
	if child.TraceID != parent.TraceID {
		t.Errorf("child trace = %s, want the parent's %s", child.TraceID, parent.TraceID)
	}
	if child.SpanID == parent.SpanID {
		t.Error("child has the same span ID as its parent")
	}
	if !child.Valid() {
		t.Error("child context is not valid")
	}
}

// --- probes -----------------------------------------------------------------

func newProbes(t *testing.T) *telemetry.Probes {
	t.Helper()
	return telemetry.NewProbes(telemetry.ProbesOptions{
		Info:         testInfo(),
		Instance:     "pod-xyz",
		Env:          "dev",
		Logger:       telemetry.Discard(),
		CheckTimeout: time.Second,
	})
}

func get(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, http.NoBody))
	return rec
}

// Liveness must answer "is the process wedged?" and never consult dependencies:
// a failing database must not cause a fleet-wide restart loop.
func TestLivezIgnoresEverything(t *testing.T) {
	t.Parallel()

	p := newProbes(t)
	p.Register(telemetry.CheckFunc{
		CheckName:  "always-fails",
		IsCritical: true,
		Fn:         func(context.Context) error { return errors.New("down") },
	})
	// Not ready, draining, and with a failing critical dependency.
	p.MarkDraining()

	if rec := get(t, p.Handler(), telemetry.PathLivez); rec.Code != http.StatusOK {
		t.Errorf("livez = %d, want 200 even while draining with a failing dependency", rec.Code)
	}
}

func TestReadyzLifecycle(t *testing.T) {
	t.Parallel()

	p := newProbes(t)
	h := p.Handler()

	if rec := get(t, h, telemetry.PathReadyz); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("readyz before MarkReady = %d, want 503", rec.Code)
	}

	p.MarkReady()
	if rec := get(t, h, telemetry.PathReadyz); rec.Code != http.StatusOK {
		t.Errorf("readyz after MarkReady = %d, want 200", rec.Code)
	}

	p.MarkDraining()
	rec := get(t, h, telemetry.PathReadyz)
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("readyz while draining = %d, want 503", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "draining") {
		t.Errorf("readyz body = %q, want it to say draining", rec.Body.String())
	}
	if !p.Draining() {
		t.Error("Draining() = false after MarkDraining()")
	}
}

// A non-critical dependency is reported but must not remove the pod from service.
func TestReadyzCriticalVersusNonCritical(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		critical bool
		wantCode int
	}{
		{"critical failure removes from service", true, http.StatusServiceUnavailable},
		{"non-critical failure is reported only", false, http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := newProbes(t)
			p.Register(telemetry.CheckFunc{
				CheckName:  "dep",
				IsCritical: tt.critical,
				Fn:         func(context.Context) error { return errors.New("boom") },
			})
			p.MarkReady()

			if rec := get(t, p.Handler(), telemetry.PathReadyz); rec.Code != tt.wantCode {
				t.Errorf("readyz = %d, want %d", rec.Code, tt.wantCode)
			}
		})
	}
}

func TestHealthzShape(t *testing.T) {
	t.Parallel()

	p := telemetry.NewProbes(telemetry.ProbesOptions{
		Info:     testInfo(),
		Instance: "pod-xyz",
		Env:      "dev",
		Logger:   telemetry.Discard(),
		ConfigFn: func() (map[string]any, error) {
			return map[string]any{"database": map[string]any{"url": config.Redacted}}, nil
		},
	})
	p.SetSchemaVersion(8)
	p.Register(
		telemetry.CheckFunc{CheckName: "ok-dep", IsCritical: true, Fn: func(context.Context) error { return nil }},
		telemetry.CheckFunc{CheckName: "bad-dep", IsCritical: false, Fn: func(context.Context) error { return errors.New("nope") }},
	)
	p.MarkReady()

	rec := get(t, p.Handler(), telemetry.PathHealthz)
	if rec.Code != http.StatusOK {
		t.Fatalf("healthz = %d, want 200 (a non-critical failure is not degraded)", rec.Code)
	}

	var body struct {
		Status        string `json:"status"`
		Service       string `json:"service"`
		Version       string `json:"version"`
		Contract      string `json:"contract"`
		Instance      string `json:"instance"`
		SchemaVersion int64  `json:"schema_version"`
		Checks        []struct {
			Name     string `json:"name"`
			Status   string `json:"status"`
			Critical bool   `json:"critical"`
			Error    string `json:"error"`
		} `json:"checks"`
		Config map[string]any `json:"config"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("healthz body is not valid JSON: %v\n%s", err, rec.Body.String())
	}

	if body.Status != "ok" || body.Service != "nebula-test" || body.Contract != "v1" {
		t.Errorf("unexpected identity fields: %+v", body)
	}
	if body.SchemaVersion != 8 {
		t.Errorf("schema_version = %d, want 8", body.SchemaVersion)
	}
	if len(body.Checks) != 2 {
		t.Fatalf("got %d checks, want 2", len(body.Checks))
	}
	byName := map[string]string{}
	for _, c := range body.Checks {
		byName[c.Name] = c.Status
	}
	if byName["ok-dep"] != "ok" || byName["bad-dep"] != "failed" {
		t.Errorf("check statuses = %v", byName)
	}
	if body.Config == nil {
		t.Error("healthz omitted the effective configuration")
	}
}

func TestHealthzReportsDegradedOnCriticalFailure(t *testing.T) {
	t.Parallel()

	p := newProbes(t)
	p.Register(telemetry.CheckFunc{
		CheckName: "postgres", IsCritical: true,
		Fn: func(context.Context) error { return errors.New("connection refused") },
	})
	p.MarkReady()

	rec := get(t, p.Handler(), telemetry.PathHealthz)
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("healthz = %d, want 503", rec.Code)
	}
	var body struct {
		Status string `json:"status"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if body.Status != "degraded" {
		t.Errorf("status = %q, want degraded", body.Status)
	}
}

// A hanging dependency must not hang the probe: a probe that never answers is
// worse than one that fails, because the pod stays in service while unresponsive.
func TestCheckTimeoutIsEnforced(t *testing.T) {
	t.Parallel()

	p := telemetry.NewProbes(telemetry.ProbesOptions{
		Info: testInfo(), Instance: "i", Env: "dev",
		Logger:       telemetry.Discard(),
		CheckTimeout: 50 * time.Millisecond,
	})
	p.Register(telemetry.CheckFunc{
		CheckName: "slow", IsCritical: true,
		Fn: func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(5 * time.Second):
				return nil
			}
		},
	})
	p.MarkReady()

	done := make(chan int, 1)
	go func() { done <- get(t, p.Handler(), telemetry.PathReadyz).Code }()

	select {
	case code := <-done:
		if code != http.StatusServiceUnavailable {
			t.Errorf("readyz with a timed-out check = %d, want 503", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("readyz did not return within 2s; the check timeout is not being enforced")
	}
}
