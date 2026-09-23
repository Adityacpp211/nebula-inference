package server_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/adityasatwar321/nebula/packages/config"
	"github.com/adityasatwar321/nebula/packages/httpx"
	"github.com/adityasatwar321/nebula/packages/telemetry"
	"github.com/adityasatwar321/nebula/packages/version"
	"github.com/adityasatwar321/nebula/services/controlplane/internal/server"
)

// newHandler builds the real handler with the real middleware chain, so this
// exercises the wiring rather than a simplified stand-in.
func newHandler(t *testing.T) (http.Handler, *telemetry.Probes) {
	t.Helper()

	cfg, err := config.Loader{
		Service: "nebula-controlplane",
		Getenv: func(k string) string {
			if k == "NEBULA_DATABASE_URL" {
				return "postgres://nebula:not-a-default-secret@localhost:5432/nebula"
			}
			return ""
		},
	}.Load()
	if err != nil {
		t.Fatalf("loading config: %v", err)
	}

	probes := telemetry.NewProbes(telemetry.ProbesOptions{
		Info:     version.Get("nebula-controlplane"),
		Instance: "test-pod",
		Env:      cfg.Env.String(),
		Logger:   telemetry.Discard(),
		ConfigFn: cfg.Redact,
	})

	h := server.New(server.Options{Config: cfg, Logger: telemetry.Discard(), Probes: probes})
	return h, probes
}

func TestProbeEndpointsAreWired(t *testing.T) {
	t.Parallel()

	h, probes := newHandler(t)
	probes.MarkReady()

	for _, path := range []string{telemetry.PathLivez, telemetry.PathReadyz, telemetry.PathHealthz} {
		t.Run(path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
			if rec.Code != http.StatusOK {
				t.Errorf("%s = %d, want 200; body: %s", path, rec.Code, rec.Body.String())
			}
			if rec.Header().Get(httpx.HeaderRequestID) == "" {
				t.Errorf("%s response carries no %s header", path, httpx.HeaderRequestID)
			}
			if got := rec.Header().Get(httpx.HeaderAPIVersion); got != version.Contract {
				t.Errorf("%s: %s = %q, want %q", path, httpx.HeaderAPIVersion, got, version.Contract)
			}
		})
	}
}

// Phase 1 serves no business endpoints, and an unrouted path must still produce
// the standard envelope rather than net/http's plain-text 404.
func TestUnknownPathReturnsTheStandardEnvelope(t *testing.T) {
	t.Parallel()

	h, probes := newHandler(t)
	probes.MarkReady()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/deployments", nil))

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type = %q, want JSON", ct)
	}

	var env struct {
		Error struct {
			Type      string `json:"type"`
			Message   string `json:"message"`
			RequestID string `json:"request_id"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("body is not the standard envelope: %v\n%s", err, rec.Body.String())
	}
	if env.Error.Type != string(httpx.TypeNotFound) {
		t.Errorf("error.type = %q, want %q", env.Error.Type, httpx.TypeNotFound)
	}
	if env.Error.RequestID == "" {
		t.Error("error envelope carries no request_id")
	}
}

// Readiness must reflect lifecycle, so Kubernetes only routes traffic to a pod
// that has finished starting and has not begun draining.
func TestReadinessReflectsLifecycle(t *testing.T) {
	t.Parallel()

	h, probes := newHandler(t)

	get := func() int {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, telemetry.PathReadyz, nil))
		return rec.Code
	}

	if code := get(); code != http.StatusServiceUnavailable {
		t.Errorf("readyz before startup completed = %d, want 503", code)
	}
	probes.MarkReady()
	if code := get(); code != http.StatusOK {
		t.Errorf("readyz after MarkReady = %d, want 200", code)
	}
	probes.MarkDraining()
	if code := get(); code != http.StatusServiceUnavailable {
		t.Errorf("readyz while draining = %d, want 503", code)
	}

	// Liveness must stay up throughout: the process is healthy, it is just not
	// accepting new work.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, telemetry.PathLivez, nil))
	if rec.Code != http.StatusOK {
		t.Errorf("livez while draining = %d, want 200", rec.Code)
	}
}

// /healthz is operator-facing and reports the effective configuration; the
// database password must not be in it.
func TestHealthzDoesNotLeakSecrets(t *testing.T) {
	t.Parallel()

	h, probes := newHandler(t)
	probes.MarkReady()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, telemetry.PathHealthz, nil))

	body := rec.Body.String()
	if strings.Contains(body, "not-a-default-secret") {
		t.Fatalf("/healthz leaked the database password:\n%s", body)
	}
	if !strings.Contains(body, config.Redacted) {
		t.Errorf("/healthz does not show the redaction placeholder:\n%s", body)
	}
	if !strings.Contains(body, "nebula-controlplane") {
		t.Errorf("/healthz does not identify the service:\n%s", body)
	}
}
