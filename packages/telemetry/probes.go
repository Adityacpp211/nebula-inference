package telemetry

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adityasatwar321/nebula/packages/version"
)

// Probe endpoint paths. Every NEBULA service serves all three at these paths.
const (
	PathLivez   = "/livez"
	PathReadyz  = "/readyz"
	PathHealthz = "/healthz"
)

// Checker reports whether one dependency is usable.
//
// Checkers are consulted by readiness only. Liveness never consults them: a
// failing dependency must not cause a restart loop across the fleet
// (docs/deployment-architecture.md §8.2).
type Checker interface {
	// Name identifies the dependency in /healthz output, e.g. "postgres".
	Name() string
	// Check returns nil when the dependency is usable. It must respect ctx.
	Check(ctx context.Context) error
	// Critical reports whether failure should make the service unready. A
	// non-critical checker is reported but does not remove the pod from service.
	Critical() bool
}

// CheckFunc adapts a function to Checker.
type CheckFunc struct {
	CheckName    string
	IsCritical   bool
	Fn           func(context.Context) error
	CheckTimeout time.Duration
}

// Name implements Checker.
func (c CheckFunc) Name() string { return c.CheckName }

// Critical implements Checker.
func (c CheckFunc) Critical() bool { return c.IsCritical }

// Check implements Checker.
func (c CheckFunc) Check(ctx context.Context) error {
	if c.Fn == nil {
		return nil
	}
	if c.CheckTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.CheckTimeout)
		defer cancel()
	}
	return c.Fn(ctx)
}

// Probes serves liveness, readiness and health.
type Probes struct {
	info     version.Info
	instance string
	env      string
	started  time.Time
	logger   *slog.Logger

	// configFn supplies the redacted effective configuration for /healthz. It is
	// a function so the caller decides what is safe to expose.
	configFn func() (map[string]any, error)

	// schemaVersion, when non-nil, is reported in /healthz. Set once the database
	// layer knows the applied schema version.
	schemaVersion atomic.Int64

	// draining flips on SIGTERM. Readiness fails first, then the process waits
	// for EndpointSlice propagation before refusing work.
	draining atomic.Bool
	// ready is set once startup has completed.
	ready atomic.Bool

	mu       sync.RWMutex
	checkers []Checker
	// checkTimeout bounds the whole readiness evaluation.
	checkTimeout time.Duration
}

// ProbesOptions configures a Probes.
type ProbesOptions struct {
	Info         version.Info
	Instance     string
	Env          string
	Logger       *slog.Logger
	ConfigFn     func() (map[string]any, error)
	CheckTimeout time.Duration
	Now          func() time.Time
}

// NewProbes creates the probe handler set. The service starts NOT ready; call
// MarkReady once startup has finished.
func NewProbes(opts ProbesOptions) *Probes {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	ct := opts.CheckTimeout
	if ct <= 0 {
		ct = 2 * time.Second
	}
	p := &Probes{
		info:         opts.Info,
		instance:     opts.Instance,
		env:          opts.Env,
		started:      now(),
		logger:       opts.Logger,
		configFn:     opts.ConfigFn,
		checkTimeout: ct,
	}
	return p
}

// Register adds a dependency checker.
func (p *Probes) Register(c ...Checker) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.checkers = append(p.checkers, c...)
}

// MarkReady declares startup complete.
func (p *Probes) MarkReady() { p.ready.Store(true) }

// MarkDraining fails readiness so load balancers stop sending new work.
func (p *Probes) MarkDraining() { p.draining.Store(true) }

// Draining reports whether the service is shutting down.
func (p *Probes) Draining() bool { return p.draining.Load() }

// SetSchemaVersion records the applied database schema version for /healthz.
func (p *Probes) SetSchemaVersion(v int64) { p.schemaVersion.Store(v) }

// checkResult is one dependency's outcome.
type checkResult struct {
	Name       string `json:"name"`
	Status     string `json:"status"`
	Critical   bool   `json:"critical"`
	Error      string `json:"error,omitempty"`
	DurationMS int64  `json:"duration_ms"`
}

// healthResponse is the /healthz body. Operator-facing, never public.
type healthResponse struct {
	Status        string         `json:"status"`
	Service       string         `json:"service"`
	Version       string         `json:"version"`
	Commit        string         `json:"commit"`
	BuildTime     string         `json:"build_time"`
	Contract      string         `json:"contract"`
	GoVersion     string         `json:"go_version"`
	Instance      string         `json:"instance"`
	Env           string         `json:"env"`
	UptimeSeconds float64        `json:"uptime_seconds"`
	Draining      bool           `json:"draining"`
	SchemaVersion int64          `json:"schema_version,omitempty"`
	Checks        []checkResult  `json:"checks"`
	Config        map[string]any `json:"config,omitempty"`
}

// Handler returns a mux serving the three probe endpoints.
func (p *Probes) Handler() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc(PathLivez, p.handleLivez)
	mux.HandleFunc(PathReadyz, p.handleReadyz)
	mux.HandleFunc(PathHealthz, p.handleHealthz)
	return mux
}

// Mount attaches the probe endpoints to an existing mux.
func (p *Probes) Mount(mux *http.ServeMux) {
	mux.HandleFunc(PathLivez, p.handleLivez)
	mux.HandleFunc(PathReadyz, p.handleReadyz)
	mux.HandleFunc(PathHealthz, p.handleHealthz)
}

// handleLivez answers "is this process wedged?" and nothing else. It deliberately
// performs no dependency checks and takes no locks that a request could hold.
func (p *Probes) handleLivez(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleReadyz answers "should this pod receive traffic right now?".
func (p *Probes) handleReadyz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")

	if p.draining.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("draining\n"))
		return
	}
	if !p.ready.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("starting\n"))
		return
	}

	// Only critical checks decide readiness, so only they are run: a non-critical
	// dependency that is slow to fail (an unreachable store waiting out its timeout)
	// must not make the probe itself time out, which Kubernetes would count as unready.
	results, failed := p.runChecks(r.Context(), true)
	if failed {
		w.WriteHeader(http.StatusServiceUnavailable)
		for _, res := range results {
			if res.Status != "ok" && res.Critical {
				_, _ = w.Write([]byte(res.Name + ": " + res.Error + "\n"))
			}
		}
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleHealthz is the operator view: full build identity, dependency detail and
// the redacted effective configuration.
func (p *Probes) handleHealthz(w http.ResponseWriter, r *http.Request) {
	results, failed := p.runChecks(r.Context(), false)

	status := "ok"
	code := http.StatusOK
	switch {
	case p.draining.Load():
		status, code = "draining", http.StatusServiceUnavailable
	case !p.ready.Load():
		status, code = "starting", http.StatusServiceUnavailable
	case failed:
		status, code = "degraded", http.StatusServiceUnavailable
	}

	resp := healthResponse{
		Status:        status,
		Service:       p.info.Service,
		Version:       p.info.Version,
		Commit:        p.info.Commit,
		BuildTime:     p.info.BuildTime,
		Contract:      p.info.Contract,
		GoVersion:     p.info.GoVersion,
		Instance:      p.instance,
		Env:           p.env,
		UptimeSeconds: time.Since(p.started).Seconds(),
		Draining:      p.draining.Load(),
		SchemaVersion: p.schemaVersion.Load(),
		Checks:        results,
	}
	if p.configFn != nil {
		if cfg, err := p.configFn(); err == nil {
			resp.Config = cfg
		} else if p.logger != nil {
			p.logger.Warn("could not render redacted config for /healthz", slog.String("error", err.Error()))
		}
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(resp); err != nil && p.logger != nil {
		p.logger.Warn("writing /healthz response", slog.String("error", err.Error()))
	}
}

// runChecks evaluates checkers concurrently under one deadline and reports whether
// any CRITICAL checker failed. With criticalOnly, non-critical checkers are skipped.
func (p *Probes) runChecks(ctx context.Context, criticalOnly bool) ([]checkResult, bool) {
	p.mu.RLock()
	checkers := make([]Checker, 0, len(p.checkers))
	for _, c := range p.checkers {
		if !criticalOnly || c.Critical() {
			checkers = append(checkers, c)
		}
	}
	p.mu.RUnlock()

	if len(checkers) == 0 {
		return []checkResult{}, false
	}

	ctx, cancel := context.WithTimeout(ctx, p.checkTimeout)
	defer cancel()

	results := make([]checkResult, len(checkers))
	var wg sync.WaitGroup
	for i, c := range checkers {
		wg.Add(1)
		go func(i int, c Checker) {
			defer wg.Done()
			start := time.Now()
			err := c.Check(ctx)
			res := checkResult{Name: c.Name(), Status: "ok", Critical: c.Critical()}
			if err != nil {
				res.Status = "failed"
				res.Error = err.Error()
			}
			res.DurationMS = time.Since(start).Milliseconds()
			results[i] = res
		}(i, c)
	}
	wg.Wait()

	failed := false
	for _, r := range results {
		if r.Status != "ok" && r.Critical {
			failed = true
		}
	}
	return results, failed
}

// ErrNotReady is returned by helpers that require a ready service.
var ErrNotReady = errors.New("service not ready")
