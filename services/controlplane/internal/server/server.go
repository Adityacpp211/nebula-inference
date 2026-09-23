// Package server wires the control plane's HTTP surface.
//
// The wiring lives here rather than in main.go so the middleware chain and the
// route table are testable without starting a process, and so each phase adds
// handlers to an existing, tested skeleton instead of restructuring the binary.
package server

import (
	"log/slog"
	"net/http"

	"github.com/adityasatwar321/nebula/packages/api"
	"github.com/adityasatwar321/nebula/packages/config"
	"github.com/adityasatwar321/nebula/packages/httpx"
	"github.com/adityasatwar321/nebula/packages/telemetry"
	cpapi "github.com/adityasatwar321/nebula/services/controlplane/internal/api"
)

// Options configures the HTTP handler.
type Options struct {
	Config *config.Config
	Logger *slog.Logger
	Probes *telemetry.Probes

	// API is the control-plane handler set. It may be nil, which mounts the probe
	// endpoints and the specification alone — the shape a binary takes when it has
	// a database but no credential pepper, and the shape the middleware tests use.
	API *cpapi.API
}

// New builds the control plane's root handler.
//
// Middleware order is deliberate and is the same in every NEBULA service. Two
// constraints fix it:
//
//   - Recover must be INSIDE RequestID and Trace, so a recovered panic still has
//     correlation identifiers to log and to return in the error envelope.
//
//   - AccessLog must be OUTSIDE Recover, so it observes the 500 that Recover
//     wrote rather than missing the request entirely.
//
//     RequestID    outermost: everything below can be correlated
//     Trace        continue or start a trace before anything logs
//     WithLogger   make the correlated logger available to handlers
//     APIVersion   advertise the contract on every response, errors included
//     AccessLog    observe the final status, including recovered panics
//     Recover      convert a panic into a 500 in the standard envelope
//     MaxBody      bound the request before a handler reads it
//
// Authentication is NOT in this chain. It is applied per route, because the probe
// endpoints and the specification must be reachable without a credential: a
// readiness probe cannot carry one, and a client cannot construct a valid request
// without the contract.
func New(opts Options) http.Handler {
	mux := http.NewServeMux()
	opts.Probes.Mount(mux)

	mux.Handle(api.SpecPath, api.Handler())

	if opts.API != nil {
		opts.API.Mount(mux)
	}

	// Anything not explicitly routed is a 404 in the standard envelope, so the
	// error shape is identical whether it came from a handler or from the router.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteError(w, r, httpx.ErrNotFound("no such endpoint"), opts.Logger)
	})

	chain := httpx.Chain(
		httpx.RequestID(),
		httpx.Trace(),
		httpx.WithLogger(opts.Logger),
		httpx.APIVersion(),
		httpx.AccessLog(opts.Logger, telemetry.PathLivez, telemetry.PathReadyz, telemetry.PathHealthz),
		httpx.Recover(opts.Logger),
		httpx.MaxBody(opts.Config.HTTP.MaxBodyBytes),
	)
	return chain(mux)
}
