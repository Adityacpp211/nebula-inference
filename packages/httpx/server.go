package httpx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/adityasatwar321/nebula/packages/config"
)

// Drainer is the readiness surface the server flips during shutdown. Satisfied by
// *telemetry.Probes; an interface so httpx does not depend on the probe
// implementation.
type Drainer interface {
	MarkDraining()
}

// Server runs an HTTP server with the shutdown sequence Kubernetes needs.
type Server struct {
	cfg     config.HTTPConfig
	handler http.Handler
	logger  *slog.Logger
	drainer Drainer

	// listener, when set, is used instead of dialing cfg.Addr. Tests use this to
	// bind port 0 and learn the real address.
	listener net.Listener
}

// ServerOptions configures a Server.
type ServerOptions struct {
	Config  config.HTTPConfig
	Handler http.Handler
	Logger  *slog.Logger
	// Drainer is notified before shutdown so readiness fails first.
	Drainer Drainer
	// Listener overrides Config.Addr when non-nil.
	Listener net.Listener
}

// NewServer creates a server. It does not listen until Run is called.
func NewServer(opts ServerOptions) *Server {
	return &Server{
		cfg:      opts.Config,
		handler:  opts.Handler,
		logger:   opts.Logger,
		drainer:  opts.Drainer,
		listener: opts.Listener,
	}
}

// Run serves until ctx is cancelled, then drains.
//
// The shutdown sequence, in order, and why each step exists:
//
//  1. Fail readiness. Kubernetes stops routing new work to this pod.
//  2. Wait DrainDelay. EndpointSlice propagation is not instant, so load
//     balancers may still send requests for a second or two after step 1. Skipping
//     this wait is the usual cause of "503s during a rolling deploy".
//  3. Graceful shutdown bounded by ShutdownGrace. In-flight requests finish;
//     idle keep-alive connections are closed.
//  4. Force close anything still running, so the process always exits.
func (s *Server) Run(ctx context.Context) error {
	ln := s.listener
	if ln == nil {
		var err error
		ln, err = net.Listen("tcp", s.cfg.Addr)
		if err != nil {
			return fmt.Errorf("listening on %s: %w", s.cfg.Addr, err)
		}
	}

	srv := &http.Server{
		Handler:           s.handler,
		ReadHeaderTimeout: s.cfg.ReadHeaderTimeout.Duration(),
		ReadTimeout:       s.cfg.ReadTimeout.Duration(),
		WriteTimeout:      s.cfg.WriteTimeout.Duration(),
		IdleTimeout:       s.cfg.IdleTimeout.Duration(),
		MaxHeaderBytes:    s.cfg.MaxHeaderBytes,
		// BaseContext ties connection contexts to the process lifetime so a
		// cancelled parent context reaches in-flight handlers.
		BaseContext: func(net.Listener) context.Context { return ctx },
	}

	errCh := make(chan error, 1)
	go func() {
		if s.logger != nil {
			s.logger.Info("http server listening", slog.String("addr", ln.Addr().String()))
		}
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("http server: %w", err)
			return
		}
		errCh <- nil
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	// Step 1: fail readiness.
	if s.drainer != nil {
		s.drainer.MarkDraining()
	}
	if s.logger != nil {
		s.logger.Info("shutdown requested, draining",
			slog.Duration("drain_delay", s.cfg.DrainDelay.Duration()),
			slog.Duration("shutdown_grace", s.cfg.ShutdownGrace.Duration()))
	}

	// Step 2: let endpoint removal propagate. Uses a fresh context because ctx is
	// already cancelled.
	if s.cfg.DrainDelay.Duration() > 0 {
		t := time.NewTimer(s.cfg.DrainDelay.Duration())
		defer t.Stop()
		<-t.C
	}

	// Step 3: graceful shutdown.
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.cfg.ShutdownGrace.Duration())
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		// Step 4: in-flight work outlasted the grace period.
		if s.logger != nil {
			s.logger.Warn("graceful shutdown exceeded grace period, closing connections",
				slog.String("error", err.Error()))
		}
		if cerr := srv.Close(); cerr != nil && s.logger != nil {
			s.logger.Warn("force close failed", slog.String("error", cerr.Error()))
		}
	}

	if err := <-errCh; err != nil {
		return err
	}
	if s.logger != nil {
		s.logger.Info("http server stopped")
	}
	return nil
}
