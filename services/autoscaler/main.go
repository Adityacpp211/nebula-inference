// Command nebula-autoscaler decides how many replicas each autoscaled deployment
// runs (docs/architecture.md §4.4, §7.2).
//
// It reads signals from Prometheus, decides with packages/autoscale, and writes
// deployments.desired_replicas plus an autoscaling_events row for every decision
// worth one, connecting to PostgreSQL as nebula_controller. It never calls the
// Kubernetes API: the controller reconciles the new desired count like any other
// spec change. One replica is active at a time, chosen by a PostgreSQL advisory
// lock; setting replicas to 0 disables autoscaling without touching anything else.
//
// Its failure behaviour is to hold: with Prometheus down every decision is
// suppressed_no_signal, and with PostgreSQL down it decides nothing.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/adityasatwar321/nebula/migrations"
	"github.com/adityasatwar321/nebula/packages/config"
	"github.com/adityasatwar321/nebula/packages/db"
	"github.com/adityasatwar321/nebula/packages/db/migrate"
	"github.com/adityasatwar321/nebula/packages/httpx"
	"github.com/adityasatwar321/nebula/packages/telemetry"
	"github.com/adityasatwar321/nebula/packages/version"
	"github.com/adityasatwar321/nebula/services/autoscaler/internal/leader"
	"github.com/adityasatwar321/nebula/services/autoscaler/internal/loop"
	"github.com/adityasatwar321/nebula/services/autoscaler/internal/signals"
	"github.com/adityasatwar321/nebula/services/autoscaler/internal/store"
)

const serviceName = config.ServiceAutoscaler

func main() {
	if err := runMain(); err != nil {
		if errors.Is(err, config.ErrHelpRequested) {
			os.Exit(0)
		}
		fmt.Fprintf(os.Stderr, "%s: %v\n", serviceName, err)
		os.Exit(1)
	}
}

func runMain() error {
	info := version.Get(serviceName)
	cfg, err := config.Loader{
		Service:  serviceName,
		Args:     os.Args[1:],
		Defaults: map[string]string{"NEBULA_HTTP_ADDR": ":8084"},
	}.Load()
	if err != nil {
		return err
	}
	instance, err := os.Hostname()
	if err != nil || instance == "" {
		instance = "unknown"
	}
	logger, err := telemetry.NewLogger(os.Stdout, cfg.Log, info, instance)
	if err != nil {
		return fmt.Errorf("configuring logger: %w", err)
	}
	logger.Info("starting", slog.String("build", info.Short()), slog.String("env", cfg.Env.String()),
		slog.String("prometheus", cfg.Autoscaler.PrometheusURL),
		slog.Duration("interval", cfg.Autoscaler.Interval.Duration()))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	loaded, err := migrate.Load(migrations.FS)
	if err != nil {
		return err
	}
	expected := loaded[len(loaded)-1].Version
	pool, err := db.Open(ctx, cfg.Database, serviceName+"/"+info.Version, logger)
	if err != nil {
		return err
	}
	defer pool.Close()
	if cfg.Database.AssertSchemaVersion {
		if err := db.AssertSchemaVersion(ctx, pool, expected); err != nil {
			return fmt.Errorf("schema check failed: %w", err)
		}
	}

	metrics := telemetry.NewMetrics()
	metrics.Gauge("nebula_schema_version").WithLabelValues(serviceName).Set(float64(expected))
	if cfg.Telemetry.MetricsAddr != "" {
		go telemetry.ServeMetrics(ctx, cfg.Telemetry.MetricsAddr, metrics, logger)
	}

	elector := &leader.Elector{Pool: pool, Logger: logger}
	l := &loop.Loop{
		Store: store.New(pool),
		Signals: &signals.Prometheus{
			BaseURL: cfg.Autoscaler.PrometheusURL,
			Client:  &http.Client{Timeout: cfg.Autoscaler.QueryTimeout.Duration()},
			Window:  cfg.Autoscaler.SignalWindow.Duration(),
		},
		Metrics: metrics,
		Logger:  logger,
	}

	probes := telemetry.NewProbes(telemetry.ProbesOptions{
		Info: info, Instance: instance, Env: cfg.Env.String(), Logger: logger, ConfigFn: cfg.Redact,
	})
	probes.SetSchemaVersion(expected)
	probes.Register(db.HealthChecker{Pool: pool, ExpectedSchema: expected})
	// A standby is ready: waiting is its job. Leadership is reported for operators.
	probes.Register(telemetry.CheckFunc{CheckName: "leadership", IsCritical: false,
		Fn: func(context.Context) error {
			if !elector.IsLeader() {
				return errors.New("standby: another replica holds the advisory lock")
			}
			return nil
		}})

	srv := httpx.NewServer(httpx.ServerOptions{Config: cfg.HTTP, Handler: probes.Handler(), Logger: logger, Drainer: probes})
	probes.MarkReady()

	errc := make(chan error, 1)
	go func() { errc <- srv.Run(ctx) }()
	done := make(chan struct{})
	go func() {
		defer close(done)
		l.Run(ctx, cfg.Autoscaler.Interval.Duration(), elector.Lead)
	}()
	logger.Info("ready", slog.String("probes", cfg.HTTP.Addr))

	err = <-errc
	stop()
	<-done
	rctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	elector.Release(rctx)
	if err != nil {
		logger.Error("stopping", slog.String("cause", err.Error()))
		return err
	}
	logger.Info("stopped")
	return nil
}
