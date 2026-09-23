// Command nebula-controlplane is NEBULA's admin API and the only writer of
// desired state.
//
// Phase 1 scope: configuration, logging, request identity, a verified database
// pool, and the three probe endpoints. It serves no business endpoints yet; those
// arrive in Phase 2 (docs/roadmap.md).
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"log/slog"

	"github.com/adityasatwar321/nebula/migrations"
	"github.com/adityasatwar321/nebula/packages/config"
	"github.com/adityasatwar321/nebula/packages/db"
	"github.com/adityasatwar321/nebula/packages/db/migrate"
	"github.com/adityasatwar321/nebula/packages/httpx"
	"github.com/adityasatwar321/nebula/packages/telemetry"
	"github.com/adityasatwar321/nebula/packages/version"
	"github.com/adityasatwar321/nebula/services/controlplane/internal/server"
)

const serviceName = "nebula-controlplane"

func main() {
	if err := run(); err != nil {
		if errors.Is(err, config.ErrHelpRequested) {
			os.Exit(0)
		}
		// Configuration and startup failures happen before the logger exists, or
		// because the logger could not be built, so they go to stderr plainly.
		fmt.Fprintf(os.Stderr, "%s: %v\n", serviceName, err)
		os.Exit(1)
	}
}

func run() error {
	info := version.Get(serviceName)

	cfg, err := config.Loader{Service: serviceName, Args: os.Args[1:]}.Load()
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
	logger.Info("starting", slog.String("build", info.Short()), slog.String("env", cfg.Env.String()))

	// Signals are handled before anything expensive starts, so an operator can
	// interrupt a slow startup.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// The schema version this binary was built against is derived from the
	// embedded migrations, so it cannot drift from what the migrator would apply.
	loaded, err := migrate.Load(migrations.FS)
	if err != nil {
		return fmt.Errorf("loading embedded migrations: %w", err)
	}
	expectedSchema := int64(0)
	if n := len(loaded); n > 0 {
		expectedSchema = loaded[n-1].Version
	}

	pool, err := db.Open(ctx, cfg.Database, serviceName+"/"+info.Version, logger)
	if err != nil {
		return err
	}
	defer pool.Close()

	if cfg.Database.AssertSchemaVersion {
		if err := db.AssertSchemaVersion(ctx, pool, expectedSchema); err != nil {
			// Refusing to serve is the point: a binary running against a schema it
			// was not built for can write columns that no longer mean what it
			// thinks they mean.
			return fmt.Errorf("schema check failed: %w", err)
		}
		logger.Info("schema version verified", slog.Int64("version", expectedSchema))
	}

	probes := telemetry.NewProbes(telemetry.ProbesOptions{
		Info:     info,
		Instance: instance,
		Env:      cfg.Env.String(),
		Logger:   logger,
		ConfigFn: cfg.Redact,
	})
	probes.SetSchemaVersion(expectedSchema)
	probes.Register(db.HealthChecker{
		Pool:           pool,
		ExpectedSchema: expectedSchema,
	})

	handler := server.New(server.Options{Config: cfg, Logger: logger, Probes: probes})

	srv := httpx.NewServer(httpx.ServerOptions{
		Config:  cfg.HTTP,
		Handler: handler,
		Logger:  logger,
		Drainer: probes,
	})

	// Ready only once every dependency has been verified. Marking ready earlier
	// would let Kubernetes route traffic to a pod that cannot serve it.
	probes.MarkReady()
	logger.Info("ready", slog.String("addr", cfg.HTTP.Addr))

	if err := srv.Run(ctx); err != nil {
		return err
	}
	logger.Info("stopped")
	return nil
}
