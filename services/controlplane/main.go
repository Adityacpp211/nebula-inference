// Command nebula-controlplane is NEBULA's admin API and the only writer of
// desired state.
//
// Phase 2 scope: the model registry, credentials, and deployment records with
// their state machine. It records desired state and never touches Kubernetes —
// reconciliation arrives in Phase 5 (docs/roadmap.md), and every response that
// describes unreconciled state says so.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"log/slog"

	"github.com/adityasatwar321/nebula/migrations"
	"github.com/adityasatwar321/nebula/packages/artifact"
	"github.com/adityasatwar321/nebula/packages/config"
	"github.com/adityasatwar321/nebula/packages/db"
	"github.com/adityasatwar321/nebula/packages/db/migrate"
	"github.com/adityasatwar321/nebula/packages/httpx"
	"github.com/adityasatwar321/nebula/packages/telemetry"
	"github.com/adityasatwar321/nebula/packages/version"
	"github.com/adityasatwar321/nebula/services/controlplane/internal/api"
	"github.com/adityasatwar321/nebula/services/controlplane/internal/seed"
	"github.com/adityasatwar321/nebula/services/controlplane/internal/server"
	"github.com/adityasatwar321/nebula/services/controlplane/internal/store"
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

	st := store.New(pool)

	// The API is built before the server so a missing or weak key pepper fails
	// startup rather than every request.
	cpAPI, err := api.New(cfg, logger, st)
	if err != nil {
		return err
	}

	if cpAPI.Verifier != nil {
		// The verifier completes finalize for store-backed versions. It resumes any
		// verification a previous process was interrupted in.
		go cpAPI.Verifier.Run(ctx)
		probes.Register(telemetry.CheckFunc{
			CheckName: "artifact_store", IsCritical: false, CheckTimeout: 2 * time.Second,
			Fn: func(ctx context.Context) error {
				if p, ok := cpAPI.Artifacts.(interface{ Ping(context.Context) error }); ok {
					return p.Ping(ctx)
				}
				return nil
			},
		})
		logger.Info("artifact store configured", slog.String("store", cfg.Artifact.Store))
		if s3, ok := cpAPI.Artifacts.(*artifact.S3); ok && cfg.Dev.CreateBucket && !cfg.Env.IsProduction() {
			// Development only. In the background and retried: the store may still be
			// starting, and the control plane must not wait on it to serve.
			go func() {
				for {
					cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
					err := s3.EnsureBucket(cctx)
					cancel()
					if err == nil {
						logger.Info("development: artifact bucket present", slog.String("bucket", cfg.Artifact.S3Bucket))
						return
					}
					select {
					case <-ctx.Done():
						return
					case <-time.After(3 * time.Second):
					}
				}
			}()
		}
	}

	if result, err := seed.Run(ctx, cfg, st, cpAPI.Hasher, logger); err != nil {
		// A failed seed fails startup. The alternative is a developer debugging a
		// 401 against fixtures that were never created.
		return fmt.Errorf("seeding development fixtures: %w", err)
	} else if result != nil {
		logger.Info("development seed complete",
			slog.String("org_id", result.OrgID),
			slog.String("key_prefix", result.Prefix))
	}

	handler := server.New(server.Options{
		Config: cfg,
		Logger: logger,
		Probes: probes,
		API:    cpAPI,
	})

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
