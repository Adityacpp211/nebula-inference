// Command nebula-controller reconciles NEBULA's desired state into Kubernetes.
//
// It reads deployments from PostgreSQL (connecting as nebula_controller, whose
// grants allow only OBSERVED columns), applies worker Deployments, Services and
// PDBs in the workload namespace, writes status back, keeps the node inventory,
// and records worker events. One replica is active at a time, chosen by a
// Kubernetes Lease (docs/architecture.md §4.3).
//
// Its failure behaviour is the documented one (docs/architecture.md §8.4): with
// PostgreSQL down it stops reconciling and deletes nothing; with the Kubernetes API
// down it stops changing things and says so; workers keep serving either way.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/adityasatwar321/nebula/migrations"
	"github.com/adityasatwar321/nebula/packages/config"
	"github.com/adityasatwar321/nebula/packages/db"
	"github.com/adityasatwar321/nebula/packages/db/migrate"
	"github.com/adityasatwar321/nebula/packages/httpx"
	"github.com/adityasatwar321/nebula/packages/k8s"
	"github.com/adityasatwar321/nebula/packages/telemetry"
	"github.com/adityasatwar321/nebula/packages/version"
	"github.com/adityasatwar321/nebula/services/controller/internal/deploymentctrl"
	"github.com/adityasatwar321/nebula/services/controller/internal/run"
	"github.com/adityasatwar321/nebula/services/controller/internal/store"
)

const serviceName = config.ServiceController

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
		Defaults: map[string]string{"NEBULA_HTTP_ADDR": ":8083"},
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
		slog.String("workload_namespace", cfg.Kube.WorkloadNamespace))

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

	restCfg, err := k8s.RestConfig(cfg.Kube.Kubeconfig)
	if err != nil {
		return err
	}
	kube, err := k8s.NewClient(restCfg, serviceName+"/"+info.Version)
	if err != nil {
		return err
	}

	st := store.New(pool)
	reconciler := &deploymentctrl.Reconciler{
		Kube:  kube,
		Store: st,
		Settings: deploymentctrl.Settings{
			Namespace:          cfg.Kube.WorkloadNamespace,
			Env:                cfg.Env.String(),
			WorkerImage:        cfg.Kube.WorkerImage,
			MockWorkerImage:    cfg.Kube.MockWorkerImage,
			PullerImage:        cfg.Kube.PullerImage,
			ImagePullPolicy:    corev1.PullPolicy(cfg.Kube.ImagePullPolicy),
			CacheHostPath:      cfg.Kube.ArtifactCacheHostPath,
			ArtifactSecretName: cfg.Kube.ArtifactSecretName,
			StartingTimeoutSec: int32(cfg.Kube.StartingTimeout.Duration().Seconds()),
			MaxArtifactBytes:   cfg.Artifact.MaxBytes,
			NATSURL:            cfg.NATS.URL,
			OTLPEndpoint:       cfg.Telemetry.OTLPEndpoint,
		},
		StartingTimeout: cfg.Kube.StartingTimeout.Duration(),
		Logger:          logger,
	}
	stopTracing, err := telemetry.SetupTracing(ctx, telemetry.TracingOptions{
		Endpoint: cfg.Telemetry.OTLPEndpoint, SampleRatio: cfg.Telemetry.TraceSampleRatio,
		Service: serviceName, Version: info.Version, Instance: instance,
	})
	if err != nil {
		return err
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = stopTracing(sctx)
	}()
	metrics := telemetry.NewMetrics()
	metrics.Gauge("nebula_schema_version").WithLabelValues(serviceName).Set(float64(expected))
	if cfg.Telemetry.MetricsAddr != "" {
		go telemetry.ServeMetrics(ctx, cfg.Telemetry.MetricsAddr, metrics, logger)
	}
	ctrl := run.New(run.Options{
		Metrics: metrics,
		Kube:    kube, Store: st, Reconciler: reconciler,
		Namespace: cfg.Kube.WorkloadNamespace, SystemNamespace: cfg.Kube.SystemNamespace,
		Identity: instance, LeaderElection: cfg.Kube.LeaderElection,
		LeaseDuration: cfg.Kube.LeaseDuration.Duration(), RenewDeadline: cfg.Kube.RenewDeadline.Duration(),
		RetryPeriod:    cfg.Kube.RetryPeriod.Duration(),
		ResyncInterval: cfg.Kube.ResyncInterval.Duration(), PollInterval: cfg.Kube.PollInterval.Duration(),
		Logger: logger,
	})

	probes := telemetry.NewProbes(telemetry.ProbesOptions{
		Info: info, Instance: instance, Env: cfg.Env.String(), Logger: logger, ConfigFn: cfg.Redact,
	})
	probes.SetSchemaVersion(expected)
	probes.Register(db.HealthChecker{Pool: pool, ExpectedSchema: expected})
	// A standby replica is ready: it is doing exactly its job, which is waiting.
	// Leadership and cache state are reported in /healthz for operators.
	probes.Register(telemetry.CheckFunc{CheckName: "kubernetes_api", IsCritical: false, CheckTimeout: 2 * time.Second,
		Fn: func(context.Context) error {
			_, err := kube.Discovery().ServerVersion()
			return err
		}})
	probes.Register(telemetry.CheckFunc{CheckName: "leadership", IsCritical: false,
		Fn: func(context.Context) error {
			if !ctrl.IsLeader() {
				return errors.New("standby: another replica holds the lease")
			}
			if !ctrl.Synced() {
				return errors.New("leader, informer caches still syncing")
			}
			return nil
		}})

	mux := probes.Handler()
	srv := httpx.NewServer(httpx.ServerOptions{Config: cfg.HTTP, Handler: mux, Logger: logger, Drainer: probes})
	probes.MarkReady()

	errc := make(chan error, 2)
	go func() { errc <- srv.Run(ctx) }()
	go func() { errc <- ctrl.Run(ctx) }()
	logger.Info("ready", slog.String("probes", cfg.HTTP.Addr), slog.Bool("leader_election", cfg.Kube.LeaderElection))

	err = <-errc
	stop()
	if err != nil {
		logger.Error("stopping", slog.String("cause", err.Error()))
		return err
	}
	logger.Info("stopped")
	return nil
}
