// Command nebula-gateway is NEBULA's data plane: the only public entry point.
//
// Phase 4 scope: API-key authentication against the control plane, Redis-backed
// rate limiting with an in-process fallback, the OpenAI-compatible inference API
// over a static route table, SSE streaming with keep-alives and terminal error
// frames, client-disconnect cancellation, usage records, and the admin proxy to
// the control plane. Dynamic routing is Phase 6 and the admission queue Phase 7
// (docs/roadmap.md).
//
// The gateway holds no database credential and imports no database package
// (docs/repository-structure.md §4, rule 3). Its only hard dependency is the
// workers it dispatches to (docs/components.md §2.1): Redis and the control plane
// are soft, with degradation paths documented where they are implemented.
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

	"github.com/redis/go-redis/v9"

	"github.com/adityasatwar321/nebula/packages/auth"
	"github.com/adityasatwar321/nebula/packages/config"
	"github.com/adityasatwar321/nebula/packages/httpx"
	"github.com/adityasatwar321/nebula/packages/telemetry"
	"github.com/adityasatwar321/nebula/packages/version"
	"github.com/adityasatwar321/nebula/services/gateway/internal/adminproxy"
	"github.com/adityasatwar321/nebula/services/gateway/internal/controlplane"
	"github.com/adityasatwar321/nebula/services/gateway/internal/credentials"
	"github.com/adityasatwar321/nebula/services/gateway/internal/dispatch"
	"github.com/adityasatwar321/nebula/services/gateway/internal/ratelimit"
	"github.com/adityasatwar321/nebula/services/gateway/internal/routes"
	"github.com/adityasatwar321/nebula/services/gateway/internal/server"
	"github.com/adityasatwar321/nebula/services/gateway/internal/usage"
)

const serviceName = config.ServiceGateway

func main() {
	if err := run(); err != nil {
		if errors.Is(err, config.ErrHelpRequested) {
			os.Exit(0)
		}
		fmt.Fprintf(os.Stderr, "%s: %v\n", serviceName, err)
		os.Exit(1)
	}
}

func run() error {
	info := version.Get(serviceName)

	cfg, err := config.Loader{
		Service: serviceName,
		Args:    os.Args[1:],
		// The gateway is the public edge; the control plane keeps :8082.
		Defaults: map[string]string{"NEBULA_HTTP_ADDR": ":8080"},
	}.Load()
	if err != nil {
		return err
	}
	if err := cfg.RequireAuth(); err != nil {
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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	hasher, err := auth.NewHasher(cfg.Auth.KeyPepper.Reveal())
	if err != nil {
		return err
	}
	signer, err := auth.NewContextSigner(cfg.Internal.AuthSecret.Reveal(), cfg.Internal.AuthMaxAge.Duration(), nil)
	if err != nil {
		return err
	}

	table := routes.Empty()
	if cfg.Gateway.RoutesFile != "" {
		table, err = routes.Load(cfg.Gateway.RoutesFile, time.Now())
		if err != nil {
			return err
		}
	}
	logger.Info("route table loaded", slog.Int("routes", table.Len()), slog.String("source", "static"),
		slog.String("file", cfg.Gateway.RoutesFile))

	var rdb redis.UniversalClient
	if !cfg.Redis.URL.IsZero() {
		opts, err := redis.ParseURL(cfg.Redis.URL.Reveal())
		if err != nil {
			return fmt.Errorf("NEBULA_REDIS_URL: %w", err)
		}
		opts.DialTimeout = cfg.Redis.DialTimeout.Duration()
		opts.ReadTimeout = cfg.Redis.OpTimeout.Duration()
		opts.WriteTimeout = cfg.Redis.OpTimeout.Duration()
		opts.PoolSize = cfg.Redis.PoolSize
		// A request must never wait on Redis for longer than one operation's budget;
		// retrying inside the client would multiply it.
		opts.MaxRetries = 0
		opts.ContextTimeoutEnabled = true
		client := redis.NewClient(opts)
		defer func() { _ = client.Close() }()
		rdb = client
		pctx, cancel := context.WithTimeout(ctx, cfg.Redis.DialTimeout.Duration())
		if err := client.Ping(pctx).Err(); err != nil {
			// Not fatal: Redis is a soft dependency. Starting degraded is better than
			// not starting, and the limiter recovers on its own when Redis returns.
			logger.Warn("redis is unreachable at startup; starting with in-process fallbacks",
				slog.String("cause", err.Error()))
		}
		cancel()
	} else {
		logger.Warn("NEBULA_REDIS_URL is not set: rate limits are per replica and approximate")
	}

	cp, err := controlplane.New(cfg.Gateway.ControlPlaneURL, signer, cfg.Gateway.ControlPlaneTimeout.Duration(), nil)
	if err != nil {
		return err
	}

	resolver := credentials.New(credentials.Options{
		Hasher:      hasher,
		Lookup:      cp,
		Redis:       rdb,
		RedisPrefix: cfg.Redis.KeyPrefix,
		RedisTTL:    cfg.Auth.KeyCacheTTL.Duration(),
		OpTimeout:   cfg.Redis.OpTimeout.Duration(),
		LocalTTL:    cfg.Gateway.LocalKeyCacheTTL.Duration(),
		NegativeTTL: cfg.Gateway.NegativeKeyCacheTTL.Duration(),
		StaleGrace:  cfg.Gateway.StaleKeyGrace.Duration(),
		MaxEntries:  cfg.Auth.KeyCacheSize,
		Logger:      logger,
	})

	limiter := ratelimit.New(ratelimit.Options{
		Redis:            rdb,
		KeyPrefix:        cfg.Redis.KeyPrefix,
		OpTimeout:        cfg.Redis.OpTimeout.Duration(),
		FallbackFraction: cfg.Limits.FallbackFraction,
		Logger:           logger,
	})

	probes := telemetry.NewProbes(telemetry.ProbesOptions{
		Info:     info,
		Instance: instance,
		Env:      cfg.Env.String(),
		Logger:   logger,
		ConfigFn: cfg.Redact,
	})
	// Neither check is critical. Redis and the control plane are soft
	// dependencies, and marking the gateway unready when one is down would take
	// inference down with them — the opposite of axiom A8. They are reported in
	// /healthz so an operator sees the degradation.
	if rdb != nil {
		probes.Register(telemetry.CheckFunc{
			CheckName: "redis", IsCritical: false, CheckTimeout: time.Second,
			Fn: func(ctx context.Context) error { return rdb.Ping(ctx).Err() },
		})
	}

	proxy := adminproxy.New(adminproxy.Options{
		Target:     cp.BaseURL(),
		Signer:     signer,
		Issuer:     serviceName,
		Logger:     logger,
		Invalidate: resolver.Invalidate,
	})

	handler := server.New(server.Deps{
		Config:  cfg,
		Logger:  logger,
		Probes:  probes,
		Auth:    resolver,
		Limiter: limiter,
		Routes:  table,
		Workers: dispatch.New(dispatch.Options{}),
		Proxy:   proxy,
		Usage:   usage.LogSink{Logger: logger},
	})

	// Streams outlive the server's WriteTimeout by design; the gateway sets a
	// per-write deadline on every frame instead (server/stream.go). The server-wide
	// timeout still bounds every non-streaming response.
	srv := httpx.NewServer(httpx.ServerOptions{
		Config:  cfg.HTTP,
		Handler: handler,
		Logger:  logger,
		Drainer: probes,
	})

	probes.MarkReady()
	logger.Info("ready", slog.String("addr", cfg.HTTP.Addr),
		slog.String("controlplane", cfg.Gateway.ControlPlaneURL),
		slog.Bool("redis", rdb != nil))

	if err := srv.Run(ctx); err != nil {
		return err
	}
	logger.Info("stopped")
	return nil
}
