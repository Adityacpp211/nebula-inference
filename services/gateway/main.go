// Command nebula-gateway is NEBULA's data plane: the only public entry point.
//
// It authenticates API keys against the control plane, rate-limits in Redis with
// an in-process fallback, serves the OpenAI-compatible inference API with SSE
// streaming and client-disconnect cancellation, emits usage records, proxies the
// admin API, and routes every request through the router: routes from the
// control plane (snapshotted to Redis for cold starts), endpoints from
// EndpointSlices, load from worker heartbeats over NATS. The admission queue is
// Phase 7 (docs/roadmap.md).
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
	"github.com/adityasatwar321/nebula/packages/k8s"
	"github.com/adityasatwar321/nebula/packages/routing"
	"github.com/adityasatwar321/nebula/packages/telemetry"
	"github.com/adityasatwar321/nebula/packages/version"
	"github.com/adityasatwar321/nebula/services/gateway/internal/adminproxy"
	"github.com/adityasatwar321/nebula/services/gateway/internal/controlplane"
	"github.com/adityasatwar321/nebula/services/gateway/internal/credentials"
	"github.com/adityasatwar321/nebula/services/gateway/internal/dispatch"
	"github.com/adityasatwar321/nebula/services/gateway/internal/ratelimit"
	"github.com/adityasatwar321/nebula/services/gateway/internal/router"
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

	// ROUTING. Three sources feed the router; each is optional and each degrades
	// to the next rather than failing the gateway (docs/architecture.md §6.2).
	var heartbeats *router.Heartbeats
	rtOpts := router.Options{
		StaleAfter: cfg.Gateway.HeartbeatStaleAfter.Duration(),
		Breaker: routing.BreakerConfig{Threshold: cfg.Gateway.BreakerThreshold,
			Cooldown: cfg.Gateway.BreakerCooldown.Duration(), MaxCooldown: 30 * time.Second},
		Logger: logger,
	}
	if cfg.NATS.URL != "" {
		heartbeats = &router.Heartbeats{URL: cfg.NATS.URL, Logger: logger, Settle: cfg.Gateway.HeartbeatStaleAfter.Duration(),
			Also: map[string]func([]byte){
				// A revocation another replica proxied: drop it from this replica's
				// in-process cache now rather than when its TTL runs out.
				credentials.RevokedSubject: func(b []byte) { resolver.InvalidateLocal(string(b)) },
			}}
		rtOpts.HeartbeatsLive = heartbeats.Live
	}
	rt := router.New(rtOpts)

	routeSource := "controlplane"
	var tables *router.TableSource
	if cfg.Gateway.RoutesFile != "" {
		routeSource = "static"
		table, err := routes.Load(cfg.Gateway.RoutesFile, time.Now())
		if err != nil {
			return err
		}
		rt.SetTable(table)
		logger.Info("route table loaded", slog.Int("routes", table.Len()), slog.String("source", routeSource),
			slog.String("file", cfg.Gateway.RoutesFile))
	} else {
		tables = &router.TableSource{
			Fetch: cp.RoutingTable, NotModified: controlplane.ErrNotModified,
			Router: rt, Interval: cfg.Gateway.RoutesRefresh.Duration(), Logger: logger,
		}
		if rdb != nil {
			tables.Snapshots = router.RedisSnapshots{Client: rdb, Key: cfg.Redis.KeyPrefix + "routing:snapshot",
				OpTimeout: cfg.Redis.OpTimeout.Duration()}
		}
		go tables.Run(ctx)
		probes.Register(telemetry.CheckFunc{
			CheckName: "routing_table", IsCritical: false, CheckTimeout: time.Second,
			Fn: func(context.Context) error {
				switch {
				case !tables.Synced.Load():
					return errors.New("no routing table yet: the control plane has not answered and there is no snapshot")
				case tables.FromSnapshot.Load():
					return errors.New("serving the Redis snapshot: the control plane is unreachable")
				}
				return nil
			},
		})
	}

	var slices *router.EndpointSlices
	if cfg.Gateway.Endpoints == "kubernetes" {
		restCfg, err := k8s.RestConfig(cfg.Kube.Kubeconfig)
		if err != nil {
			return err
		}
		kube, err := k8s.NewClient(restCfg, serviceName+"/"+info.Version)
		if err != nil {
			return err
		}
		slices = &router.EndpointSlices{Client: kube, Namespace: cfg.Kube.WorkloadNamespace, Router: rt,
			Logger: logger, Resync: 5 * time.Minute}
		go func() {
			if err := slices.Run(ctx); err != nil && ctx.Err() == nil {
				logger.Error("endpoint discovery stopped", slog.String("cause", err.Error()))
			}
		}()
		probes.Register(telemetry.CheckFunc{
			CheckName: "endpoint_discovery", IsCritical: false, CheckTimeout: time.Second,
			Fn: func(context.Context) error {
				if !slices.Synced() {
					return errors.New("EndpointSlices not synced yet")
				}
				return nil
			},
		})
	}
	if heartbeats != nil {
		heartbeats.Router = rt
		go func() {
			if err := heartbeats.Run(ctx); err != nil && ctx.Err() == nil {
				logger.Error("heartbeat subscription stopped", slog.String("cause", err.Error()))
			}
		}()
		probes.Register(telemetry.CheckFunc{
			CheckName: "heartbeats", IsCritical: false, CheckTimeout: time.Second,
			Fn: func(context.Context) error {
				if !heartbeats.Live() {
					return errors.New("not receiving heartbeats: routing on readiness and local observation")
				}
				return nil
			},
		})
	}

	proxy := adminproxy.New(adminproxy.Options{
		Target: cp.BaseURL(),
		Signer: signer,
		Issuer: serviceName,
		Logger: logger,
		Invalidate: func(ctx context.Context, prefix string) {
			resolver.Invalidate(ctx, prefix)
			if heartbeats != nil {
				if err := heartbeats.Publish(credentials.RevokedSubject, []byte(prefix)); err != nil {
					logger.Warn("revocation broadcast failed; other replicas evict the key when its local TTL ends",
						slog.String("cause", err.Error()))
				}
			}
		},
	})

	handler := server.New(server.Deps{
		Config:      cfg,
		Logger:      logger,
		Probes:      probes,
		Auth:        resolver,
		Limiter:     limiter,
		Router:      rt,
		RouteSource: routeSource,
		Workers:     dispatch.New(dispatch.Options{}),
		Proxy:       proxy,
		Usage:       usage.LogSink{Logger: logger},
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
		slog.Bool("redis", rdb != nil), slog.String("routes", routeSource),
		slog.String("endpoints", cfg.Gateway.Endpoints), slog.Bool("heartbeats", heartbeats != nil))

	if err := srv.Run(ctx); err != nil {
		return err
	}
	logger.Info("stopped")
	return nil
}
