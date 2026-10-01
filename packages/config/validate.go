package config

import (
	"fmt"
	"net"
	"net/url"
	"sort"
	"strings"
	"time"
)

// FieldError is one problem with one configuration field.
type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

func (e FieldError) Error() string { return e.Field + ": " + e.Message }

// ValidationErrors aggregates every configuration problem found.
//
// Aggregating rather than returning the first error is the whole point: an
// operator fixing a misconfigured deploy should see all of it in one restart,
// not discover the next problem after each fix.
type ValidationErrors struct {
	Errors []FieldError `json:"errors"`
}

// Add records a problem.
func (v *ValidationErrors) Add(field, format string, args ...any) {
	v.Errors = append(v.Errors, FieldError{Field: field, Message: fmt.Sprintf(format, args...)})
}

// AddIf records a problem only when cond is true.
func (v *ValidationErrors) AddIf(cond bool, field, format string, args ...any) {
	if cond {
		v.Add(field, format, args...)
	}
}

// Len returns the number of problems.
func (v *ValidationErrors) Len() int { return len(v.Errors) }

func (v *ValidationErrors) Error() string {
	if len(v.Errors) == 0 {
		return "invalid configuration"
	}
	sorted := make([]FieldError, len(v.Errors))
	copy(sorted, v.Errors)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Field < sorted[j].Field })

	var b strings.Builder
	fmt.Fprintf(&b, "invalid configuration (%d problem", len(sorted))
	if len(sorted) != 1 {
		b.WriteString("s")
	}
	b.WriteString("):")
	for _, e := range sorted {
		fmt.Fprintf(&b, "\n  - %s: %s", e.Field, e.Message)
	}
	return b.String()
}

// Validate checks the whole configuration and returns every problem at once.
func (c *Config) Validate() error {
	var v ValidationErrors

	if !c.Env.Valid() {
		v.Add("NEBULA_ENV", "must be one of dev, staging, production (got %q)", c.Env)
	}

	// --- logging ---
	if !c.Log.LogLevelValid() {
		v.Add("NEBULA_LOG_LEVEL", "must be one of debug, info, warn, error (got %q)", c.Log.Level)
	}
	switch strings.ToLower(c.Log.Format) {
	case "json", "text":
	default:
		v.Add("NEBULA_LOG_FORMAT", "must be json or text (got %q)", c.Log.Format)
	}

	// --- http ---
	if c.HTTP.Addr == "" {
		v.Add("NEBULA_HTTP_ADDR", "must not be empty")
	} else if _, _, err := net.SplitHostPort(c.HTTP.Addr); err != nil {
		v.Add("NEBULA_HTTP_ADDR", "must be host:port or :port (got %q)", c.HTTP.Addr)
	}
	v.AddIf(c.HTTP.ReadHeaderTimeout.Duration() <= 0, "NEBULA_HTTP_READ_HEADER_TIMEOUT", "must be positive")
	v.AddIf(c.HTTP.ReadTimeout.Duration() <= 0, "NEBULA_HTTP_READ_TIMEOUT", "must be positive")
	v.AddIf(c.HTTP.WriteTimeout.Duration() <= 0, "NEBULA_HTTP_WRITE_TIMEOUT", "must be positive")
	v.AddIf(c.HTTP.IdleTimeout.Duration() <= 0, "NEBULA_HTTP_IDLE_TIMEOUT", "must be positive")
	v.AddIf(c.HTTP.ShutdownGrace.Duration() <= 0, "NEBULA_HTTP_SHUTDOWN_GRACE", "must be positive")
	v.AddIf(c.HTTP.MaxHeaderBytes <= 0, "NEBULA_HTTP_MAX_HEADER_BYTES", "must be positive")
	v.AddIf(c.HTTP.MaxBodyBytes <= 0, "NEBULA_HTTP_MAX_BODY_BYTES", "must be positive")
	v.AddIf(c.HTTP.DrainDelay.Duration() >= c.HTTP.ShutdownGrace.Duration() && c.HTTP.ShutdownGrace.Duration() > 0,
		"NEBULA_HTTP_DRAIN_DELAY",
		"must be shorter than NEBULA_HTTP_SHUTDOWN_GRACE (%s), or in-flight requests get no time to finish",
		c.HTTP.ShutdownGrace)

	// --- database ---
	// The gateway imports no database package at all
	// (docs/repository-structure.md §4, rule 3), so requiring a URL it would never
	// open would only teach operators to hand the data plane a credential.
	if c.usesDatabase() {
		if c.Database.URL.IsZero() {
			v.Add("NEBULA_DATABASE_URL", "is required")
		} else if !plausibleDSN(c.Database.URL.Reveal()) {
			v.Add("NEBULA_DATABASE_URL", "must be a postgres:// URL or a libpq keyword/value string")
		}
	} else if !c.Database.URL.IsZero() {
		v.Add("NEBULA_DATABASE_URL",
			"must not be set for %s: it has no database access by design, and a credential it never uses is one more to leak",
			c.service)
	}
	v.AddIf(c.Database.MaxConns < 1, "NEBULA_DATABASE_MAX_CONNS", "must be at least 1")
	v.AddIf(c.Database.MinConns < 0, "NEBULA_DATABASE_MIN_CONNS", "must not be negative")
	v.AddIf(c.Database.MinConns > c.Database.MaxConns,
		"NEBULA_DATABASE_MIN_CONNS", "must not exceed NEBULA_DATABASE_MAX_CONNS (%d)", c.Database.MaxConns)
	v.AddIf(c.Database.ConnectTimeout.Duration() <= 0, "NEBULA_DATABASE_CONNECT_TIMEOUT", "must be positive")
	v.AddIf(c.Database.StatementTimeout.Duration() <= 0, "NEBULA_DATABASE_STATEMENT_TIMEOUT", "must be positive")

	// --- auth ---
	// The pepper is optional at this layer because nebula-migrate does not
	// authenticate anyone. A service that does calls RequireAuth() at startup.
	if !c.Auth.KeyPepper.IsZero() && len(c.Auth.KeyPepper.Reveal()) < 32 {
		v.Add("NEBULA_AUTH_KEY_PEPPER", "must be at least 32 bytes (generate with: openssl rand -base64 32)")
	}
	v.AddIf(c.Auth.KeyCacheTTL.Duration() <= 0, "NEBULA_AUTH_KEY_CACHE_TTL", "must be positive")
	v.AddIf(c.Auth.KeyCacheTTL.Duration() > 5*time.Minute, "NEBULA_AUTH_KEY_CACHE_TTL",
		"must not exceed 5m: until revocation invalidation exists, this TTL is the whole revocation guarantee")
	v.AddIf(c.Auth.KeyCacheSize < 1, "NEBULA_AUTH_KEY_CACHE_SIZE", "must be at least 1")

	// --- internal service authentication ---
	if !c.Internal.AuthSecret.IsZero() && len(c.Internal.AuthSecret.Reveal()) < 32 {
		v.Add("NEBULA_INTERNAL_AUTH_SECRET", "must be at least 32 bytes (generate with: openssl rand -base64 32)")
	}
	v.AddIf(c.Internal.AuthMaxAge.Duration() <= 0, "NEBULA_INTERNAL_AUTH_MAX_AGE", "must be positive")
	v.AddIf(c.Internal.AuthMaxAge.Duration() > time.Minute, "NEBULA_INTERNAL_AUTH_MAX_AGE",
		"must not exceed 1m: it is the replay window for a signed identity")

	// --- redis ---
	if !c.Redis.URL.IsZero() {
		u := c.Redis.URL.Reveal()
		if !strings.HasPrefix(u, "redis://") && !strings.HasPrefix(u, "rediss://") {
			v.Add("NEBULA_REDIS_URL", "must be a redis:// or rediss:// URL")
		}
	}
	v.AddIf(c.Redis.DialTimeout.Duration() <= 0, "NEBULA_REDIS_DIAL_TIMEOUT", "must be positive")
	v.AddIf(c.Redis.OpTimeout.Duration() <= 0, "NEBULA_REDIS_OP_TIMEOUT", "must be positive")
	v.AddIf(c.Redis.PoolSize < 1, "NEBULA_REDIS_POOL_SIZE", "must be at least 1")

	// --- artifact store ---
	switch c.Artifact.Store {
	case "none":
	case "s3":
		v.AddIf(c.Artifact.S3Endpoint == "", "NEBULA_ARTIFACT_S3_ENDPOINT", "is required when NEBULA_ARTIFACT_STORE=s3")
		v.AddIf(strings.Contains(c.Artifact.S3Endpoint, "://"), "NEBULA_ARTIFACT_S3_ENDPOINT",
			"must be host:port without a scheme; set NEBULA_ARTIFACT_S3_USE_TLS for https")
		v.AddIf(c.Artifact.S3Bucket == "", "NEBULA_ARTIFACT_S3_BUCKET", "is required when NEBULA_ARTIFACT_STORE=s3")
		v.AddIf(c.Artifact.S3AccessKey.IsZero() || c.Artifact.S3SecretKey.IsZero(), "NEBULA_ARTIFACT_S3_ACCESS_KEY",
			"and NEBULA_ARTIFACT_S3_SECRET_KEY are required when NEBULA_ARTIFACT_STORE=s3")
	case "file":
		v.AddIf(c.Artifact.Dir == "", "NEBULA_ARTIFACT_DIR", "is required when NEBULA_ARTIFACT_STORE=file")
	default:
		v.Add("NEBULA_ARTIFACT_STORE", "must be s3, file or none (got %q)", c.Artifact.Store)
	}
	v.AddIf(c.Artifact.MaxBytes <= 0, "NEBULA_ARTIFACT_MAX_BYTES", "must be positive")
	v.AddIf(c.Artifact.PresignTTL.Duration() < time.Minute || c.Artifact.PresignTTL.Duration() > 7*24*time.Hour,
		"NEBULA_ARTIFACT_PRESIGN_TTL", "must be between 1m and 168h (the S3 maximum)")
	v.AddIf(c.Artifact.VerifyInterval.Duration() <= 0, "NEBULA_ARTIFACT_VERIFY_INTERVAL", "must be positive")

	if c.service == ServiceGateway {
		c.validateGateway(&v)
	}
	if c.service == ServiceController {
		k := c.Kube
		v.AddIf(k.WorkloadNamespace == "", "NEBULA_KUBE_WORKLOAD_NAMESPACE", "must not be empty")
		v.AddIf(k.SystemNamespace == "", "NEBULA_KUBE_SYSTEM_NAMESPACE", "must not be empty")
		v.AddIf(k.WorkloadNamespace == k.SystemNamespace, "NEBULA_KUBE_WORKLOAD_NAMESPACE",
			"must differ from NEBULA_KUBE_SYSTEM_NAMESPACE: the controller may write workloads but never NEBULA itself")
		v.AddIf(k.RenewDeadline.Duration() >= k.LeaseDuration.Duration(), "NEBULA_KUBE_RENEW_DEADLINE",
			"must be shorter than NEBULA_KUBE_LEASE_DURATION")
		v.AddIf(k.RetryPeriod.Duration() <= 0 || k.RetryPeriod.Duration() >= k.RenewDeadline.Duration(),
			"NEBULA_KUBE_RETRY_PERIOD", "must be positive and shorter than NEBULA_KUBE_RENEW_DEADLINE")
		v.AddIf(k.ResyncInterval.Duration() < time.Second, "NEBULA_KUBE_RESYNC_INTERVAL", "must be at least 1s")
		v.AddIf(k.PollInterval.Duration() <= 0, "NEBULA_KUBE_POLL_INTERVAL", "must be positive")
		v.AddIf(k.StartingTimeout.Duration() < time.Minute, "NEBULA_KUBE_STARTING_TIMEOUT", "must be at least 1m")
		for name, img := range map[string]string{
			"NEBULA_KUBE_WORKER_IMAGE": k.WorkerImage, "NEBULA_KUBE_MOCK_WORKER_IMAGE": k.MockWorkerImage,
			"NEBULA_KUBE_PULLER_IMAGE": k.PullerImage,
		} {
			if img == "" || !strings.Contains(img, ":") || strings.HasSuffix(img, ":latest") {
				v.Add(name, "must name an image with an explicit tag, never latest (got %q)", img)
			}
		}
		switch k.ImagePullPolicy {
		case "Always", "IfNotPresent", "Never":
		default:
			v.Add("NEBULA_KUBE_IMAGE_PULL_POLICY", "must be Always, IfNotPresent or Never")
		}
	}

	// --- production gating -------------------------------------------------
	// Every development affordance is a startup FAILURE in production, not a
	// warning. See docs/architecture.md axiom A9 and risk R-17.
	if c.Env.IsProduction() {
		v.AddIf(c.Dev.Seed, "NEBULA_DEV_SEED", "must be false when NEBULA_ENV=production")
		v.AddIf(c.Dev.MockRuntime, "NEBULA_DEV_MOCK_RUNTIME",
			"must be false when NEBULA_ENV=production: the mock runtime is a declared development stub")
		v.AddIf(c.Dev.DebugEndpoints, "NEBULA_DEV_DEBUG_ENDPOINTS", "must be false when NEBULA_ENV=production")
		v.AddIf(!c.Dev.SeedKey.IsZero(), "NEBULA_DEV_SEED_KEY", "must not be set when NEBULA_ENV=production")
		v.AddIf(c.Dev.CreateBucket, "NEBULA_DEV_CREATE_BUCKET", "must be false when NEBULA_ENV=production: create the bucket at install time")
		v.AddIf(!strings.EqualFold(c.Log.Format, "json"), "NEBULA_LOG_FORMAT",
			"must be json when NEBULA_ENV=production: text logs are not machine-parseable")
		if cred, found := defaultCredential(c.Database.URL.Reveal()); found {
			v.Add("NEBULA_DATABASE_URL",
				"contains a well-known development credential (%s); refusing to start in production", cred)
		}
		v.AddIf(c.Auth.KeyPepper.IsZero(), "NEBULA_AUTH_KEY_PEPPER",
			"is required when NEBULA_ENV=production")
		v.AddIf(isDevPepper(c.Auth.KeyPepper.Reveal()), "NEBULA_AUTH_KEY_PEPPER",
			"is the well-known development value; refusing to start in production")
		v.AddIf(c.Artifact.Store == "file", "NEBULA_ARTIFACT_STORE",
			"must not be file when NEBULA_ENV=production: a directory on one host is not reachable from every node")
		v.AddIf(c.Artifact.S3SecretKey.Reveal() == "minioadmin", "NEBULA_ARTIFACT_S3_SECRET_KEY",
			"is MinIO's default; refusing to start in production")
		v.AddIf(c.Internal.AuthSecret.Reveal() == DevInternalSecret, "NEBULA_INTERNAL_AUTH_SECRET",
			"is the well-known development value; refusing to start in production")
		if c.service == ServiceGateway {
			v.AddIf(!c.Limits.Enabled, "NEBULA_LIMITS_ENABLED",
				"must be true when NEBULA_ENV=production: an unlimited gateway is a load-test affordance")
			v.AddIf(c.Redis.URL.IsZero(), "NEBULA_REDIS_URL",
				"is required when NEBULA_ENV=production: without it every replica rate-limits alone, permanently")
		}
	}

	if v.Len() > 0 {
		return &v
	}
	return nil
}

// Service names. The config package knows them only to decide which sections a
// binary needs; it holds no other per-service behaviour.
const (
	ServiceControlPlane = "nebula-controlplane"
	ServiceGateway      = "nebula-gateway"
	ServiceMigrate      = "nebula-migrate"
	ServiceController   = "nebula-controller"
)

// usesDatabase reports whether the service opens PostgreSQL. An unknown service
// name is assumed to, so a new binary gets the strict check until it opts out.
func (c *Config) usesDatabase() bool { return c.service != ServiceGateway }

// validateGateway checks what only the data plane needs.
func (c *Config) validateGateway(v *ValidationErrors) {
	g := c.Gateway
	if u, err := url.Parse(g.ControlPlaneURL); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		v.Add("NEBULA_GATEWAY_CONTROLPLANE_URL", "must be an absolute http:// or https:// URL (got %q)", g.ControlPlaneURL)
	}
	v.AddIf(g.ControlPlaneTimeout.Duration() <= 0, "NEBULA_GATEWAY_CONTROLPLANE_TIMEOUT", "must be positive")
	v.AddIf(g.DefaultTimeout.Duration() <= 0, "NEBULA_GATEWAY_DEFAULT_TIMEOUT", "must be positive")
	v.AddIf(g.MaxTimeout.Duration() < g.DefaultTimeout.Duration(), "NEBULA_GATEWAY_MAX_TIMEOUT",
		"must not be shorter than NEBULA_GATEWAY_DEFAULT_TIMEOUT (%s)", g.DefaultTimeout)
	v.AddIf(g.KeepAliveInterval.Duration() < time.Second, "NEBULA_GATEWAY_KEEPALIVE_INTERVAL", "must be at least 1s")
	v.AddIf(g.StreamWriteTimeout.Duration() <= 0, "NEBULA_GATEWAY_STREAM_WRITE_TIMEOUT", "must be positive")
	v.AddIf(g.CancelDrainTimeout.Duration() <= 0, "NEBULA_GATEWAY_CANCEL_DRAIN_TIMEOUT", "must be positive")
	v.AddIf(g.LocalKeyCacheTTL.Duration() <= 0, "NEBULA_GATEWAY_LOCAL_KEY_CACHE_TTL", "must be positive")
	v.AddIf(g.LocalKeyCacheTTL.Duration() > c.Auth.KeyCacheTTL.Duration(), "NEBULA_GATEWAY_LOCAL_KEY_CACHE_TTL",
		"must not exceed NEBULA_AUTH_KEY_CACHE_TTL (%s): the in-process cache sits in front of the shared one", c.Auth.KeyCacheTTL)
	v.AddIf(g.NegativeKeyCacheTTL.Duration() <= 0, "NEBULA_GATEWAY_NEGATIVE_KEY_CACHE_TTL", "must be positive")
	v.AddIf(g.StaleKeyGrace.Duration() > time.Hour, "NEBULA_GATEWAY_STALE_KEY_GRACE",
		"must not exceed 1h: past that a revoked key keeps working through a control-plane outage")
	v.AddIf(g.DefaultMaxTokens < 1, "NEBULA_GATEWAY_DEFAULT_MAX_TOKENS", "must be at least 1")
	v.AddIf(g.RoutesRefresh.Duration() < 100*time.Millisecond, "NEBULA_GATEWAY_ROUTES_REFRESH", "must be at least 100ms")
	switch g.Endpoints {
	case "static":
		v.AddIf(g.RoutesFile == "" && c.Env.IsProduction(), "NEBULA_GATEWAY_ENDPOINTS",
			"is static with no route file: routes from the control plane need kubernetes endpoints")
	case "kubernetes":
		v.AddIf(c.Kube.WorkloadNamespace == "", "NEBULA_KUBE_WORKLOAD_NAMESPACE", "must not be empty")
	default:
		v.Add("NEBULA_GATEWAY_ENDPOINTS", "must be static or kubernetes (got %q)", g.Endpoints)
	}
	v.AddIf(g.HeartbeatStaleAfter.Duration() < time.Second, "NEBULA_GATEWAY_HEARTBEAT_STALE_AFTER", "must be at least 1s")
	v.AddIf(g.BreakerThreshold < 1, "NEBULA_GATEWAY_BREAKER_THRESHOLD", "must be at least 1")
	v.AddIf(g.BreakerCooldown.Duration() <= 0, "NEBULA_GATEWAY_BREAKER_COOLDOWN", "must be positive")
	if c.NATS.URL != "" {
		if u, err := url.Parse(c.NATS.URL); err != nil || (u.Scheme != "nats" && u.Scheme != "tls") || u.Host == "" {
			v.Add("NEBULA_NATS_URL", "must be a nats:// or tls:// URL (got %q)", c.NATS.URL)
		}
	}

	if c.Internal.AuthSecret.IsZero() {
		v.Add("NEBULA_INTERNAL_AUTH_SECRET",
			"is required by nebula-gateway: it signs the identity attached to every control-plane call")
	}

	l := c.Limits
	for _, f := range []struct {
		name string
		val  int
	}{
		{"NEBULA_LIMITS_KEY_RPM", l.KeyRPM}, {"NEBULA_LIMITS_KEY_TPM", l.KeyTPM},
		{"NEBULA_LIMITS_KEY_CONCURRENCY", l.KeyConcurrency},
		{"NEBULA_LIMITS_ORG_RPM", l.OrgRPM}, {"NEBULA_LIMITS_ORG_TPM", l.OrgTPM},
		{"NEBULA_LIMITS_ORG_CONCURRENCY", l.OrgConcurrency},
	} {
		v.AddIf(f.val < 1, f.name, "must be at least 1")
	}
	v.AddIf(l.FallbackFraction <= 0 || l.FallbackFraction > 1, "NEBULA_LIMITS_FALLBACK_FRACTION",
		"must be in (0, 1]: it scales limits down while replicas count independently")
}

// DevInternalSecret is the internal auth secret used by the Makefile's local
// targets. Named so production validation can refuse it.
const DevInternalSecret = "nebula-development-internal-secret-do-not-use"

// plausibleDSN does a shape check only. Full parsing belongs to the driver; this
// exists so an obviously wrong value fails at startup with a clear message
// instead of as a connection error later.
func plausibleDSN(s string) bool {
	if strings.HasPrefix(s, "postgres://") || strings.HasPrefix(s, "postgresql://") {
		return true
	}
	// libpq keyword/value form, e.g. "host=localhost user=nebula dbname=nebula"
	return strings.Contains(s, "=") && !strings.ContainsAny(s, "\n\r")
}

// DevPepper is the pepper used by docker-compose and `make run-controlplane`. It
// is a named constant so config validation can refuse it in production rather
// than hoping nobody copies the compose file.
const DevPepper = "nebula-development-pepper-do-not-use-in-production"

func isDevPepper(p string) bool { return p == DevPepper }

// RequireAuth reports an error when the configuration lacks what a service that
// authenticates callers needs. Called explicitly by such a service at startup, so
// nebula-migrate does not have to carry a pepper it never uses.
func (c *Config) RequireAuth() error {
	if c.Auth.KeyPepper.IsZero() {
		var v ValidationErrors
		v.Add("NEBULA_AUTH_KEY_PEPPER",
			"is required by %s: it is mixed into every API key hash "+
				"(generate one with: openssl rand -base64 32)", c.service)
		return &v
	}
	return nil
}

// defaultCredential detects credentials that ship in development compose files.
// An install that would otherwise succeed with a well-known password is a silent
// security failure, so it is made loud.
func defaultCredential(dsn string) (string, bool) {
	l := strings.ToLower(dsn)
	for _, bad := range []string{
		":postgres@", "password=postgres",
		":password@", "password=password",
		":nebula@", "password=nebula",
		":changeme@", "password=changeme",
	} {
		if strings.Contains(l, bad) {
			return bad, true
		}
	}
	return "", false
}
