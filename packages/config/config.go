// Package config loads and validates service configuration from four layers,
// in increasing precedence: code defaults, a config file, environment variables,
// then command-line flags.
//
// Two rules make this package worth having rather than reading os.Getenv inline:
//
//  1. Validation reports EVERY problem at once and exits non-zero, rather than
//     failing on the first one at 3 a.m. under load (docs/deployment-architecture.md §9).
//  2. Secrets are typed (Secret) so they cannot be logged, marshalled, or
//     formatted into an error by accident.
package config

import (
	"fmt"
	"strings"
)

// Env is the deployment environment. It gates development-only behaviour.
type Env string

// Valid environments.
const (
	EnvDev        Env = "dev"
	EnvStaging    Env = "staging"
	EnvProduction Env = "production"
)

// IsProduction reports whether development affordances must be refused.
func (e Env) IsProduction() bool { return e == EnvProduction }

// Valid reports whether e is a known environment.
func (e Env) Valid() bool {
	switch e {
	case EnvDev, EnvStaging, EnvProduction:
		return true
	}
	return false
}

func (e Env) String() string { return string(e) }

// Config is the full configuration surface shared by every NEBULA service.
//
// Service-specific sections are added by the phase that introduces the service;
// Phase 1 covers what the control plane skeleton and the migrator need.
type Config struct {
	// Env gates development-only behaviour. Set with NEBULA_ENV.
	Env Env `json:"env" yaml:"env" env:"NEBULA_ENV" flag:"env" usage:"deployment environment: dev, staging, production" default:"dev"`

	Log      LogConfig      `json:"log" yaml:"log"`
	HTTP     HTTPConfig     `json:"http" yaml:"http"`
	Database DatabaseConfig `json:"database" yaml:"database"`
	Auth     AuthConfig     `json:"auth" yaml:"auth"`
	Internal InternalConfig `json:"internal" yaml:"internal"`
	Redis    RedisConfig    `json:"redis" yaml:"redis"`
	Gateway  GatewayConfig  `json:"gateway" yaml:"gateway"`
	Limits   LimitsConfig   `json:"limits" yaml:"limits"`
	Artifact ArtifactConfig `json:"artifact" yaml:"artifact"`
	Kube     KubeConfig     `json:"kube" yaml:"kube"`
	NATS     NATSConfig     `json:"nats" yaml:"nats"`
	Dev      DevConfig      `json:"dev" yaml:"dev"`

	// service is set by the binary, never by configuration.
	service string `yaml:"-"`
}

// LogConfig controls structured logging. See docs/observability.md §4.
type LogConfig struct {
	// Level is one of debug, info, warn, error.
	Level string `json:"level" yaml:"level" env:"NEBULA_LOG_LEVEL" flag:"log-level" usage:"debug, info, warn, error" default:"info"`
	// Format is json or text. text is for local development only.
	Format string `json:"format" yaml:"format" env:"NEBULA_LOG_FORMAT" flag:"log-format" usage:"json or text" default:"json"`
	// AddSource includes file:line. Useful locally, noisy in production.
	AddSource bool `json:"add_source" yaml:"add_source" env:"NEBULA_LOG_ADD_SOURCE" default:"false"`
}

// HTTPConfig controls the HTTP server. Timeouts are explicit because a server
// with no timeouts is a resource leak waiting for a slow client.
type HTTPConfig struct {
	Addr string `json:"addr" yaml:"addr" env:"NEBULA_HTTP_ADDR" flag:"http-addr" usage:"listen address, host:port or :port" default:":8082"`

	ReadHeaderTimeout Duration `json:"read_header_timeout" yaml:"read_header_timeout" env:"NEBULA_HTTP_READ_HEADER_TIMEOUT" default:"10s"`
	ReadTimeout       Duration `json:"read_timeout" yaml:"read_timeout" env:"NEBULA_HTTP_READ_TIMEOUT" default:"30s"`
	WriteTimeout      Duration `json:"write_timeout" yaml:"write_timeout" env:"NEBULA_HTTP_WRITE_TIMEOUT" default:"60s"`
	IdleTimeout       Duration `json:"idle_timeout" yaml:"idle_timeout" env:"NEBULA_HTTP_IDLE_TIMEOUT" default:"120s"`

	// ShutdownGrace bounds how long in-flight requests get to finish on SIGTERM.
	// Must be shorter than the pod's terminationGracePeriodSeconds.
	ShutdownGrace Duration `json:"shutdown_grace" yaml:"shutdown_grace" env:"NEBULA_HTTP_SHUTDOWN_GRACE" default:"25s"`
	// DrainDelay is the pause after failing readiness before refusing traffic,
	// so EndpointSlice propagation completes first. This is the fix for
	// "503s during a rolling deploy".
	DrainDelay Duration `json:"drain_delay" yaml:"drain_delay" env:"NEBULA_HTTP_DRAIN_DELAY" default:"5s"`

	MaxHeaderBytes int   `json:"max_header_bytes" yaml:"max_header_bytes" env:"NEBULA_HTTP_MAX_HEADER_BYTES" default:"1048576"`
	MaxBodyBytes   int64 `json:"max_body_bytes" yaml:"max_body_bytes" env:"NEBULA_HTTP_MAX_BODY_BYTES" default:"10485760"`
}

// DatabaseConfig controls the PostgreSQL connection pool.
type DatabaseConfig struct {
	// URL is a libpq connection string or postgres:// URL.
	URL Secret `json:"url" yaml:"url" env:"NEBULA_DATABASE_URL"`

	MaxConns int32 `json:"max_conns" yaml:"max_conns" env:"NEBULA_DATABASE_MAX_CONNS" default:"10"`
	MinConns int32 `json:"min_conns" yaml:"min_conns" env:"NEBULA_DATABASE_MIN_CONNS" default:"2"`

	MaxConnLifetime Duration `json:"max_conn_lifetime" yaml:"max_conn_lifetime" env:"NEBULA_DATABASE_MAX_CONN_LIFETIME" default:"1h"`
	MaxConnIdleTime Duration `json:"max_conn_idle_time" yaml:"max_conn_idle_time" env:"NEBULA_DATABASE_MAX_CONN_IDLE_TIME" default:"30m"`
	ConnectTimeout  Duration `json:"connect_timeout" yaml:"connect_timeout" env:"NEBULA_DATABASE_CONNECT_TIMEOUT" default:"10s"`

	// StatementTimeout is applied server-side to every session in the pool, so a
	// runaway query cannot hold a connection indefinitely.
	StatementTimeout Duration `json:"statement_timeout" yaml:"statement_timeout" env:"NEBULA_DATABASE_STATEMENT_TIMEOUT" default:"30s"`

	// Role, when set, is assumed on every connection (SET ROLE, as a startup
	// parameter). The controller runs as nebula_controller, whose column-level grants
	// let it write OBSERVED state and nothing a user wrote (migration 000007): the
	// permission model is enforced by PostgreSQL, not by the controller's good
	// behaviour. The login user must be a member of the role.
	Role string `json:"role" yaml:"role" env:"NEBULA_DATABASE_ROLE"`

	// AssertSchemaVersion refuses to serve when the database schema is not the
	// version this binary was built against. Leave this on.
	AssertSchemaVersion bool `json:"assert_schema_version" yaml:"assert_schema_version" env:"NEBULA_DATABASE_ASSERT_SCHEMA_VERSION" default:"true"`
}

// AuthConfig holds credential-handling settings.
type AuthConfig struct {
	// KeyPepper is mixed into every API key hash. It lives only in the
	// application's memory, so a database dump alone does not yield verifiable
	// hashes (ADR-0011). Required by any service that authenticates callers;
	// generate one with `openssl rand -base64 32`.
	KeyPepper Secret `json:"key_pepper" yaml:"key_pepper" env:"NEBULA_AUTH_KEY_PEPPER"`

	// KeyCacheTTL bounds how long a verified key stays cached in process.
	//
	// Revocation publishes an invalidation in Phase 4; until then this TTL is the
	// whole guarantee, so it is kept short deliberately.
	KeyCacheTTL Duration `json:"key_cache_ttl" yaml:"key_cache_ttl" env:"NEBULA_AUTH_KEY_CACHE_TTL" default:"30s"`

	// KeyCacheSize bounds the cache so a flood of distinct prefixes cannot grow it
	// without limit.
	KeyCacheSize int `json:"key_cache_size" yaml:"key_cache_size" env:"NEBULA_AUTH_KEY_CACHE_SIZE" default:"4096"`
}

// InternalConfig secures service-to-service calls inside the cluster.
type InternalConfig struct {
	// AuthSecret keys the HMAC on X-Nebula-Auth-Context, the signed identity the
	// gateway attaches to every call it makes to the control plane
	// (docs/security-boundaries.md §2, B2). Shared by exactly those two services.
	// Without it the control plane accepts only direct API-key authentication.
	AuthSecret Secret `json:"auth_secret" yaml:"auth_secret" env:"NEBULA_INTERNAL_AUTH_SECRET"`

	// AuthMaxAge bounds how old a signed context may be. It is the replay window,
	// so it is seconds, not minutes: a context is minted per call, never reused.
	AuthMaxAge Duration `json:"auth_max_age" yaml:"auth_max_age" env:"NEBULA_INTERNAL_AUTH_MAX_AGE" default:"10s"`
}

// RedisConfig controls the Redis client. Everything NEBULA keeps in Redis is
// transient and rebuildable (docs/architecture.md §3.2), so Redis is a soft
// dependency: every caller has a documented behaviour for when it is down.
type RedisConfig struct {
	// URL is redis://[user:password@]host:port/db. Empty disables Redis, which is
	// legal in dev and makes the gateway use its in-process fallbacks from the start.
	URL Secret `json:"url" yaml:"url" env:"NEBULA_REDIS_URL"`

	// KeyPrefix namespaces every key, so two installations can share a server.
	KeyPrefix string `json:"key_prefix" yaml:"key_prefix" env:"NEBULA_REDIS_KEY_PREFIX" default:"nebula:"`

	DialTimeout Duration `json:"dial_timeout" yaml:"dial_timeout" env:"NEBULA_REDIS_DIAL_TIMEOUT" default:"2s"`
	// OpTimeout bounds every command. Short, because Redis sits in the request path
	// and a slow Redis must degrade to the fallback rather than add its latency to
	// every request.
	OpTimeout Duration `json:"op_timeout" yaml:"op_timeout" env:"NEBULA_REDIS_OP_TIMEOUT" default:"150ms"`
	PoolSize  int      `json:"pool_size" yaml:"pool_size" env:"NEBULA_REDIS_POOL_SIZE" default:"64"`
}

// GatewayConfig controls nebula-gateway, the data plane.
type GatewayConfig struct {
	// ControlPlaneURL is where credentials are resolved and admin calls proxied.
	ControlPlaneURL string `json:"controlplane_url" yaml:"controlplane_url" env:"NEBULA_GATEWAY_CONTROLPLANE_URL" flag:"controlplane-url" usage:"base URL of nebula-controlplane" default:"http://127.0.0.1:8082"`
	// ControlPlaneTimeout bounds one call to the control plane.
	ControlPlaneTimeout Duration `json:"controlplane_timeout" yaml:"controlplane_timeout" env:"NEBULA_GATEWAY_CONTROLPLANE_TIMEOUT" default:"5s"`

	// RoutesFile is a static route table, YAML or JSON, with worker endpoints
	// written into it: for running without Kubernetes and without a control plane
	// routing table. Empty (the default) reads routes from the control plane.
	RoutesFile string `json:"routes_file" yaml:"routes_file" env:"NEBULA_GATEWAY_ROUTES_FILE" flag:"routes-file" usage:"static route table (YAML or JSON); empty reads routes from the control plane"`
	// RoutesRefresh is how often the routing table is re-read from the control
	// plane. A route change reaches this gateway within it.
	RoutesRefresh Duration `json:"routes_refresh" yaml:"routes_refresh" env:"NEBULA_GATEWAY_ROUTES_REFRESH" default:"2s"`
	// Endpoints is where worker endpoints come from: "static" (the route file) or
	// "kubernetes" (EndpointSlices in NEBULA_KUBE_WORKLOAD_NAMESPACE).
	Endpoints string `json:"endpoints" yaml:"endpoints" env:"NEBULA_GATEWAY_ENDPOINTS" default:"static"`
	// HeartbeatStaleAfter drops an endpoint whose heartbeat is older than this
	// while heartbeats are flowing: three one-second intervals by default
	// (docs/architecture.md §6.2).
	HeartbeatStaleAfter Duration `json:"heartbeat_stale_after" yaml:"heartbeat_stale_after" env:"NEBULA_GATEWAY_HEARTBEAT_STALE_AFTER" default:"3s"`
	// BreakerThreshold consecutive upstream failures open an endpoint's breaker; a
	// failure to connect opens it at once. BreakerCooldown is the first opening's
	// length, doubling on each failed probe up to 30s.
	BreakerThreshold int      `json:"breaker_threshold" yaml:"breaker_threshold" env:"NEBULA_GATEWAY_BREAKER_THRESHOLD" default:"3"`
	BreakerCooldown  Duration `json:"breaker_cooldown" yaml:"breaker_cooldown" env:"NEBULA_GATEWAY_BREAKER_COOLDOWN" default:"2s"`

	// QueueMaxDepth bounds each deployment's admission queue; a full queue answers
	// 429 with Retry-After at once (docs/architecture.md §6.3).
	QueueMaxDepth int `json:"queue_max_depth" yaml:"queue_max_depth" env:"NEBULA_GATEWAY_QUEUE_MAX_DEPTH" default:"128"`
	// QueueAging is how long a queued request waits to gain one priority level, so
	// LOW is never starved by sustained HIGH.
	QueueAging Duration `json:"queue_aging" yaml:"queue_aging" env:"NEBULA_GATEWAY_QUEUE_AGING" default:"5s"`
	// DefaultSlots is an endpoint's concurrency when no heartbeat reports it, and
	// SlotOvercommit multiplies slots into how many requests the gateway lets reach
	// one endpoint at once (the rest wait in the admission queue).
	DefaultSlots   int `json:"default_slots" yaml:"default_slots" env:"NEBULA_GATEWAY_DEFAULT_SLOTS" default:"4"`
	SlotOvercommit int `json:"slot_overcommit" yaml:"slot_overcommit" env:"NEBULA_GATEWAY_SLOT_OVERCOMMIT" default:"2"`

	// DefaultTimeout is a request's budget when neither the client nor the route
	// sets one. MaxTimeout caps whatever the client asks for.
	DefaultTimeout Duration `json:"default_timeout" yaml:"default_timeout" env:"NEBULA_GATEWAY_DEFAULT_TIMEOUT" default:"60s"`
	MaxTimeout     Duration `json:"max_timeout" yaml:"max_timeout" env:"NEBULA_GATEWAY_MAX_TIMEOUT" default:"10m"`

	// KeepAliveInterval is how long a stream may be idle before an SSE comment is
	// written, so an idle proxy does not close a connection whose model is still
	// thinking (docs/api.md §2).
	KeepAliveInterval Duration `json:"keepalive_interval" yaml:"keepalive_interval" env:"NEBULA_GATEWAY_KEEPALIVE_INTERVAL" default:"15s"`
	// StreamWriteTimeout bounds one write to a streaming client. A client that stops
	// reading is cancelled and its worker slot freed rather than holding it forever
	// (docs/components.md §2.1, "slow client").
	StreamWriteTimeout Duration `json:"stream_write_timeout" yaml:"stream_write_timeout" env:"NEBULA_GATEWAY_STREAM_WRITE_TIMEOUT" default:"10s"`
	// CancelDrainTimeout is how long, after a client disconnects, the gateway keeps
	// reading the cancelled worker stream for its final usage frame. Bounded, because
	// the client is already gone and this exists only for accounting.
	CancelDrainTimeout Duration `json:"cancel_drain_timeout" yaml:"cancel_drain_timeout" env:"NEBULA_GATEWAY_CANCEL_DRAIN_TIMEOUT" default:"3s"`

	// LocalKeyCacheTTL is the in-process credential cache in front of Redis. Shorter
	// than the Redis TTL, because it is the part a revocation cannot reach on other
	// replicas: this number is the cross-replica revocation delay.
	LocalKeyCacheTTL Duration `json:"local_key_cache_ttl" yaml:"local_key_cache_ttl" env:"NEBULA_GATEWAY_LOCAL_KEY_CACHE_TTL" default:"5s"`
	// NegativeKeyCacheTTL caches "no such key" so a flood of invented keys costs one
	// control-plane lookup per prefix per TTL, not one per request.
	NegativeKeyCacheTTL Duration `json:"negative_key_cache_ttl" yaml:"negative_key_cache_ttl" env:"NEBULA_GATEWAY_NEGATIVE_KEY_CACHE_TTL" default:"5s"`
	// StaleKeyGrace is how long a cached credential keeps authenticating after its
	// TTL when the control plane cannot be reached (axiom A8: a degraded control plane
	// must not break inference). Bounded, then it fails closed.
	StaleKeyGrace Duration `json:"stale_key_grace" yaml:"stale_key_grace" env:"NEBULA_GATEWAY_STALE_KEY_GRACE" default:"5m"`

	// DefaultMaxTokens applies when a request sets neither max_tokens nor
	// max_completion_tokens and the route declares no default.
	DefaultMaxTokens int `json:"default_max_tokens" yaml:"default_max_tokens" env:"NEBULA_GATEWAY_DEFAULT_MAX_TOKENS" default:"256"`
}

// LimitsConfig is the gateway's admission control: requests per minute, tokens
// per minute and concurrency, per API key and per organization.
//
// Key limits come from the key's rate_limit_policy when it has one and from the
// defaults here when it does not. Organization limits come from here.
type LimitsConfig struct {
	// Enabled switches rate limiting off entirely. Only for load tests that measure
	// the serving path rather than the limiter; refused in production.
	Enabled bool `json:"enabled" yaml:"enabled" env:"NEBULA_LIMITS_ENABLED" default:"true"`

	KeyRPM         int `json:"key_rpm" yaml:"key_rpm" env:"NEBULA_LIMITS_KEY_RPM" default:"600"`
	KeyTPM         int `json:"key_tpm" yaml:"key_tpm" env:"NEBULA_LIMITS_KEY_TPM" default:"200000"`
	KeyConcurrency int `json:"key_concurrency" yaml:"key_concurrency" env:"NEBULA_LIMITS_KEY_CONCURRENCY" default:"32"`

	OrgRPM         int `json:"org_rpm" yaml:"org_rpm" env:"NEBULA_LIMITS_ORG_RPM" default:"6000"`
	OrgTPM         int `json:"org_tpm" yaml:"org_tpm" env:"NEBULA_LIMITS_ORG_TPM" default:"2000000"`
	OrgConcurrency int `json:"org_concurrency" yaml:"org_concurrency" env:"NEBULA_LIMITS_ORG_CONCURRENCY" default:"256"`

	// FallbackFraction scales every limit while Redis is unreachable and each
	// replica is counting alone. Below 1 so that N replicas counting independently
	// admit less, not N times more — "conservative" in docs/architecture.md §3.2 is
	// this number. Approximate by construction, and documented as such.
	FallbackFraction float64 `json:"fallback_fraction" yaml:"fallback_fraction" env:"NEBULA_LIMITS_FALLBACK_FRACTION" default:"0.5"`
}

// ArtifactConfig selects and configures the model artifact store
// (docs/deployment-architecture.md §5).
type ArtifactConfig struct {
	// Store is "s3", "file" or "none". With "none" the registry records whatever
	// artifact_uri a client declares and finalize can only compare declared
	// checksums — the Phase 2 behaviour, reported as verification: declared_checksum.
	Store string `json:"store" yaml:"store" env:"NEBULA_ARTIFACT_STORE" default:"none"`

	S3Endpoint string `json:"s3_endpoint" yaml:"s3_endpoint" env:"NEBULA_ARTIFACT_S3_ENDPOINT"`
	// S3PublicEndpoint is the host:port clients use; presigned URLs are signed for it.
	S3PublicEndpoint string `json:"s3_public_endpoint" yaml:"s3_public_endpoint" env:"NEBULA_ARTIFACT_S3_PUBLIC_ENDPOINT"`
	S3Bucket         string `json:"s3_bucket" yaml:"s3_bucket" env:"NEBULA_ARTIFACT_S3_BUCKET" default:"nebula-models"`
	S3Region         string `json:"s3_region" yaml:"s3_region" env:"NEBULA_ARTIFACT_S3_REGION" default:"us-east-1"`
	S3AccessKey      Secret `json:"s3_access_key" yaml:"s3_access_key" env:"NEBULA_ARTIFACT_S3_ACCESS_KEY"`
	S3SecretKey      Secret `json:"s3_secret_key" yaml:"s3_secret_key" env:"NEBULA_ARTIFACT_S3_SECRET_KEY"`
	S3UseTLS         bool   `json:"s3_use_tls" yaml:"s3_use_tls" env:"NEBULA_ARTIFACT_S3_USE_TLS" default:"false"`

	// Dir is the root of a "file" store.
	Dir string `json:"dir" yaml:"dir" env:"NEBULA_ARTIFACT_DIR"`

	// MaxBytes bounds one artifact. 50 GiB by default: larger than any model a CPU
	// cluster will serve, small enough that a runaway upload is refused.
	MaxBytes   int64    `json:"max_bytes" yaml:"max_bytes" env:"NEBULA_ARTIFACT_MAX_BYTES" default:"53687091200"`
	PresignTTL Duration `json:"presign_ttl" yaml:"presign_ttl" env:"NEBULA_ARTIFACT_PRESIGN_TTL" default:"1h"`
	// VerifyInterval is how often the registry looks for versions to verify, as a
	// backstop to the immediate verification finalize starts.
	VerifyInterval Duration `json:"verify_interval" yaml:"verify_interval" env:"NEBULA_ARTIFACT_VERIFY_INTERVAL" default:"5s"`
}

// NATSConfig locates the signalling bus (ADR-0007). Worker heartbeats travel on
// core NATS; nothing durable depends on it yet.
type NATSConfig struct {
	// URL is nats://host:port. Empty disables heartbeats: routing then uses
	// endpoint readiness and local observation only, which is correct but less
	// informed (docs/events.md §3.1).
	URL string `json:"url" yaml:"url" env:"NEBULA_NATS_URL" flag:"nats-url" usage:"NATS server URL (empty: no heartbeats)"`
}

// KubeConfig configures the controller's view of Kubernetes.
type KubeConfig struct {
	// Kubeconfig is a kubeconfig path; empty means in-cluster configuration.
	Kubeconfig string `json:"kubeconfig" yaml:"kubeconfig" env:"NEBULA_KUBECONFIG" flag:"kubeconfig" usage:"kubeconfig path (empty: in-cluster)"`
	// WorkloadNamespace holds every worker. The controller's write permissions are
	// scoped to it alone (docs/deployment-architecture.md §3).
	WorkloadNamespace string `json:"workload_namespace" yaml:"workload_namespace" env:"NEBULA_KUBE_WORKLOAD_NAMESPACE" default:"nebula-workloads"`
	// SystemNamespace holds NEBULA itself and the leader-election Lease.
	SystemNamespace string `json:"system_namespace" yaml:"system_namespace" env:"NEBULA_KUBE_SYSTEM_NAMESPACE" default:"nebula-system"`

	LeaderElection bool     `json:"leader_election" yaml:"leader_election" env:"NEBULA_KUBE_LEADER_ELECTION" default:"true"`
	LeaseDuration  Duration `json:"lease_duration" yaml:"lease_duration" env:"NEBULA_KUBE_LEASE_DURATION" default:"15s"`
	RenewDeadline  Duration `json:"renew_deadline" yaml:"renew_deadline" env:"NEBULA_KUBE_RENEW_DEADLINE" default:"10s"`
	RetryPeriod    Duration `json:"retry_period" yaml:"retry_period" env:"NEBULA_KUBE_RETRY_PERIOD" default:"2s"`

	// ResyncInterval re-reconciles every deployment periodically, so a missed event
	// is corrected within this bound rather than never.
	ResyncInterval Duration `json:"resync_interval" yaml:"resync_interval" env:"NEBULA_KUBE_RESYNC_INTERVAL" default:"30s"`
	// PollInterval is how often the controller asks PostgreSQL what changed. NATS
	// reconcile signals (Phase 6) make it a backstop rather than the trigger.
	PollInterval Duration `json:"poll_interval" yaml:"poll_interval" env:"NEBULA_KUBE_POLL_INTERVAL" default:"2s"`
	// StartingTimeout fails a deployment whose pods have not become ready in time.
	StartingTimeout Duration `json:"starting_timeout" yaml:"starting_timeout" env:"NEBULA_KUBE_STARTING_TIMEOUT" default:"10m"`

	// Images the controller puts in worker pods. Explicit tags, never "latest".
	WorkerImage     string `json:"worker_image" yaml:"worker_image" env:"NEBULA_KUBE_WORKER_IMAGE" default:"nebula/worker-llamacpp:dev"`
	MockWorkerImage string `json:"mock_worker_image" yaml:"mock_worker_image" env:"NEBULA_KUBE_MOCK_WORKER_IMAGE" default:"nebula/worker-mock:dev"`
	PullerImage     string `json:"puller_image" yaml:"puller_image" env:"NEBULA_KUBE_PULLER_IMAGE" default:"nebula/artifact-puller:dev"`
	ImagePullPolicy string `json:"image_pull_policy" yaml:"image_pull_policy" env:"NEBULA_KUBE_IMAGE_PULL_POLICY" default:"IfNotPresent"`
	// ArtifactCacheHostPath is the node-local, content-addressed model cache.
	ArtifactCacheHostPath string `json:"artifact_cache_host_path" yaml:"artifact_cache_host_path" env:"NEBULA_KUBE_ARTIFACT_CACHE_HOST_PATH" default:"/var/lib/nebula/models"`
	// ArtifactSecretName holds the store credentials the puller reads. The
	// controller references it by name and never reads it (it holds no secrets
	// permission, docs/deployment-architecture.md §3).
	ArtifactSecretName string `json:"artifact_secret_name" yaml:"artifact_secret_name" env:"NEBULA_KUBE_ARTIFACT_SECRET_NAME" default:"nebula-artifact-store"`
}

// DevConfig holds affordances that must never be enabled in production. Every
// field here is rejected by Validate when Env is production.
type DevConfig struct {
	// Seed inserts a development organization, user and API key at startup.
	Seed bool `json:"seed" yaml:"seed" env:"NEBULA_DEV_SEED" default:"false"`
	// MockRuntime allows the declared-stub inference runtime (Phase 3).
	MockRuntime bool `json:"mock_runtime" yaml:"mock_runtime" env:"NEBULA_DEV_MOCK_RUNTIME" default:"false"`
	// SeedKey, when set, is the plaintext of the seeded API key instead of a random
	// one, so a development cluster can be recreated without losing its key. It is a
	// credential everyone who reads the values file knows, which is exactly why the
	// seed is refused in production.
	SeedKey Secret `json:"seed_key" yaml:"seed_key" env:"NEBULA_DEV_SEED_KEY"`
	// CreateBucket makes the control plane create the artifact bucket at startup.
	// Buckets are otherwise an install-time decision made by an operator; a service
	// that creates them silently is one that silently writes to the wrong account.
	CreateBucket bool `json:"create_bucket" yaml:"create_bucket" env:"NEBULA_DEV_CREATE_BUCKET" default:"false"`
	// DebugEndpoints exposes /debug/pprof.
	DebugEndpoints bool `json:"debug_endpoints" yaml:"debug_endpoints" env:"NEBULA_DEV_DEBUG_ENDPOINTS" default:"false"`
}

// Service returns the service name the config was loaded for.
func (c *Config) Service() string { return c.service }

// String is deliberately not the full config: use Redact for that. This avoids a
// half-redacted dump reaching a log through %v on the struct.
func (c *Config) String() string {
	return fmt.Sprintf("Config{service:%s env:%s}", c.service, c.Env)
}

// LogLevelValid reports whether the configured level is recognised.
func (l LogConfig) LogLevelValid() bool {
	switch strings.ToLower(l.Level) {
	case "debug", "info", "warn", "error":
		return true
	}
	return false
}
