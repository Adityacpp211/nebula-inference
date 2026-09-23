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

	// AssertSchemaVersion refuses to serve when the database schema is not the
	// version this binary was built against. Leave this on.
	AssertSchemaVersion bool `json:"assert_schema_version" yaml:"assert_schema_version" env:"NEBULA_DATABASE_ASSERT_SCHEMA_VERSION" default:"true"`
}

// DevConfig holds affordances that must never be enabled in production. Every
// field here is rejected by Validate when Env is production.
type DevConfig struct {
	// Seed inserts a development organization, user and API key at startup.
	Seed bool `json:"seed" yaml:"seed" env:"NEBULA_DEV_SEED" default:"false"`
	// MockRuntime allows the declared-stub inference runtime (Phase 3).
	MockRuntime bool `json:"mock_runtime" yaml:"mock_runtime" env:"NEBULA_DEV_MOCK_RUNTIME" default:"false"`
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
