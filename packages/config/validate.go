package config

import (
	"fmt"
	"net"
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
	if c.Database.URL.IsZero() {
		v.Add("NEBULA_DATABASE_URL", "is required")
	} else if !plausibleDSN(c.Database.URL.Reveal()) {
		v.Add("NEBULA_DATABASE_URL", "must be a postgres:// URL or a libpq keyword/value string")
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

	// --- production gating -------------------------------------------------
	// Every development affordance is a startup FAILURE in production, not a
	// warning. See docs/architecture.md axiom A9 and risk R-17.
	if c.Env.IsProduction() {
		v.AddIf(c.Dev.Seed, "NEBULA_DEV_SEED", "must be false when NEBULA_ENV=production")
		v.AddIf(c.Dev.MockRuntime, "NEBULA_DEV_MOCK_RUNTIME",
			"must be false when NEBULA_ENV=production: the mock runtime is a declared development stub")
		v.AddIf(c.Dev.DebugEndpoints, "NEBULA_DEV_DEBUG_ENDPOINTS", "must be false when NEBULA_ENV=production")
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
	}

	if v.Len() > 0 {
		return &v
	}
	return nil
}

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
