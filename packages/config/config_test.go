package config_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/adityasatwar321/nebula/packages/config"
)

// validDSN is a connection string that passes validation without tripping the
// default-credential check.
const validDSN = "postgres://nebula_app:s3cr3t-not-default@db:5432/nebula"

// envMap builds a Getenv function over a map, so no test mutates the real
// process environment and tests stay parallel-safe.
func envMap(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func baseEnv(extra map[string]string) map[string]string {
	m := map[string]string{"NEBULA_DATABASE_URL": validDSN}
	for k, v := range extra {
		m[k] = v
	}
	return m
}

func load(t *testing.T, args []string, env, files map[string]string) (*config.Config, error) {
	t.Helper()
	return config.Loader{
		Service: "test-service",
		Args:    args,
		Getenv:  envMap(env),
		ReadFile: func(p string) ([]byte, error) {
			b, ok := files[p]
			if !ok {
				return nil, fmt.Errorf("no such file: %s", p)
			}
			return []byte(b), nil
		},
		Output: io.Discard,
	}.Load()
}

func TestDefaultsApplied(t *testing.T) {
	t.Parallel()

	cfg, err := load(t, nil, baseEnv(nil), nil)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	checks := []struct {
		name string
		got  any
		want any
	}{
		{"Env", cfg.Env, config.EnvDev},
		{"Log.Level", cfg.Log.Level, "info"},
		{"Log.Format", cfg.Log.Format, "json"},
		{"Log.AddSource", cfg.Log.AddSource, false},
		{"HTTP.Addr", cfg.HTTP.Addr, ":8082"},
		{"HTTP.ReadHeaderTimeout", cfg.HTTP.ReadHeaderTimeout.Duration(), 10 * time.Second},
		{"HTTP.ShutdownGrace", cfg.HTTP.ShutdownGrace.Duration(), 25 * time.Second},
		{"HTTP.DrainDelay", cfg.HTTP.DrainDelay.Duration(), 5 * time.Second},
		{"HTTP.MaxBodyBytes", cfg.HTTP.MaxBodyBytes, int64(10485760)},
		{"Database.MaxConns", cfg.Database.MaxConns, int32(10)},
		{"Database.StatementTimeout", cfg.Database.StatementTimeout.Duration(), 30 * time.Second},
		{"Database.AssertSchemaVersion", cfg.Database.AssertSchemaVersion, true},
		{"Dev.Seed", cfg.Dev.Seed, false},
		{"Service", cfg.Service(), "test-service"},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

// TestPrecedence is the core contract of this package: defaults < file < env <
// flags. Each layer must override the one before it and nothing else.
func TestPrecedence(t *testing.T) {
	t.Parallel()

	const path = "/etc/nebula/config.yaml"
	files := map[string]string{path: `
env: staging
log:
  level: warn
  format: text
http:
  addr: ":9000"
  read_timeout: 45s
database:
  max_conns: 25
`}

	tests := []struct {
		name       string
		args       []string
		env        map[string]string
		wantEnv    config.Env
		wantLevel  string
		wantAddr   string
		wantMax    int32
		wantRead   time.Duration
		wantFormat string
	}{
		{
			name:       "file over defaults",
			args:       []string{"-config", path},
			env:        baseEnv(nil),
			wantEnv:    config.EnvStaging,
			wantLevel:  "warn",
			wantAddr:   ":9000",
			wantMax:    25,
			wantRead:   45 * time.Second,
			wantFormat: "text",
		},
		{
			name: "env over file",
			args: []string{"-config", path},
			env: baseEnv(map[string]string{
				"NEBULA_LOG_LEVEL": "error",
				"NEBULA_HTTP_ADDR": ":9100",
			}),
			wantEnv:    config.EnvStaging,
			wantLevel:  "error",
			wantAddr:   ":9100",
			wantMax:    25, // untouched by env, still from the file
			wantRead:   45 * time.Second,
			wantFormat: "text",
		},
		{
			name: "flag over env",
			args: []string{"-config", path, "-log-level", "debug", "-http-addr", ":9200"},
			env: baseEnv(map[string]string{
				"NEBULA_LOG_LEVEL": "error",
				"NEBULA_HTTP_ADDR": ":9100",
			}),
			wantEnv:    config.EnvStaging,
			wantLevel:  "debug",
			wantAddr:   ":9200",
			wantMax:    25,
			wantRead:   45 * time.Second,
			wantFormat: "text",
		},
		{
			name:       "config file path from environment",
			args:       nil,
			env:        baseEnv(map[string]string{config.EnvFileVar: path}),
			wantEnv:    config.EnvStaging,
			wantLevel:  "warn",
			wantAddr:   ":9000",
			wantMax:    25,
			wantRead:   45 * time.Second,
			wantFormat: "text",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg, err := load(t, tt.args, tt.env, files)
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if cfg.Env != tt.wantEnv {
				t.Errorf("Env = %q, want %q", cfg.Env, tt.wantEnv)
			}
			if cfg.Log.Level != tt.wantLevel {
				t.Errorf("Log.Level = %q, want %q", cfg.Log.Level, tt.wantLevel)
			}
			if cfg.Log.Format != tt.wantFormat {
				t.Errorf("Log.Format = %q, want %q", cfg.Log.Format, tt.wantFormat)
			}
			if cfg.HTTP.Addr != tt.wantAddr {
				t.Errorf("HTTP.Addr = %q, want %q", cfg.HTTP.Addr, tt.wantAddr)
			}
			if cfg.Database.MaxConns != tt.wantMax {
				t.Errorf("Database.MaxConns = %d, want %d", cfg.Database.MaxConns, tt.wantMax)
			}
			if cfg.HTTP.ReadTimeout.Duration() != tt.wantRead {
				t.Errorf("HTTP.ReadTimeout = %v, want %v", cfg.HTTP.ReadTimeout.Duration(), tt.wantRead)
			}
		})
	}
}

// An empty environment variable must be treated as unset. In Kubernetes an unset
// optional variable is routinely rendered as "", and treating that as an explicit
// override would silently wipe a good default.
func TestEmptyEnvVarDoesNotOverride(t *testing.T) {
	t.Parallel()

	cfg, err := load(t, nil, baseEnv(map[string]string{"NEBULA_LOG_LEVEL": ""}), nil)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Log.Level != "info" {
		t.Errorf("Log.Level = %q, want the default %q", cfg.Log.Level, "info")
	}
}

// Validation must report EVERY problem at once, not just the first.
func TestValidationAggregatesAllProblems(t *testing.T) {
	t.Parallel()

	_, err := load(t, nil, map[string]string{
		"NEBULA_ENV":                "banana",
		"NEBULA_LOG_LEVEL":          "screaming",
		"NEBULA_LOG_FORMAT":         "xml",
		"NEBULA_HTTP_ADDR":          "not-an-address",
		"NEBULA_DATABASE_MAX_CONNS": "0",
		// NEBULA_DATABASE_URL deliberately absent
	}, nil)
	if err == nil {
		t.Fatal("Load() succeeded on invalid configuration, want error")
	}

	var verrs *config.ValidationErrors
	if !errors.As(err, &verrs) {
		t.Fatalf("error is %T, want *config.ValidationErrors", err)
	}

	wantFields := []string{
		"NEBULA_ENV", "NEBULA_LOG_LEVEL", "NEBULA_LOG_FORMAT",
		"NEBULA_HTTP_ADDR", "NEBULA_DATABASE_MAX_CONNS", "NEBULA_DATABASE_URL",
	}
	got := map[string]bool{}
	for _, e := range verrs.Errors {
		got[e.Field] = true
	}
	for _, f := range wantFields {
		if !got[f] {
			t.Errorf("no validation error reported for %s; got %v", f, verrs.Errors)
		}
	}
	if verrs.Len() < len(wantFields) {
		t.Errorf("reported %d problems, want at least %d", verrs.Len(), len(wantFields))
	}
	// The rendered message must list them all, since that is what an operator sees.
	msg := verrs.Error()
	for _, f := range wantFields {
		if !strings.Contains(msg, f) {
			t.Errorf("Error() message omits %s:\n%s", f, msg)
		}
	}
}

func TestDrainDelayMustBeShorterThanShutdownGrace(t *testing.T) {
	t.Parallel()

	_, err := load(t, nil, baseEnv(map[string]string{
		"NEBULA_HTTP_DRAIN_DELAY":    "30s",
		"NEBULA_HTTP_SHUTDOWN_GRACE": "10s",
	}), nil)
	if err == nil {
		t.Fatal("Load() accepted a drain delay longer than the shutdown grace, want error")
	}
	if !strings.Contains(err.Error(), "NEBULA_HTTP_DRAIN_DELAY") {
		t.Errorf("error does not mention the offending field: %v", err)
	}
}

// Production gating: every development affordance must be a startup FAILURE, not
// a warning (axiom A9, risk R-17).
func TestProductionGating(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		env       map[string]string
		wantField string
	}{
		{"dev seed", map[string]string{"NEBULA_DEV_SEED": "true"}, "NEBULA_DEV_SEED"},
		{"mock runtime", map[string]string{"NEBULA_DEV_MOCK_RUNTIME": "true"}, "NEBULA_DEV_MOCK_RUNTIME"},
		{"debug endpoints", map[string]string{"NEBULA_DEV_DEBUG_ENDPOINTS": "true"}, "NEBULA_DEV_DEBUG_ENDPOINTS"},
		{"text logs", map[string]string{"NEBULA_LOG_FORMAT": "text"}, "NEBULA_LOG_FORMAT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			env := baseEnv(tt.env)
			env["NEBULA_ENV"] = "production"

			_, err := load(t, nil, env, nil)
			if err == nil {
				t.Fatalf("production config with %s accepted, want error", tt.name)
			}
			if !strings.Contains(err.Error(), tt.wantField) {
				t.Errorf("error does not name %s: %v", tt.wantField, err)
			}

			// The same settings must be fine outside production.
			env["NEBULA_ENV"] = "dev"
			if _, err := load(t, nil, env, nil); err != nil {
				t.Errorf("dev config with %s rejected: %v", tt.name, err)
			}
		})
	}
}

// An install that would otherwise succeed with a well-known password is a silent
// security failure, so production refuses it.
func TestProductionRejectsDefaultCredentials(t *testing.T) {
	t.Parallel()

	for _, dsn := range []string{
		"postgres://postgres:postgres@db:5432/nebula",
		"postgres://nebula:nebula@db:5432/nebula",
		"host=db user=nebula password=changeme dbname=nebula",
	} {
		t.Run(dsn, func(t *testing.T) {
			t.Parallel()
			_, err := load(t, nil, map[string]string{
				"NEBULA_ENV":          "production",
				"NEBULA_DATABASE_URL": dsn,
			}, nil)
			if err == nil {
				t.Fatalf("production accepted a default credential in %q", dsn)
			}
			if !strings.Contains(err.Error(), "NEBULA_DATABASE_URL") {
				t.Errorf("error does not name the database URL: %v", err)
			}
		})
	}
}

// An unknown key in a config file is a typo, and a typo that is silently ignored
// is a setting the operator believes is applied when it is not.
func TestUnknownConfigFileKeyIsRejected(t *testing.T) {
	t.Parallel()

	const path = "/etc/nebula/bad.yaml"
	_, err := load(t, []string{"-config", path}, baseEnv(nil), map[string]string{
		path: "log:\n  levl: debug\n",
	})
	if err == nil {
		t.Fatal("Load() accepted an unknown config key, want error")
	}
	if !strings.Contains(err.Error(), "levl") {
		t.Errorf("error does not name the unknown key: %v", err)
	}
}

func TestBadDurationAndBoolReported(t *testing.T) {
	t.Parallel()

	_, err := load(t, nil, baseEnv(map[string]string{
		"NEBULA_HTTP_READ_TIMEOUT": "soon",
		"NEBULA_LOG_ADD_SOURCE":    "perhaps",
	}), nil)
	if err == nil {
		t.Fatal("Load() accepted unparseable values, want error")
	}
	for _, want := range []string{"NEBULA_HTTP_READ_TIMEOUT", "NEBULA_LOG_ADD_SOURCE"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not name %s: %v", want, err)
		}
	}
}

// A secret must not be renderable by any of the ways a value normally escapes:
// formatting verbs, JSON, YAML, or an error message.
func TestSecretNeverRenders(t *testing.T) {
	t.Parallel()

	const plaintext = "super-secret-password"
	s := config.Secret(plaintext)

	renderings := map[string]string{
		"String()": s.String(),
		"%v":       s.String(),
		"%s":       s.String(),
		"%q":       fmt.Sprintf("%q", s),
		"%#v":      fmt.Sprintf("%#v", s),
		"%+v":      fmt.Sprintf("%+v", s),
	}
	for name, got := range renderings {
		if strings.Contains(got, plaintext) {
			t.Errorf("%s leaked the secret: %s", name, got)
		}
	}

	b, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("json.Marshal(Secret) error = %v", err)
	}
	if strings.Contains(string(b), plaintext) {
		t.Errorf("JSON marshalling leaked the secret: %s", b)
	}
	if s.Reveal() != plaintext {
		t.Errorf("Reveal() = %q, want the original value", s.Reveal())
	}
}

// Redact is what /healthz serves, so it must not contain the DSN.
func TestRedactHidesDatabaseURL(t *testing.T) {
	t.Parallel()

	cfg, err := load(t, nil, baseEnv(nil), nil)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	m, err := cfg.Redact()
	if err != nil {
		t.Fatalf("Redact() error = %v", err)
	}

	b, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshalling redacted config: %v", err)
	}
	if strings.Contains(string(b), "s3cr3t-not-default") {
		t.Fatalf("redacted config leaked the database password: %s", b)
	}
	if !strings.Contains(string(b), config.Redacted) {
		t.Errorf("redacted config does not contain the placeholder: %s", b)
	}
	if m["service"] != "test-service" {
		t.Errorf("redacted config service = %v, want test-service", m["service"])
	}
}

func TestHelpRequestedIsNotAFailure(t *testing.T) {
	t.Parallel()

	_, err := load(t, []string{"-help"}, baseEnv(nil), nil)
	if !errors.Is(err, config.ErrHelpRequested) {
		t.Fatalf("Load() with -help returned %v, want ErrHelpRequested", err)
	}
}

func TestEnvPredicates(t *testing.T) {
	t.Parallel()

	if !config.EnvProduction.IsProduction() {
		t.Error("EnvProduction.IsProduction() = false")
	}
	if config.EnvDev.IsProduction() || config.EnvStaging.IsProduction() {
		t.Error("non-production environment reported as production")
	}
	for _, e := range []config.Env{config.EnvDev, config.EnvStaging, config.EnvProduction} {
		if !e.Valid() {
			t.Errorf("%q reported invalid", e)
		}
	}
	if config.Env("prod").Valid() {
		t.Error(`"prod" reported valid; only the three exact names are allowed`)
	}
}
