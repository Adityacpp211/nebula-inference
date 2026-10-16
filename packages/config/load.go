package config

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// EnvFileVar names the environment variable holding a config file path.
const EnvFileVar = "NEBULA_CONFIG_FILE"

// Loader gathers the inputs for Load. Everything is injectable so the whole
// precedence chain is testable without touching the real process environment.
type Loader struct {
	// Service is the binary's name, recorded in logs and /healthz.
	Service string
	// Args are the command-line arguments excluding argv[0]. May be nil.
	Args []string
	// Getenv reads the environment. Defaults to os.Getenv.
	Getenv func(string) string
	// ReadFile reads the config file. Defaults to os.ReadFile.
	ReadFile func(string) ([]byte, error)
	// Output receives flag parse errors and -help. Defaults to os.Stderr.
	Output io.Writer
	// Defaults overrides code defaults for this binary, keyed by environment
	// variable name, e.g. {"NEBULA_HTTP_ADDR": ":8080"}. Lowest precedence after the
	// struct tags, so a file, the environment or a flag still wins. It exists because
	// two services sharing one struct cannot share one listen port.
	Defaults map[string]string
}

// ErrHelpRequested is returned when -help was passed, so the caller can exit 0
// rather than treating it as a failure.
var ErrHelpRequested = errors.New("help requested")

// Load resolves configuration from defaults, then file, then environment, then
// flags, and validates the result.
//
// On invalid configuration it returns a *ValidationErrors listing every problem.
func (l Loader) Load() (*Config, error) {
	if l.Getenv == nil {
		l.Getenv = os.Getenv
	}
	if l.ReadFile == nil {
		l.ReadFile = os.ReadFile
	}
	if l.Output == nil {
		l.Output = os.Stderr
	}

	cfg := &Config{service: l.Service}
	if err := applyDefaults(cfg); err != nil {
		return nil, fmt.Errorf("applying defaults: %w", err)
	}
	if len(l.Defaults) > 0 {
		if err := applyEnv(cfg, func(k string) string { return l.Defaults[k] }); err != nil {
			return nil, fmt.Errorf("applying service defaults: %w", err)
		}
	}

	// Layer 2: file. Its path may come from a flag or the environment, so the
	// flag set is parsed once here for the path only, then again for values.
	path, err := l.resolveFilePath()
	if err != nil {
		return nil, err
	}
	if path != "" {
		b, err := l.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("reading config file %q: %w", path, err)
		}
		dec := yaml.NewDecoder(strings.NewReader(string(b)))
		dec.KnownFields(true) // an unknown key is a typo, not something to ignore
		if err := dec.Decode(cfg); err != nil && !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("parsing config file %q: %w", path, err)
		}
	}

	// Layer 3: environment.
	if err := applyEnv(cfg, l.Getenv); err != nil {
		return nil, err
	}

	// Layer 4: flags, highest precedence.
	if err := l.applyFlags(cfg); err != nil {
		return nil, err
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// resolveFilePath finds the config file path from flags or the environment
// without consuming the flag set used for values.
func (l Loader) resolveFilePath() (string, error) {
	path := l.Getenv(EnvFileVar)
	for i, a := range l.Args {
		switch {
		case a == "-config" || a == "--config":
			if i+1 < len(l.Args) {
				path = l.Args[i+1]
			} else {
				return "", errors.New("-config requires a path")
			}
		case strings.HasPrefix(a, "-config="):
			path = strings.TrimPrefix(a, "-config=")
		case strings.HasPrefix(a, "--config="):
			path = strings.TrimPrefix(a, "--config=")
		}
	}
	return path, nil
}

// applyFlags binds every field carrying a `flag` tag and applies only those the
// user actually set, so a flag default can never override an env value.
//
// Secret fields intentionally have no flag tag: a secret passed as a flag leaks
// into `ps` output and crash dumps (docs/security-boundaries.md §3).
func (l Loader) applyFlags(cfg *Config) error {
	fs := flag.NewFlagSet(l.Service, flag.ContinueOnError)
	fs.SetOutput(l.Output)
	_ = fs.String("config", "", "path to a YAML config file")

	type binding struct {
		val reflect.Value
		str *string
	}
	bindings := map[string]binding{}

	err := walk(reflect.ValueOf(cfg).Elem(), "", func(f reflect.StructField, v reflect.Value, path string) error {
		name := f.Tag.Get("flag")
		if name == "" {
			return nil
		}
		if v.Type() == reflect.TypeOf(Secret("")) {
			return fmt.Errorf("field %s is a Secret and must not be exposed as a flag", path)
		}
		s := fs.String(name, "", f.Tag.Get("usage"))
		bindings[name] = binding{val: v, str: s}
		return nil
	})
	if err != nil {
		return err
	}

	if err := fs.Parse(l.Args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return ErrHelpRequested
		}
		return fmt.Errorf("parsing flags: %w", err)
	}

	var verrs ValidationErrors
	fs.Visit(func(f *flag.Flag) { // Visit reports only explicitly-set flags
		b, ok := bindings[f.Name]
		if !ok {
			return
		}
		if err := setFromString(b.val, *b.str); err != nil {
			verrs.Add("-"+f.Name, "%s", err.Error())
		}
	})
	if len(verrs.Errors) > 0 {
		return &verrs
	}
	return nil
}

func applyDefaults(cfg *Config) error {
	return walk(reflect.ValueOf(cfg).Elem(), "", func(f reflect.StructField, v reflect.Value, path string) error {
		d, ok := f.Tag.Lookup("default")
		if !ok {
			return nil
		}
		if err := setFromString(v, d); err != nil {
			return fmt.Errorf("%s: bad default %q: %w", path, d, err)
		}
		return nil
	})
}

func applyEnv(cfg *Config, getenv func(string) string) error {
	var verrs ValidationErrors
	err := walk(reflect.ValueOf(cfg).Elem(), "", func(f reflect.StructField, v reflect.Value, _ string) error {
		key := f.Tag.Get("env")
		if key == "" {
			return nil
		}
		raw, ok := lookup(getenv, key)
		if !ok {
			return nil
		}
		if err := setFromString(v, raw); err != nil {
			verrs.Add(key, "%s", err.Error())
		}
		return nil
	})
	if err != nil {
		return err
	}
	if len(verrs.Errors) > 0 {
		return &verrs
	}
	return nil
}

// lookup treats an empty value as unset. A deliberate choice: in Kubernetes an
// unset optional env var is routinely rendered as "", and treating that as an
// explicit empty override would wipe a perfectly good default.
func lookup(getenv func(string) string, key string) (string, bool) {
	v := getenv(key)
	if v == "" {
		return "", false
	}
	return v, true
}

// walk visits every settable leaf field of a struct, recursing into nested
// structs but treating time.Duration and named string types as leaves.
func walk(v reflect.Value, prefix string, fn func(reflect.StructField, reflect.Value, string) error) error {
	t := v.Type()
	for i := range t.NumField() {
		f := t.Field(i)
		fv := v.Field(i)
		if !fv.CanSet() { // unexported
			continue
		}
		path := f.Name
		if prefix != "" {
			path = prefix + "." + f.Name
		}
		if fv.Kind() == reflect.Struct && fv.Type() != reflect.TypeOf(time.Time{}) {
			if err := walk(fv, path, fn); err != nil {
				return err
			}
			continue
		}
		if err := fn(f, fv, path); err != nil {
			return err
		}
	}
	return nil
}

func setFromString(v reflect.Value, s string) error {
	// Duration types must be checked before the int64 kind they are built on.
	if v.Type() == reflect.TypeOf(Duration(0)) {
		d, err := time.ParseDuration(s)
		if err != nil {
			return fmt.Errorf("not a duration (e.g. 30s, 5m): %q", s)
		}
		if d < 0 {
			return fmt.Errorf("duration must not be negative: %q", s)
		}
		v.SetInt(int64(d))
		return nil
	}
	if v.Type() == reflect.TypeOf(time.Duration(0)) {
		d, err := time.ParseDuration(s)
		if err != nil {
			return fmt.Errorf("not a duration (e.g. 30s, 5m): %q", s)
		}
		if d < 0 {
			return fmt.Errorf("duration must not be negative: %q", s)
		}
		v.SetInt(int64(d))
		return nil
	}

	switch v.Kind() {
	case reflect.String:
		v.SetString(s)
		return nil
	case reflect.Bool:
		b, err := strconv.ParseBool(s)
		if err != nil {
			return fmt.Errorf("not a boolean (true/false): %q", s)
		}
		v.SetBool(b)
		return nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(s, 10, v.Type().Bits())
		if err != nil {
			return fmt.Errorf("not an integer: %q", s)
		}
		v.SetInt(n)
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, err := strconv.ParseUint(s, 10, v.Type().Bits())
		if err != nil {
			return fmt.Errorf("not an unsigned integer: %q", s)
		}
		v.SetUint(n)
		return nil
	case reflect.Float32, reflect.Float64:
		f, err := strconv.ParseFloat(s, v.Type().Bits())
		if err != nil {
			return fmt.Errorf("not a number: %q", s)
		}
		v.SetFloat(f)
		return nil
	case reflect.Slice:
		if v.Type().Elem().Kind() != reflect.String {
			return fmt.Errorf("unsupported slice type %s", v.Type())
		}
		parts := []string{}
		for _, p := range strings.Split(s, ",") {
			if p = strings.TrimSpace(p); p != "" {
				parts = append(parts, p)
			}
		}
		v.Set(reflect.ValueOf(parts))
		return nil
	default:
		return fmt.Errorf("unsupported config field type %s", v.Type())
	}
}
