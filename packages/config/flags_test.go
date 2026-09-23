package config

import (
	"reflect"
	"testing"
	"time"
)

// A Secret must never be exposed as a command-line flag: flags leak into `ps`
// output and crash dumps (docs/security-boundaries.md §3).
//
// This test walks the real Config struct, so it keeps holding as fields are added
// in later phases. It lives in the internal test package because it needs the
// unexported walker.
func TestNoSecretFieldHasAFlag(t *testing.T) {
	t.Parallel()

	var cfg Config
	secretType := reflect.TypeOf(Secret(""))

	err := walk(reflect.ValueOf(&cfg).Elem(), "", func(f reflect.StructField, v reflect.Value, path string) error {
		if v.Type() == secretType {
			if name := f.Tag.Get("flag"); name != "" {
				t.Errorf("%s is a Secret but is exposed as flag -%s; secrets must not be passable on the command line", path, name)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk() error = %v", err)
	}
}

// Every field must be reachable by at least one layer, otherwise it is dead
// configuration that an operator cannot set.
func TestEveryFieldIsSettable(t *testing.T) {
	t.Parallel()

	var cfg Config
	err := walk(reflect.ValueOf(&cfg).Elem(), "", func(f reflect.StructField, _ reflect.Value, path string) error {
		if f.Tag.Get("env") == "" && f.Tag.Get("flag") == "" {
			t.Errorf("%s has neither an env nor a flag tag, so it cannot be configured", path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk() error = %v", err)
	}
}

// Environment variable names must be unique and consistently prefixed, since a
// duplicate would silently make one field unsettable.
func TestEnvTagsAreUniqueAndPrefixed(t *testing.T) {
	t.Parallel()

	var cfg Config
	seen := map[string]string{}
	err := walk(reflect.ValueOf(&cfg).Elem(), "", func(f reflect.StructField, _ reflect.Value, path string) error {
		key := f.Tag.Get("env")
		if key == "" {
			return nil
		}
		if len(key) < 7 || key[:7] != "NEBULA_" {
			t.Errorf("%s uses env var %q, which is not prefixed NEBULA_", path, key)
		}
		if prev, dup := seen[key]; dup {
			t.Errorf("env var %q is used by both %s and %s", key, prev, path)
		}
		seen[key] = path
		return nil
	})
	if err != nil {
		t.Fatalf("walk() error = %v", err)
	}
}

func TestSetFromString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		target  any
		in      string
		want    any
		wantErr bool
	}{
		{name: "string", target: new(string), in: "hello", want: "hello"},
		{name: "bool true", target: new(bool), in: "true", want: true},
		{name: "bool 1", target: new(bool), in: "1", want: true},
		{name: "bad bool", target: new(bool), in: "perhaps", wantErr: true},
		{name: "int", target: new(int), in: "42", want: 42},
		{name: "int32", target: new(int32), in: "7", want: int32(7)},
		{name: "bad int", target: new(int), in: "4.2", wantErr: true},
		{name: "int64 overflow", target: new(int32), in: "99999999999", wantErr: true},
		{name: "float", target: new(float64), in: "1.5", want: 1.5},
		{name: "duration", target: new(time.Duration), in: "30s", want: 30 * time.Second},
		{name: "config duration", target: new(Duration), in: "30s", want: Duration(30 * time.Second)},
		{name: "duration minutes", target: new(time.Duration), in: "5m", want: 5 * time.Minute},
		{name: "bad duration", target: new(time.Duration), in: "30 seconds", wantErr: true},
		{name: "negative duration", target: new(time.Duration), in: "-5s", wantErr: true},
		{name: "secret", target: new(Secret), in: "hunter2", want: Secret("hunter2")},
		{name: "env", target: new(Env), in: "staging", want: EnvStaging},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := reflect.ValueOf(tt.target).Elem()
			err := setFromString(v, tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("setFromString(%q) succeeded, want error", tt.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("setFromString(%q) error = %v", tt.in, err)
			}
			if got := v.Interface(); got != tt.want {
				t.Errorf("setFromString(%q) = %v (%T), want %v (%T)", tt.in, got, got, tt.want, tt.want)
			}
		})
	}
}

func TestSetFromStringSlice(t *testing.T) {
	t.Parallel()

	var got []string
	if err := setFromString(reflect.ValueOf(&got).Elem(), "a, b ,c,"); err != nil {
		t.Fatalf("setFromString() error = %v", err)
	}
	want := []string{"a", "b", "c"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("setFromString() = %v, want %v (blank entries trimmed)", got, want)
	}
}
