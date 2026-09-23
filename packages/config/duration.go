package config

import (
	"encoding/json"
	"fmt"
	"time"
)

// Duration is a time.Duration that reads and writes as a human string.
//
// It exists because /healthz serves the effective configuration to operators,
// and a raw time.Duration marshals to nanoseconds: "30000000000" instead of
// "30s". During an incident that is the difference between reading a value and
// decoding one.
//
// It accepts both forms on the way in — "30s" and a bare number of seconds — so
// neither a YAML file nor a JSON one has to know which it is dealing with.
type Duration time.Duration

// Duration returns the underlying time.Duration.
func (d Duration) Duration() time.Duration { return time.Duration(d) }

// String renders the Go duration form, e.g. "1h30m0s".
func (d Duration) String() string { return time.Duration(d).String() }

// MarshalJSON implements json.Marshaler.
func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(d.String()) }

// MarshalYAML implements yaml.Marshaler.
func (d Duration) MarshalYAML() (any, error) { return d.String(), nil }

// UnmarshalJSON implements json.Unmarshaler, accepting "30s" or a number of
// seconds.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	return d.set(v)
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (d *Duration) UnmarshalYAML(unmarshal func(any) error) error {
	var v any
	if err := unmarshal(&v); err != nil {
		return err
	}
	return d.set(v)
}

func (d *Duration) set(v any) error {
	switch t := v.(type) {
	case string:
		parsed, err := time.ParseDuration(t)
		if err != nil {
			return fmt.Errorf("not a duration (e.g. 30s, 5m, 1h): %q", t)
		}
		*d = Duration(parsed)
		return nil
	case int:
		*d = Duration(time.Duration(t) * time.Second)
		return nil
	case int64:
		*d = Duration(time.Duration(t) * time.Second)
		return nil
	case float64:
		*d = Duration(time.Duration(t * float64(time.Second)))
		return nil
	default:
		return fmt.Errorf("not a duration: %v", v)
	}
}
