package config

import (
	"encoding/json"
	"fmt"
)

// Redacted is the placeholder rendered in place of any secret value.
const Redacted = "[REDACTED]"

// Secret is a string that refuses to render itself.
//
// It implements Stringer, GoStringer, json.Marshaler, yaml.Marshaler and
// encoding.TextMarshaler, so a secret cannot reach a log line, an error message,
// a %v/%+v/%#v format, a JSON response, or a config dump by accident. Reaching
// the real value requires calling Reveal(), which is greppable in review.
//
// This is the mechanism behind "never store plaintext secrets" and
// "GET /healthz reports the effective configuration with secret values
// redacted" — see docs/security-boundaries.md §3.
type Secret string

// String implements fmt.Stringer.
func (s Secret) String() string { return Redacted }

// GoString implements fmt.GoStringer so %#v is also safe.
func (s Secret) GoString() string { return Redacted }

// MarshalJSON implements json.Marshaler.
func (s Secret) MarshalJSON() ([]byte, error) { return json.Marshal(Redacted) }

// MarshalYAML implements yaml.Marshaler.
func (s Secret) MarshalYAML() (any, error) { return Redacted, nil }

// MarshalText implements encoding.TextMarshaler.
func (s Secret) MarshalText() ([]byte, error) { return []byte(Redacted), nil }

// UnmarshalJSON implements json.Unmarshaler. Reading a secret in is fine; it is
// writing one out that is forbidden.
func (s *Secret) UnmarshalJSON(b []byte) error {
	var v string
	if err := json.Unmarshal(b, &v); err != nil {
		return fmt.Errorf("secret must be a string: %w", err)
	}
	*s = Secret(v)
	return nil
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (s *Secret) UnmarshalYAML(unmarshal func(any) error) error {
	var v string
	if err := unmarshal(&v); err != nil {
		return fmt.Errorf("secret must be a string: %w", err)
	}
	*s = Secret(v)
	return nil
}

// Reveal returns the underlying value. Every call site is a deliberate decision.
func (s Secret) Reveal() string { return string(s) }

// IsZero reports whether the secret is unset.
func (s Secret) IsZero() bool { return s == "" }
