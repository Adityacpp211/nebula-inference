package config

import (
	"encoding/json"
	"fmt"
)

// Redact returns the effective configuration as a generic map with every Secret
// replaced by the redaction placeholder.
//
// It round-trips through JSON on purpose: Secret.MarshalJSON is the single place
// that decides how a secret renders, so Redact cannot drift from it and there is
// no second list of "fields to hide" to keep in sync.
//
// This is what GET /healthz serves, so "what is this pod actually running with"
// is answerable without a shell (docs/deployment-architecture.md §9).
func (c *Config) Redact() (map[string]any, error) {
	b, err := json.Marshal(c)
	if err != nil {
		return nil, fmt.Errorf("marshalling config: %w", err)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, fmt.Errorf("unmarshalling redacted config: %w", err)
	}
	out["service"] = c.service
	return out, nil
}

// MarshalJSON gives Config a stable JSON shape. It is declared explicitly so the
// unexported service field is not silently dropped from /healthz output.
func (c Config) MarshalJSON() ([]byte, error) {
	type alias struct {
		Service  string         `json:"service"`
		Env      Env            `json:"env"`
		Log      LogConfig      `json:"log"`
		HTTP     HTTPConfig     `json:"http"`
		Database DatabaseConfig `json:"database"`
		Dev      DevConfig      `json:"dev"`
	}
	return json.Marshal(alias{
		Service:  c.service,
		Env:      c.Env,
		Log:      c.Log,
		HTTP:     c.HTTP,
		Database: c.Database,
		Dev:      c.Dev,
	})
}
