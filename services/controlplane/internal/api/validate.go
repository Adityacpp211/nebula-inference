package api

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/adityasatwar321/nebula/packages/httpx"
)

// Request validation happens here, not in the database alone.
//
// The database constraints are the backstop and must stay: they catch a bug in
// this file, and they apply to a hand-written UPDATE during an incident. But a
// constraint violation surfaces as "ck_models__name_format" with no indication of
// which field or what the rule is, and a caller cannot act on that. So every rule
// with a constraint behind it is checked here too, with the field named and the
// rule stated — and the patterns are copied from the migrations verbatim so the
// two cannot mean different things. A test asserts a value this file accepts is a
// value the database accepts.

var (
	// modelNamePattern mirrors ck_models__name_format.
	modelNamePattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9._-]{0,62}[a-z0-9])?$`)
	// versionPattern mirrors ck_model_versions__version_format.
	versionPattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9._-]{0,62}[a-z0-9])?$`)
	// deploymentNamePattern mirrors ck_deployments__name_format. Shorter than a
	// model name because it becomes a Kubernetes object name, whose 63-character
	// label limit has to accommodate the suffixes the controller adds.
	deploymentNamePattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,45}[a-z0-9])?$`)
	// namespacePattern mirrors ck_deployments__namespace_format.
	namespacePattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)
	// emailPattern is deliberately loose: the only reliable email validator is
	// sending mail. This rejects what is certainly wrong and nothing more.
	emailPattern = regexp.MustCompile(`^[^@\s]+@[^@\s.]+(\.[^@\s.]+)+$`)
)

// requireName validates an identifier against a pattern.
func requireName(value, field string, pattern *regexp.Regexp, rule string) error {
	if value == "" {
		return httpx.ErrInvalidRequest(field+" is required", "missing_field", field)
	}
	if !pattern.MatchString(value) {
		return httpx.ErrInvalidRequest(rule, "invalid_"+field, field)
	}
	return nil
}

// requireEnum validates a value against a closed set, listing the alternatives.
// Listing them is the difference between a caller fixing their request in one
// attempt and reading the source.
func requireEnum(value, field string, allowed []string) error {
	if value == "" {
		return httpx.ErrInvalidRequest(field+" is required", "missing_field", field)
	}
	for _, a := range allowed {
		if value == a {
			return nil
		}
	}
	return httpx.ErrInvalidRequest(
		fmt.Sprintf("%s must be one of: %s", field, strings.Join(allowed, ", ")),
		"invalid_"+field, field)
}

// requireChecksum validates a lowercase hex SHA-256 and returns its bytes.
func requireChecksum(value, field string) ([]byte, error) {
	if value == "" {
		return nil, httpx.ErrInvalidRequest(field+" is required", "missing_field", field)
	}
	if len(value) != 64 || value != strings.ToLower(value) {
		return nil, httpx.ErrInvalidRequest(
			field+" must be a lowercase hex SHA-256 digest (64 characters)", "invalid_checksum", field)
	}
	raw, err := hex.DecodeString(value)
	if err != nil {
		return nil, httpx.ErrInvalidRequest(
			field+" is not valid hexadecimal", "invalid_checksum", field)
	}
	return raw, nil
}

// requireArtifactURI validates where the weights live.
//
// The scheme set is closed because the artifact service (Phase 3) has to be able
// to fetch it, and an unsupported scheme accepted here would become a deployment
// that fails minutes later with an opaque error. file:// is allowed only outside
// production: a path on one node's disk is not a cluster-wide artifact, and a
// deployment referencing one works until it is scheduled somewhere else.
func requireArtifactURI(value string, allowFile bool) error {
	if value == "" {
		return httpx.ErrInvalidRequest("artifact_uri is required", "missing_field", "artifact_uri")
	}
	u, err := url.Parse(value)
	if err != nil || u.Scheme == "" {
		return httpx.ErrInvalidRequest(
			"artifact_uri must be an absolute URI", "invalid_artifact_uri", "artifact_uri")
	}
	switch u.Scheme {
	case "s3", "https", "hf":
		return nil
	case "file":
		if allowFile {
			return nil
		}
		return unprocessable(
			"file:// artifacts are refused outside development: a path on one node's disk is not reachable from every node",
			"artifact_uri_not_portable", "artifact_uri")
	default:
		return httpx.ErrInvalidRequest(
			"artifact_uri scheme must be one of: s3, https, hf, file (development only)",
			"invalid_artifact_uri", "artifact_uri")
	}
}

// requireJSONObject validates that a raw jsonb field is an object, substituting a
// default when absent. The database CHECK constraints require objects; catching it
// here names the field.
func requireJSONObject(in json.RawMessage, field string) (json.RawMessage, error) {
	if strings.TrimSpace(string(in)) == "" || string(in) == "null" {
		return json.RawMessage(`{}`), nil
	}
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(in, &probe); err != nil {
		return nil, httpx.ErrInvalidRequest(
			field+" must be a JSON object", "invalid_type", field)
	}
	return in, nil
}

// replicaBounds validates the replica triple as a unit.
//
// Validated together rather than field by field because the constraint is a
// relationship: each of min=2, desired=1, max=5 is individually reasonable, and
// the combination is not. The database enforces the same rule
// (ck_deployments__replica_bounds); this version can say which pair is wrong.
func replicaBounds(desired, minReplicas, maxReplicas int32) error {
	switch {
	case minReplicas < 0:
		return httpx.ErrInvalidRequest("min_replicas must not be negative", "invalid_replicas", "min_replicas")
	case maxReplicas < minReplicas:
		return httpx.ErrInvalidRequest(
			fmt.Sprintf("max_replicas (%d) must be at least min_replicas (%d)", maxReplicas, minReplicas),
			"invalid_replicas", "max_replicas")
	case desired < minReplicas || desired > maxReplicas:
		return httpx.ErrInvalidRequest(
			fmt.Sprintf("replicas (%d) must be between min_replicas (%d) and max_replicas (%d)",
				desired, minReplicas, maxReplicas),
			"invalid_replicas", "replicas")
	case maxReplicas > maxReplicasCeiling:
		// A guard against a typo, not a capacity decision: capacity admission is the
		// scheduler's job (Phase 6). 0 replicas is legal — that is how a deployment
		// is scaled to nothing without being deleted.
		return unprocessable(
			fmt.Sprintf("max_replicas (%d) exceeds the per-deployment ceiling of %d",
				maxReplicas, maxReplicasCeiling),
			"replicas_over_ceiling", "max_replicas")
	}
	return nil
}

// maxReplicasCeiling bounds a single deployment's replica count.
const maxReplicasCeiling = 1000
