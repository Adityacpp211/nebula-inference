package store_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/google/uuid"

	"github.com/adityasatwar321/nebula/packages/db/models"
	"github.com/adityasatwar321/nebula/services/controlplane/internal/store"
)

func baseDeployment() *models.Deployment {
	policy := uuid.MustParse("01920000-0000-7000-8000-000000000001")
	return &models.Deployment{
		ID:               uuid.MustParse("01920000-0000-7000-8000-00000000000a"),
		OrgID:            uuid.MustParse("01920000-0000-7000-8000-00000000000b"),
		ModelVersionID:   uuid.MustParse("01920000-0000-7000-8000-00000000000c"),
		Name:             "spec-test",
		Namespace:        "nebula-test",
		DesiredReplicas:  2,
		MinReplicas:      1,
		MaxReplicas:      4,
		Resources:        json.RawMessage(`{"cpu_milli":4000,"memory_mib":8192}`),
		RuntimeOverrides: json.RawMessage(`{"n_ctx":8192}`),
		Autoscaling:      json.RawMessage(`{"enabled":true}`),
		QueueConfig:      json.RawMessage(`{"max_depth":128}`),
		RoutingPolicyID:  &policy,
		Generation:       3,
		CurrentRevision:  3,
	}
}

// The spec hash must depend only on desired state. Two deployments whose desired
// state is identical but whose observed state differs must hash the same, or the
// controller would see spurious work every time a status was written.
func TestSpecHashIgnoresObservedState(t *testing.T) {
	t.Parallel()

	a := baseDeployment()
	b := baseDeployment()

	b.State = models.DeploymentReady
	b.StateReason = ptr("ReplicasReady")
	b.ReadyReplicas = 2
	b.UpdatedReplicas = 2
	b.ObservedGeneration = 3
	b.Conditions = json.RawMessage(`[{"type":"Available","status":"True"}]`)
	b.LastError = ptr("a transient failure that has since cleared")
	b.Generation = 99
	b.CurrentRevision = 42

	if hashOf(t, a) != hashOf(t, b) {
		t.Error("the spec hash changed when only observed state differed; " +
			"the controller would see work to do on every status write")
	}
}

// Whitespace in a jsonb field must not change the hash. json.RawMessage is emitted
// verbatim, so without compaction a reformatted request body would look like a spec
// change.
func TestSpecHashIsWhitespaceInsensitive(t *testing.T) {
	t.Parallel()

	a := baseDeployment()
	b := baseDeployment()
	b.Resources = json.RawMessage("{\n  \"cpu_milli\": 4000,\n  \"memory_mib\": 8192\n}")

	if hashOf(t, a) != hashOf(t, b) {
		t.Error("reformatting a jsonb field changed the spec hash")
	}
}

// Every field that IS part of the spec must change the hash. Checked one at a time so
// a failure names the field that is silently ignored — a field missing from the
// canonical encoding is a change the controller would never notice.
func TestSpecHashCoversEverySpecField(t *testing.T) {
	t.Parallel()

	baseline := hashOf(t, baseDeployment())
	otherVersion := uuid.MustParse("01920000-0000-7000-8000-0000000000ff")
	otherPolicy := uuid.MustParse("01920000-0000-7000-8000-0000000000fe")

	cases := []struct {
		field  string
		mutate func(*models.Deployment)
	}{
		{"model_version_id", func(d *models.Deployment) { d.ModelVersionID = otherVersion }},
		{"namespace", func(d *models.Deployment) { d.Namespace = "somewhere-else" }},
		{"desired_replicas", func(d *models.Deployment) { d.DesiredReplicas = 3 }},
		{"min_replicas", func(d *models.Deployment) { d.MinReplicas = 0 }},
		{"max_replicas", func(d *models.Deployment) { d.MaxReplicas = 8 }},
		{"resources", func(d *models.Deployment) {
			d.Resources = json.RawMessage(`{"cpu_milli":8000,"memory_mib":8192}`)
		}},
		{"runtime_overrides", func(d *models.Deployment) {
			d.RuntimeOverrides = json.RawMessage(`{"n_ctx":4096}`)
		}},
		{"autoscaling", func(d *models.Deployment) {
			d.Autoscaling = json.RawMessage(`{"enabled":false}`)
		}},
		{"queue_config", func(d *models.Deployment) {
			d.QueueConfig = json.RawMessage(`{"max_depth":256}`)
		}},
		{"routing_policy_id", func(d *models.Deployment) { d.RoutingPolicyID = &otherPolicy }},
		{"routing_policy_id cleared", func(d *models.Deployment) { d.RoutingPolicyID = nil }},
	}

	for _, c := range cases {
		d := baseDeployment()
		c.mutate(d)
		if hashOf(t, d) == baseline {
			t.Errorf("changing %s did not change the spec hash", c.field)
		}
	}
}

// The hash must be stable across calls and across processes, because it is stored and
// compared later. Map iteration order or a timestamp creeping into the encoding would
// break that silently.
func TestSpecHashIsDeterministic(t *testing.T) {
	t.Parallel()

	first := hashOf(t, baseDeployment())
	for i := 0; i < 20; i++ {
		if got := hashOf(t, baseDeployment()); got != first {
			t.Fatalf("hash %d = %s, first = %s", i, got, first)
		}
	}
}

// Absent jsonb fields must encode as {} rather than as null, because the columns are
// NOT NULL and a null would be rejected at insert time — after the hash was computed.
func TestSpecOfDefaultsEmptyJSONFields(t *testing.T) {
	t.Parallel()

	d := baseDeployment()
	d.Resources = nil
	d.RuntimeOverrides = nil
	d.Autoscaling = nil
	d.QueueConfig = nil

	spec, err := store.SpecOf(d)
	if err != nil {
		t.Fatalf("SpecOf: %v", err)
	}
	for name, value := range map[string]json.RawMessage{
		"resources":         spec.Resources,
		"runtime_overrides": spec.RuntimeOverrides,
		"autoscaling":       spec.Autoscaling,
		"queue_config":      spec.QueueConfig,
	} {
		if string(value) != "{}" {
			t.Errorf("%s encoded as %q, want {}", name, value)
		}
	}
}

// Apply must be the inverse of SpecOf for spec fields and must leave observed fields
// alone. Rollback depends on both halves: it restores an old spec without resetting
// what the cluster currently reports.
func TestSpecApplyRoundTripsWithoutTouchingStatus(t *testing.T) {
	t.Parallel()

	source := baseDeployment()
	source.DesiredReplicas = 1
	source.Namespace = "old-namespace"
	spec, err := store.SpecOf(source)
	if err != nil {
		t.Fatalf("SpecOf: %v", err)
	}

	target := baseDeployment()
	target.State = models.DeploymentDegraded
	target.ReadyReplicas = 1
	target.ObservedGeneration = 7
	target.Generation = 9
	target.CurrentRevision = 9
	target.LastError = ptr("a replica was lost")

	spec.Apply(target)

	if target.DesiredReplicas != 1 || target.Namespace != "old-namespace" {
		t.Errorf("Apply did not restore the spec: replicas %d, namespace %q",
			target.DesiredReplicas, target.Namespace)
	}
	if target.State != models.DeploymentDegraded || target.ReadyReplicas != 1 ||
		target.ObservedGeneration != 7 {
		t.Error("Apply overwrote observed state")
	}
	if target.Generation != 9 || target.CurrentRevision != 9 {
		t.Error("Apply changed the generation or revision; those advance only through UpdateSpec")
	}
	if hashOf(t, target) != hashOf(t, source) {
		t.Error("a deployment with an applied spec does not hash like its source")
	}
}

// A jsonb field that is not valid JSON is reported rather than hashed, so a corrupt
// value fails before it becomes a revision nobody can decode.
func TestSpecOfRejectsInvalidJSON(t *testing.T) {
	t.Parallel()

	d := baseDeployment()
	d.Autoscaling = json.RawMessage(`{"enabled":`)

	if _, err := store.SpecOf(d); err == nil {
		t.Fatal("SpecOf accepted a truncated jsonb value")
	}
}

// hashOf returns the spec hash as hex, so tests can compare it with == and report a
// readable value when they fail.
func hashOf(t *testing.T, d *models.Deployment) string {
	t.Helper()
	spec, err := store.SpecOf(d)
	if err != nil {
		t.Fatalf("SpecOf: %v", err)
	}
	_, hash, err := spec.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(hash) != sha256.Size {
		t.Fatalf("spec hash is %d bytes, want %d: the column requires exactly that",
			len(hash), sha256.Size)
	}
	return hex.EncodeToString(hash)
}

func ptr[T any](v T) *T { return &v }
