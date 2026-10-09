package controlplane_test

import (
	"testing"
	"time"

	"github.com/adityasatwar321/nebula/packages/db"
	"github.com/adityasatwar321/nebula/packages/testsupport/dbtest"
)

// addNode writes a node row as the controller's inventory reconciler would.
func (f *fixture) addNode(t *testing.T, name string, cpuMilli, memMiB int, syncedAgo time.Duration) {
	t.Helper()
	_, err := f.Pool.Exec(dbtest.Context(t), `
		INSERT INTO nodes (id, name, labels, taints, capacity, allocatable, requested, schedulable, synced_at)
		VALUES ($1, $2, '{"nebula.dev/accelerator":"cpu"}', '[]', $3, $3, '{}', true, now() - $4::interval)`,
		db.MustNewID(), name,
		map[string]int{"cpu_milli": cpuMilli, "memory_mib": memMiB, "gpu": 0},
		syncedAgo.String())
	if err != nil {
		t.Fatal(err)
	}
}

// The Phase 5 admission test: an inadmissible request is refused with 422 before
// any object — here, any row — is created.
func TestInadmissibleDeploymentIsRefusedBeforeCreation(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	_, versionID := f.createReadyVersion(t, "big", "v1")
	f.addNode(t, "w1-"+f.OrgID.String()[:8], 4000, 8192, 0)

	var env apiError
	code := f.do(t, "POST", "/v1/deployments", map[string]any{
		"name": "too-big", "model_version_id": versionID, "replicas": 1,
		"resources": map[string]int{"cpu_milli": 2000, "memory_mib": 65536},
	}, &env)
	if code != 422 || env.Error.Code != "insufficient_capacity" || env.Error.Param != "resources" {
		t.Fatalf("%d %+v", code, env)
	}
	var n int
	_ = f.Pool.QueryRow(dbtest.Context(t), `SELECT count(*) FROM deployments WHERE name = 'too-big'`).Scan(&n)
	if n != 0 {
		t.Error("a refused deployment must not be written")
	}

	var ok struct {
		Placement struct {
			Admitted    bool   `json:"admitted"`
			Inventory   string `json:"inventory"`
			Constraints struct {
				NodeSelector map[string]string `json:"nodeSelector"`
				Requests     map[string]string `json:"requests"`
			} `json:"constraints"`
			CapacityNote string `json:"capacity_note"`
		} `json:"placement"`
	}
	code = f.do(t, "POST", "/v1/deployments", map[string]any{
		"name": "fits", "model_version_id": versionID, "replicas": 1,
		"resources": map[string]int{"cpu_milli": 1000, "memory_mib": 1024},
	}, &ok)
	if code != 202 || !ok.Placement.Admitted || ok.Placement.Inventory != "observed" ||
		ok.Placement.Constraints.NodeSelector["nebula.dev/accelerator"] != "cpu" ||
		ok.Placement.Constraints.Requests["memory"] != "1Gi" || ok.Placement.CapacityNote == "" {
		t.Fatalf("%d %+v", code, ok.Placement)
	}
}

// Stale inventory is not evidence: admission says it could not check rather than
// refusing on old numbers.
func TestStaleInventoryDoesNotRefuse(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	_, versionID := f.createReadyVersion(t, "stale", "v1")
	f.addNode(t, "old-"+f.OrgID.String()[:8], 1000, 512, time.Hour)

	var out struct {
		Placement struct {
			Admitted  bool   `json:"admitted"`
			Inventory string `json:"inventory"`
		} `json:"placement"`
	}
	code := f.do(t, "POST", "/v1/deployments", map[string]any{
		"name": "stale-dep", "model_version_id": versionID, "replicas": 1,
		"resources": map[string]int{"cpu_milli": 2000, "memory_mib": 4096},
	}, &out)
	if code != 202 || !out.Placement.Admitted || out.Placement.Inventory != "unavailable" {
		t.Fatalf("%d %+v", code, out)
	}
}

// A deleted deployment's name can be used again (migration 000011), while two live
// deployments still cannot share one.
func TestDeletedDeploymentNameCanBeReused(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	_, versionID := f.createReadyVersion(t, "reuse", "v1")
	id := f.createDeployment(t, "reused", versionID)

	var env apiError
	if code := f.do(t, "POST", "/v1/deployments", map[string]any{
		"name": "reused", "model_version_id": versionID, "replicas": 1,
		"resources": map[string]int{"cpu_milli": 500, "memory_mib": 512},
	}, &env); code != 409 {
		t.Fatalf("a second live deployment with the same name: %d", code)
	}

	if code := f.do(t, "POST", "/v1/deployments/"+id.String()+"/stop", map[string]any{}, nil); code >= 300 {
		t.Fatalf("stop: %d", code)
	}
	// Nothing reconciles here, so the test moves stopping -> stopped as the
	// controller would once the pods are gone.
	if code := f.transition(t, id, "stopping", "stopped", "Drained"); code >= 300 {
		t.Fatalf("transition: %d", code)
	}
	if code := f.do(t, "DELETE", "/v1/deployments/"+id.String(), nil, &env); code >= 300 {
		t.Fatalf("delete: %d %+v", code, env)
	}
	f.createDeployment(t, "reused", versionID)
}
