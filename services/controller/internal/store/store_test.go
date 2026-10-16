package store_test

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/adityasatwar321/nebula/packages/db"
	"github.com/adityasatwar321/nebula/packages/telemetry"
	"github.com/adityasatwar321/nebula/packages/testsupport/dbtest"
	"github.com/adityasatwar321/nebula/services/controller/internal/store"
)

// fixture: a migrated database with one org, one ready version and one pending
// deployment, and a controller store connected AS nebula_controller — so the
// grants in migration 000007 are what these tests exercise, not a superuser.
func fixture(t *testing.T) (*store.Store, uuid.UUID, func(string, ...any) error) {
	t.Helper()
	admin := dbtest.Migrated(t)
	ctx := dbtest.Context(t)
	org, model, version, dep := db.MustNewID(), db.MustNewID(), db.MustNewID(), db.MustNewID()
	for _, stmt := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO organizations (id, slug, name, default_namespace) VALUES ($1, 'acme', 'Acme', 'nebula-acme')`, []any{org}},
		{`INSERT INTO models (id, org_id, name, task) VALUES ($1, $2, 'qwen', 'chat')`, []any{model, org}},
		{`INSERT INTO model_versions (id, model_id, version, format, runtime, size_bytes, checksum_sha256,
		     context_window, artifact_uri, hardware_profile, status, ready_at)
		  VALUES ($1, $2, 'v1', 'mock', 'mock', 10, decode(repeat('ab', 32), 'hex'), 512,
		     's3://b/sha256/' || repeat('ab', 32), '{}', 'ready', now())`, []any{version, model}},
		{`INSERT INTO deployments (id, org_id, model_version_id, name, namespace, resources)
		  VALUES ($1, $2, $3, 'dep', 'nebula-acme', '{"cpu_milli":500,"memory_mib":256}')`, []any{dep, org, version}},
	} {
		if _, err := admin.Exec(ctx, stmt.sql, stmt.args...); err != nil {
			t.Fatalf("seeding: %v", err)
		}
	}

	url := admin.Config().ConnString()
	cfg := dbtest.Config(url)
	cfg.Role = "nebula_controller"
	pool, err := db.Open(ctx, cfg, "controller-test", telemetry.Discard())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	asController := func(sql string, args ...any) error {
		_, err := pool.Exec(ctx, sql, args...)
		return err
	}
	return store.New(pool), dep, asController
}

func TestControllerRoleCanDoItsJob(t *testing.T) {
	t.Parallel()
	st, id, _ := fixture(t)
	ctx := dbtest.Context(t)

	d, err := st.Get(ctx, id)
	if err != nil {
		t.Fatalf("the controller must read deployments across orgs: %v", err)
	}
	if d.OrgSlug != "acme" || d.Version.Ref() != "qwen:v1" || d.State != "pending" || len(d.Version.ChecksumHex) != 64 {
		t.Errorf("deployment: %+v", d)
	}

	ok, err := st.Transition(ctx, id, "pending", "provisioning", "Claimed", "applying")
	if err != nil || !ok {
		t.Fatalf("transition: %v %v", ok, err)
	}
	// Compare-and-set: a stale from-state changes nothing and is not an error.
	if ok, err := st.Transition(ctx, id, "pending", "provisioning", "x", ""); err != nil || ok {
		t.Errorf("stale transition: %v %v", ok, err)
	}
	// The trigger refuses an illegal edge even for the controller.
	if _, err := st.Transition(ctx, id, "provisioning", "stopped", "x", ""); err == nil {
		t.Error("an illegal edge must be refused by the database")
	}

	if err := st.WriteStatus(ctx, id, store.Status{ObservedGeneration: 1, ReadyReplicas: 1, UpdatedReplicas: 1,
		Conditions: []store.Condition{{Type: "Available", Status: "True", Reason: "ReplicasReady", LastTransitionTime: time.Now()}}}); err != nil {
		t.Fatalf("status: %v", err)
	}
	// observed_generation never moves backwards.
	if err := st.WriteStatus(ctx, id, store.Status{ObservedGeneration: 0}); err != nil {
		t.Fatal(err)
	}
	d, _ = st.Get(ctx, id)
	if d.ObservedGeneration != 1 || len(d.Conditions) != 0 {
		t.Errorf("observed generation %d, conditions %+v (the second write had none)", d.ObservedGeneration, d.Conditions)
	}

	if err := st.SyncNodes(ctx, []store.Node{{Name: "w1", Schedulable: true,
		Allocatable: store.Amounts{CPUMilli: 4000, MemoryMiB: 8192}, Requested: store.Amounts{CPUMilli: 500}}}); err != nil {
		t.Fatalf("nodes: %v", err)
	}
	if err := st.SyncNodes(ctx, []store.Node{{Name: "w2", Schedulable: true}}); err != nil {
		t.Fatalf("nodes resync: %v", err)
	}
	if err := st.RecordWorkerEvent(ctx, store.WorkerEvent{OrgID: d.OrgID, DeploymentID: id, Pod: "p", Type: "ready",
		OccurredAt: time.Now()}); err != nil {
		t.Fatalf("worker event: %v", err)
	}

	live, err := st.ListLiveIDs(ctx)
	if err != nil || !live[id] {
		t.Errorf("live ids: %v %v", live, err)
	}
	work, err := st.ListNeedingWork(ctx)
	if err != nil || len(work) != 1 {
		t.Errorf("needing work: %v %v", work, err)
	}

	// The transition history records the controller as the system actor.
	var actor string
	if err := st.Pool().QueryRow(ctx, `SELECT actor_type FROM deployment_state_transitions
		WHERE deployment_id = $1 AND to_state = 'provisioning'`, id).Scan(&actor); err != nil || actor != "system" {
		t.Errorf("history actor %q %v", actor, err)
	}
}

// The permission model is enforced by PostgreSQL: the controller role cannot
// rewrite anything a user wrote.
func TestControllerRoleCannotWriteDesiredState(t *testing.T) {
	t.Parallel()
	_, id, exec := fixture(t)
	for _, sql := range []string{
		`UPDATE deployments SET resources = '{"cpu_milli":1}' WHERE id = $1`,
		`UPDATE deployments SET name = 'renamed' WHERE id = $1`,
		`UPDATE deployments SET model_version_id = model_version_id WHERE id = $1`,
		`DELETE FROM deployments WHERE id = $1`,
		`UPDATE model_versions SET status = 'archived' WHERE id = (SELECT model_version_id FROM deployments WHERE id = $1)`,
	} {
		err := exec(sql, id)
		if err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Errorf("%s: want permission denied, got %v", sql, err)
		}
	}
}
