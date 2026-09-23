package controlplane_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/google/uuid"

	"github.com/adityasatwar321/nebula/packages/db/models"
	"github.com/adityasatwar321/nebula/packages/testsupport/dbtest"
	"github.com/adityasatwar321/nebula/services/controlplane/internal/store"
)

// A version becomes immutable at ready, enforced by a database trigger. This is the
// test that proves the trigger exists and fires, which no amount of Go-side checking
// can substitute for: the trigger is what protects a version against a hand-written
// UPDATE during an incident (ADR-0010).
func TestReadyVersionIsImmutableInTheDatabase(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	ctx := dbtest.Context(t)

	_, versionID := f.createReadyVersion(t, "immutable-model", "v1")

	// Every frozen field, one statement each, so a failure names the column that is
	// not actually protected.
	frozen := []struct {
		column string
		value  any
	}{
		{"artifact_uri", "s3://somewhere-else/hijacked"},
		{"checksum_sha256", make([]byte, 32)},
		{"size_bytes", int64(999)},
		{"context_window", int32(128)},
		{"format", "gguf"},
		{"runtime", "llamacpp"},
		{"version", "v2"},
	}

	for _, c := range frozen {
		err := f.inTx(t, func(q store.Querier) error {
			_, err := q.Exec(ctx,
				`UPDATE model_versions SET `+c.column+` = $2 WHERE id = $1`, versionID, c.value)
			return err
		})
		if err == nil {
			t.Errorf("a ready version's %s was changed; the immutability trigger did not fire", c.column)
		}
	}

	// The status may still move to archived, which is the one legal change.
	f.mustInTx(t, func(q store.Querier) error {
		return f.Store.Versions.SetStatus(ctx, q, f.OrgID, versionID,
			models.VersionReady, models.VersionArchived, nil)
	})
}

// model_versions carries no org_id: the tenant check is a join through models. A bug
// there would read another tenant's version, so it is tested with two real tenants
// in one database rather than reasoned about.
func TestVersionReadsAreTenantScoped(t *testing.T) {
	t.Parallel()

	pool := dbtest.Migrated(t)
	alice := newFixtureOn(t, pool)
	bob := newFixtureOn(t, pool)
	ctx := dbtest.Context(t)

	_, aliceVersion := alice.createReadyVersion(t, "alice-model", "v1")

	// Bob asks for Alice's version by id, through the store.
	err := bob.inTx(t, func(q store.Querier) error {
		_, err := bob.Store.Versions.Get(ctx, q, bob.OrgID, aliceVersion)
		return err
	})
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("reading another tenant's version returned %v, want ErrNotFound", err)
	}

	// And over HTTP, where the answer must be a 404 rather than a 403: telling Bob
	// the id exists somewhere else is a disclosure by itself.
	if code := bob.do(t, http.MethodGet, "/v1/model-versions/"+aliceVersion.String(), nil, nil); code != http.StatusNotFound {
		t.Errorf("GET another tenant's version = %d, want 404", code)
	}

	// Alice can still read her own, so the check is scoping rather than a blanket
	// refusal.
	if code := alice.do(t, http.MethodGet, "/v1/model-versions/"+aliceVersion.String(), nil, nil); code != http.StatusOK {
		t.Errorf("GET own version = %d, want 200", code)
	}
}

// Row-level security is the second line of defence: it turns a missing application
// filter into an empty result instead of a leak. The application connects as a
// superuser in these tests, which bypasses RLS, so the policies are exercised by
// switching to the nebula_app role explicitly — otherwise the policies would be
// untested and nobody would know until production (ADR-0022).
func TestRowLevelSecurityIsolatesTenants(t *testing.T) {
	t.Parallel()

	pool := dbtest.Migrated(t)
	alice := newFixtureOn(t, pool)
	bob := newFixtureOn(t, pool)
	ctx := dbtest.Context(t)

	alice.createReadyVersion(t, "rls-model", "v1")

	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquiring a connection: %v", err)
	}
	defer conn.Release()
	// RESET ROLE has to run BEFORE the release, not in a t.Cleanup: cleanups run
	// after the deferred release, and a pooled connection handed back while still
	// SET ROLE nebula_app would silently change what a later test is allowed to do.
	defer func() { _, _ = conn.Exec(ctx, `RESET ROLE`) }()

	if _, err := conn.Exec(ctx, `SET ROLE nebula_app`); err != nil {
		t.Fatalf("switching to nebula_app: %v", err)
	}

	// Scoped to Alice: her model is visible.
	if _, err := conn.Exec(ctx, `SELECT set_config('app.current_org', $1, false)`,
		alice.OrgID.String()); err != nil {
		t.Fatalf("setting app.current_org: %v", err)
	}
	var visible int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM models`).Scan(&visible); err != nil {
		t.Fatalf("counting models as nebula_app: %v", err)
	}
	if visible == 0 {
		t.Fatal("row-level security hid the tenant's own rows")
	}

	// Scoped to Bob: the same unfiltered query returns nothing. This is the property
	// that makes a forgotten WHERE org_id harmless.
	if _, err := conn.Exec(ctx, `SELECT set_config('app.current_org', $1, false)`,
		bob.OrgID.String()); err != nil {
		t.Fatalf("setting app.current_org: %v", err)
	}
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM models`).Scan(&visible); err != nil {
		t.Fatalf("counting models as nebula_app: %v", err)
	}
	if visible != 0 {
		t.Errorf("row-level security let a tenant see %d of another tenant's models", visible)
	}

	// With no tenant set at all the result must be empty rather than everything: a
	// code path that forgets to scope the session must fail closed.
	if _, err := conn.Exec(ctx, `SELECT set_config('app.current_org', '', false)`); err != nil {
		t.Fatalf("clearing app.current_org: %v", err)
	}
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM models`).Scan(&visible); err != nil {
		t.Fatalf("counting models with no tenant set: %v", err)
	}
	if visible != 0 {
		t.Errorf("with no tenant set, %d models were visible; RLS must fail closed", visible)
	}
}

// The application role must not be able to rewrite history. Expressed as a grant
// rather than only as a trigger, because a trigger is one careless migration away
// from not existing.
func TestAppendOnlyTablesRefuseUpdates(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	ctx := dbtest.Context(t)

	_, versionID := f.createReadyVersion(t, "history-model", "v1")
	deploymentID := f.createDeployment(t, "history-dep", versionID)

	// Both tables have rows by now: revision 1 and the creation transition.
	for _, table := range []string{"deployment_revisions", "deployment_state_transitions", "audit_logs"} {
		var rows int
		if err := f.Pool.QueryRow(ctx, `SELECT count(*) FROM `+table).Scan(&rows); err != nil {
			t.Fatalf("counting %s: %v", table, err)
		}
		if rows == 0 {
			t.Errorf("%s is empty; the test cannot prove it is append-only", table)
		}
	}

	conn, err := f.Pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquiring a connection: %v", err)
	}
	defer conn.Release()
	defer func() { _, _ = conn.Exec(ctx, `RESET ROLE`) }()
	if _, err := conn.Exec(ctx, `SET ROLE nebula_app`); err != nil {
		t.Fatalf("switching to nebula_app: %v", err)
	}

	cases := []struct {
		name string
		sql  string
	}{
		{"update a revision", `UPDATE deployment_revisions SET reason = 'rollback'`},
		{"delete a revision", `DELETE FROM deployment_revisions`},
		{"update a transition", `UPDATE deployment_state_transitions SET to_state = 'ready'`},
		{"delete a transition", `DELETE FROM deployment_state_transitions`},
		{"update an audit record", `UPDATE audit_logs SET action = 'nothing.happened'`},
		{"delete an audit record", `DELETE FROM audit_logs`},
		{"rewrite the edge table", `UPDATE deployment_state_edges SET note = 'anything goes'`},
	}
	for _, c := range cases {
		if _, err := conn.Exec(ctx, c.sql); err == nil {
			t.Errorf("nebula_app could %s; that history is meant to be unrewritable", c.name)
		}
	}

	_ = deploymentID
}

// A version must be ready before it can be deployed. The check belongs in the API
// because the alternative is a pod that fails minutes later with the cause buried in
// kubelet events.
func TestOnlyReadyVersionsCanBeDeployed(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	var model struct {
		ID uuid.UUID `json:"id"`
	}
	if code := f.do(t, http.MethodPost, "/v1/models",
		map[string]any{"name": "not-ready", "task": "chat"}, &model); code != http.StatusCreated {
		t.Fatalf("creating model = %d", code)
	}

	checksum := fakeChecksum("not-ready")
	var version struct {
		ID     uuid.UUID `json:"id"`
		Status string    `json:"status"`
	}
	if code := f.do(t, http.MethodPost, "/v1/models/"+model.ID.String()+"/versions", map[string]any{
		"version": "v1", "format": "mock", "runtime": "mock",
		"artifact_uri": "s3://nebula-test/" + checksum, "size_bytes": 10,
		"checksum_sha256": checksum, "context_window": 2048,
	}, &version); code != http.StatusCreated {
		t.Fatalf("creating version = %d", code)
	}
	if version.Status != string(models.VersionUploading) {
		t.Fatalf("new version is %q, want uploading", version.Status)
	}

	env := f.expectError(t, http.StatusUnprocessableEntity, http.MethodPost, "/v1/deployments",
		map[string]any{
			"name":             "premature",
			"model_version_id": version.ID,
		})
	if env.Error.Code != "version_not_ready" {
		t.Errorf("error code = %q, want version_not_ready", env.Error.Code)
	}
	if env.Error.Param != "model_version_id" {
		t.Errorf("error param = %q, want model_version_id", env.Error.Param)
	}
}

// A declared checksum that does not match at finalize fails the version terminally,
// and the failure is recorded. Terminal on purpose: a retryable mismatch lets a
// caller keep guessing until one matches.
func TestChecksumMismatchFailsTheVersionTerminally(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	var model struct {
		ID uuid.UUID `json:"id"`
	}
	f.do(t, http.MethodPost, "/v1/models",
		map[string]any{"name": "mismatch", "task": "chat"}, &model)

	declared := fakeChecksum("declared")
	var version struct {
		ID uuid.UUID `json:"id"`
	}
	f.do(t, http.MethodPost, "/v1/models/"+model.ID.String()+"/versions", map[string]any{
		"version": "v1", "format": "mock", "runtime": "mock",
		"artifact_uri": "s3://nebula-test/" + declared, "size_bytes": 10,
		"checksum_sha256": declared, "context_window": 2048,
	}, &version)

	env := f.expectError(t, http.StatusUnprocessableEntity, http.MethodPost,
		"/v1/model-versions/"+version.ID.String()+"/finalize",
		map[string]any{"checksum_sha256": fakeChecksum("something-else")})
	if env.Error.Code != "checksum_mismatch" {
		t.Errorf("error code = %q, want checksum_mismatch", env.Error.Code)
	}

	// The refusal must be durable: the version is failed, with both digests in the
	// reason, even though the request ended in an error.
	var after struct {
		Status        string  `json:"status"`
		FailureReason *string `json:"failure_reason"`
	}
	if code := f.do(t, http.MethodGet, "/v1/model-versions/"+version.ID.String(), nil, &after); code != http.StatusOK {
		t.Fatalf("GET version = %d", code)
	}
	if after.Status != string(models.VersionFailed) {
		t.Fatalf("version status = %q, want failed", after.Status)
	}
	if after.FailureReason == nil || *after.FailureReason == "" {
		t.Fatal("a failed version carries no failure_reason; the failure would be unexplainable")
	}

	// And a second attempt cannot resurrect it.
	if code := f.do(t, http.MethodPost, "/v1/model-versions/"+version.ID.String()+"/finalize",
		map[string]any{"checksum_sha256": declared}, nil); code != http.StatusConflict {
		t.Errorf("finalizing a failed version = %d, want 409", code)
	}
}

// A model cannot be deleted while a deployment references one of its versions, and
// a version cannot be archived while a deployment references it. Both are reported
// as 409 with a message naming what is blocking.
func TestRegistryDeletionRespectsReferences(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	modelID, versionID := f.createReadyVersion(t, "referenced", "v1")
	deploymentID := f.createDeployment(t, "referencing", versionID)

	env := f.expectError(t, http.StatusConflict, http.MethodDelete,
		"/v1/models/"+modelID.String(), nil)
	if env.Error.Code != "in_use" {
		t.Errorf("deleting a referenced model: code = %q, want in_use", env.Error.Code)
	}

	env = f.expectError(t, http.StatusConflict, http.MethodDelete,
		"/v1/model-versions/"+versionID.String(), nil)
	if env.Error.Code != "in_use" {
		t.Errorf("archiving a referenced version: code = %q, want in_use", env.Error.Code)
	}

	// Once the deployment is gone, both succeed. The deployment has to be stopped
	// first, which is the rule the next test covers in its own right.
	if code := f.transition(t, deploymentID, models.DeploymentPending, models.DeploymentStopping,
		"StopRequested"); code != http.StatusOK {
		t.Fatalf("stopping the deployment = %d", code)
	}
	if code := f.transition(t, deploymentID, models.DeploymentStopping, models.DeploymentStopped,
		"Drained"); code != http.StatusOK {
		t.Fatalf("marking the deployment stopped = %d", code)
	}
	if code := f.do(t, http.MethodDelete, "/v1/deployments/"+deploymentID.String(), nil, nil); code != http.StatusNoContent {
		t.Fatalf("deleting the deployment = %d, want 204", code)
	}

	if code := f.do(t, http.MethodDelete, "/v1/model-versions/"+versionID.String(), nil, nil); code != http.StatusNoContent {
		t.Errorf("archiving an unreferenced version = %d, want 204", code)
	}
	if code := f.do(t, http.MethodDelete, "/v1/models/"+modelID.String(), nil, nil); code != http.StatusNoContent {
		t.Errorf("deleting an unreferenced model = %d, want 204", code)
	}
}

// A model's name is immutable through the API: it appears in Kubernetes object names
// and in every audit record referencing the model.
func TestModelNameCannotBeChanged(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	var model struct {
		ID   uuid.UUID `json:"id"`
		Name string    `json:"name"`
	}
	f.do(t, http.MethodPost, "/v1/models",
		map[string]any{"name": "fixed-name", "task": "chat"}, &model)

	// A rename attempt is rejected as an unknown field rather than silently ignored:
	// silently ignoring it would let a caller believe the rename happened.
	env := f.expectError(t, http.StatusBadRequest, http.MethodPatch,
		"/v1/models/"+model.ID.String(), map[string]any{"name": "new-name"})
	if env.Error.Code != "invalid_json" && env.Error.Code != "unknown_field" {
		t.Logf("rename rejected with code %q: %s", env.Error.Code, env.Error.Message)
	}

	// The mutable metadata still updates.
	family := "qwen"
	var updated struct {
		Name   string  `json:"name"`
		Family *string `json:"family"`
	}
	if code := f.do(t, http.MethodPatch, "/v1/models/"+model.ID.String(),
		map[string]any{"family": family}, &updated); code != http.StatusOK {
		t.Fatalf("PATCH model = %d, want 200", code)
	}
	if updated.Name != "fixed-name" {
		t.Errorf("name changed to %q", updated.Name)
	}
	if updated.Family == nil || *updated.Family != family {
		t.Errorf("family = %v, want %q", updated.Family, family)
	}
}

// Every mutation writes an audit record in the same transaction as the change.
// Sampled across the registry rather than asserted once, because the rule only holds
// if every handler follows it.
func TestMutationsAreAudited(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	modelID, versionID := f.createReadyVersion(t, "audited", "v1")
	deploymentID := f.createDeployment(t, "audited-dep", versionID)

	var page struct {
		Data []struct {
			Action       string          `json:"action"`
			ResourceType string          `json:"resource_type"`
			ResourceID   *uuid.UUID      `json:"resource_id"`
			ActorType    string          `json:"actor_type"`
			ActorLabel   *string         `json:"actor_label"`
			RequestID    *uuid.UUID      `json:"request_id"`
			After        json.RawMessage `json:"after"`
		} `json:"data"`
	}
	if code := f.do(t, http.MethodGet, "/v1/audit-logs?limit=100", nil, &page); code != http.StatusOK {
		t.Fatalf("GET /v1/audit-logs = %d", code)
	}

	byAction := map[string]int{}
	for _, e := range page.Data {
		byAction[e.Action]++

		if e.ActorType != string(models.ActorAPIKey) {
			t.Errorf("audit record for %s has actor_type %q, want api_key", e.Action, e.ActorType)
		}
		if e.ActorLabel == nil || *e.ActorLabel == "" {
			// Denormalised so the record stays readable after the key is deleted.
			t.Errorf("audit record for %s carries no actor_label", e.Action)
		}
		if e.RequestID == nil {
			t.Errorf("audit record for %s carries no request_id, so it cannot be correlated with logs", e.Action)
		}
	}

	for _, want := range []string{
		store.ActionModelCreate,
		store.ActionModelVersionCreate,
		store.ActionModelVersionFinalize,
		store.ActionDeploymentCreate,
	} {
		if byAction[want] == 0 {
			t.Errorf("no audit record for %s", want)
		}
	}

	// Filtering by resource must work, since that is how an incident review finds
	// what happened to one deployment.
	var filtered struct {
		Data []struct {
			Action     string     `json:"action"`
			ResourceID *uuid.UUID `json:"resource_id"`
		} `json:"data"`
	}
	if code := f.do(t, http.MethodGet,
		"/v1/audit-logs?resource_type=deployment&resource_id="+deploymentID.String(),
		nil, &filtered); code != http.StatusOK {
		t.Fatalf("filtered audit query = %d", code)
	}
	if len(filtered.Data) == 0 {
		t.Fatal("no audit records for the deployment")
	}
	for _, e := range filtered.Data {
		if e.ResourceID == nil || *e.ResourceID != deploymentID {
			t.Errorf("filtered query returned a record for %v", e.ResourceID)
		}
	}

	_ = modelID
}
