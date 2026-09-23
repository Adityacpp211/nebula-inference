package controlplane_test

import (
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/adityasatwar321/nebula/packages/db"
	"github.com/adityasatwar321/nebula/packages/db/models"
	"github.com/adityasatwar321/nebula/packages/lifecycle"
	"github.com/adityasatwar321/nebula/packages/testsupport/dbtest"
	"github.com/adityasatwar321/nebula/services/controlplane/internal/store"
)

// A deployment and its first revision appear together or not at all. Tested by
// making the revision insert fail inside the same transaction and then checking that
// the deployment is not there either: a deployment with no recorded spec would have
// nothing to roll back to and nothing for an incident review to read.
func TestDeploymentAndFirstRevisionAreAtomic(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	ctx := dbtest.Context(t)

	_, versionID := f.createReadyVersion(t, "atomic", "v1")

	id := db.MustNewID()
	// A revision id that already exists forces the second statement to fail on the
	// primary key, which is the cheapest way to fail mid-transaction without
	// reaching into the store's internals.
	duplicate := db.MustNewID()

	first := &models.Deployment{
		ID: db.MustNewID(), OrgID: f.OrgID, ModelVersionID: versionID,
		Name: "atomic-one", Namespace: "nebula-test",
		DesiredReplicas: 1, MinReplicas: 1, MaxReplicas: 1,
	}
	f.mustInTx(t, func(q store.Querier) error {
		_, err := f.Store.Deployments.Create(ctx, q, first, duplicate)
		return err
	})

	second := &models.Deployment{
		ID: id, OrgID: f.OrgID, ModelVersionID: versionID,
		Name: "atomic-two", Namespace: "nebula-test",
		DesiredReplicas: 1, MinReplicas: 1, MaxReplicas: 1,
	}
	err := f.inTx(t, func(q store.Querier) error {
		_, err := f.Store.Deployments.Create(ctx, q, second, duplicate)
		return err
	})
	if err == nil {
		t.Fatal("creating a deployment with a colliding revision id succeeded")
	}

	// The deployment row must have rolled back with the revision.
	var exists bool
	if err := f.Pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM deployments WHERE id = $1)`, id).Scan(&exists); err != nil {
		t.Fatalf("checking for the deployment: %v", err)
	}
	if exists {
		t.Error("the deployment survived a failed revision insert: it has no spec history")
	}

	// And the one that succeeded has exactly one revision, numbered 1, with reason
	// create.
	var revisions []struct {
		Revision int32
		Reason   string
	}
	rows, err := f.Pool.Query(ctx,
		`SELECT revision, reason FROM deployment_revisions WHERE deployment_id = $1 ORDER BY revision`,
		first.ID)
	if err != nil {
		t.Fatalf("listing revisions: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var r struct {
			Revision int32
			Reason   string
		}
		if err := rows.Scan(&r.Revision, &r.Reason); err != nil {
			t.Fatalf("scanning revision: %v", err)
		}
		revisions = append(revisions, r)
	}
	if len(revisions) != 1 {
		t.Fatalf("got %d revisions, want 1", len(revisions))
	}
	if revisions[0].Revision != 1 || revisions[0].Reason != string(models.ReasonCreate) {
		t.Errorf("first revision = %d/%s, want 1/create", revisions[0].Revision, revisions[0].Reason)
	}
}

// Creating a deployment records a transition row with no from_state, written by a
// trigger. That row is what makes "when did this deployment come into existence" a
// query rather than an inference from created_at.
func TestCreationRecordsAnInitialTransition(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	_, versionID := f.createReadyVersion(t, "initial", "v1")
	id := f.createDeployment(t, "initial-dep", versionID)

	var page struct {
		Data []struct {
			FromState  *string    `json:"from_state"`
			ToState    string     `json:"to_state"`
			ActorType  string     `json:"actor_type"`
			ActorID    *uuid.UUID `json:"actor_id"`
			Generation int64      `json:"generation"`
		} `json:"data"`
	}
	if code := f.do(t, http.MethodGet,
		"/v1/deployments/"+id.String()+"/transitions", nil, &page); code != http.StatusOK {
		t.Fatalf("GET transitions = %d", code)
	}
	if len(page.Data) != 1 {
		t.Fatalf("got %d transitions after creation, want 1", len(page.Data))
	}

	row := page.Data[0]
	if row.FromState != nil {
		t.Errorf("the creation row has from_state %q, want null", *row.FromState)
	}
	if row.ToState != string(models.DeploymentPending) {
		t.Errorf("to_state = %q, want pending", row.ToState)
	}
	// The trigger reads the actor from the session variables the store sets, which is
	// what attributes a change to the credential that made it.
	if row.ActorType != string(models.ActorAPIKey) {
		t.Errorf("actor_type = %q, want api_key", row.ActorType)
	}
	if row.ActorID == nil || *row.ActorID != f.KeyID {
		t.Errorf("actor_id = %v, want the calling key %v", row.ActorID, f.KeyID)
	}
	if row.Generation != 1 {
		t.Errorf("generation = %d, want 1", row.Generation)
	}
}

// Every transition through the API is recorded, in the same transaction. The history
// is the answer to "why was it degraded at 02:00" a day later, which Kubernetes
// Events cannot give because they expire in about an hour.
func TestTransitionsAreRecordedWithReasons(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	_, versionID := f.createReadyVersion(t, "history", "v1")
	id := f.createDeployment(t, "history-dep", versionID)

	steps := []struct {
		from, to models.DeploymentState
		reason   string
	}{
		{models.DeploymentPending, models.DeploymentProvisioning, "Claimed"},
		{models.DeploymentProvisioning, models.DeploymentStarting, "ObjectsApplied"},
		{models.DeploymentStarting, models.DeploymentReady, "ReplicasReady"},
		{models.DeploymentReady, models.DeploymentDegraded, "ReplicaLost"},
		{models.DeploymentDegraded, models.DeploymentReady, "ReplicasRecovered"},
	}
	for _, s := range steps {
		if code := f.transition(t, id, s.from, s.to, s.reason); code != http.StatusOK {
			t.Fatalf("%s -> %s = %d, want 200", s.from, s.to, code)
		}
	}

	var page struct {
		Data []struct {
			FromState *string `json:"from_state"`
			ToState   string  `json:"to_state"`
			Reason    *string `json:"reason"`
		} `json:"data"`
	}
	if code := f.do(t, http.MethodGet,
		"/v1/deployments/"+id.String()+"/transitions?limit=50", nil, &page); code != http.StatusOK {
		t.Fatalf("GET transitions = %d", code)
	}
	// One creation row plus one per step.
	if len(page.Data) != len(steps)+1 {
		t.Fatalf("got %d transitions, want %d", len(page.Data), len(steps)+1)
	}

	// Newest first, and every step recorded its reason.
	for i, s := range steps {
		row := page.Data[len(steps)-1-i]
		if row.ToState != string(s.to) {
			t.Errorf("transition %d: to_state = %q, want %q", i, row.ToState, s.to)
		}
		if row.FromState == nil || *row.FromState != string(s.from) {
			t.Errorf("transition %d: from_state = %v, want %q", i, row.FromState, s.from)
		}
		if row.Reason == nil || *row.Reason != s.reason {
			t.Errorf("transition %d: reason = %v, want %q", i, row.Reason, s.reason)
		}
	}
}

// state_entered_at is the basis for "stuck in starting for 20 minutes". It must move
// when the state changes and must NOT move when a controller re-reports the same
// state, or the stuck detector never fires.
func TestStateEnteredAtTracksOnlyRealChanges(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	ctx := dbtest.Context(t)

	_, versionID := f.createReadyVersion(t, "timer", "v1")
	id := f.createDeployment(t, "timer-dep", versionID)

	readEnteredAt := func() time.Time {
		var at time.Time
		if err := f.Pool.QueryRow(ctx,
			`SELECT state_entered_at FROM deployments WHERE id = $1`, id).Scan(&at); err != nil {
			t.Fatalf("reading state_entered_at: %v", err)
		}
		return at
	}

	atCreation := readEnteredAt()

	waitFor()
	if code := f.transition(t, id, models.DeploymentPending, models.DeploymentProvisioning,
		"Claimed"); code != http.StatusOK {
		t.Fatalf("transition = %d", code)
	}
	afterChange := readEnteredAt()
	if !afterChange.After(atCreation) {
		t.Fatalf("state_entered_at did not advance on a state change: %v then %v", atCreation, afterChange)
	}

	// A same-state write must not restart the timer. Driven through the store so it
	// is the trigger being tested, not the handler's early return.
	waitFor()
	f.mustInTx(t, func(q store.Querier) error {
		_, err := q.Exec(ctx,
			`UPDATE deployments SET state = 'provisioning', state_message = 'still working' WHERE id = $1`, id)
		return err
	})
	afterSameState := readEnteredAt()
	if !afterSameState.Equal(afterChange) {
		t.Errorf("state_entered_at moved on a same-state write: %v then %v; "+
			"a controller re-reporting its state would reset the stuck timer",
			afterChange, afterSameState)
	}

	// And no history row was written for the non-change.
	var transitions int
	if err := f.Pool.QueryRow(ctx,
		`SELECT count(*) FROM deployment_state_transitions WHERE deployment_id = $1`, id).Scan(&transitions); err != nil {
		t.Fatalf("counting transitions: %v", err)
	}
	if transitions != 2 { // creation + the one real change
		t.Errorf("got %d transition rows, want 2: a same-state write recorded history", transitions)
	}
}

// The database refuses an edge that is not in deployment_state_edges, independently of
// the Go state machine. This is the check that survives a bug in the handler and a
// hand-written UPDATE in psql.
func TestDatabaseRefusesIllegalTransitions(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	ctx := dbtest.Context(t)

	_, versionID := f.createReadyVersion(t, "illegal", "v1")
	id := f.createDeployment(t, "illegal-dep", versionID)

	// pending -> ready is not an edge: nothing has been provisioned.
	err := f.inTx(t, func(q store.Querier) error {
		_, err := q.Exec(ctx, `UPDATE deployments SET state = 'ready' WHERE id = $1`, id)
		return err
	})
	if err == nil {
		t.Fatal("the database allowed pending -> ready")
	}

	// And through the API it is a 422 naming what was allowed instead, rather than a
	// 500 from a constraint violation.
	env := f.expectError(t, http.StatusUnprocessableEntity, http.MethodPost,
		"/v1/deployments/"+id.String()+"/transition",
		map[string]any{"from": "pending", "to": "ready", "reason": "ReplicasReady"})
	if env.Error.Code != "illegal_transition" {
		t.Errorf("code = %q, want illegal_transition", env.Error.Code)
	}
	if !contains(env.Error.Message, "provisioning") {
		t.Errorf("the message does not say what is allowed instead: %q", env.Error.Message)
	}

	// Every edge the Go machine rejects must also be rejected by the database. Checked
	// exhaustively, because a single divergence is a 500 in production.
	for _, from := range lifecycle.States() {
		for _, to := range lifecycle.States() {
			if from == to || lifecycle.CanTransition(from, to) {
				continue
			}
			probeID := f.createDeployment(t, "probe-"+shortID(), versionID)
			// Put the deployment into `from` the legal way, when possible.
			if !driveTo(t, f, probeID, from) {
				continue
			}
			err := f.inTx(t, func(q store.Querier) error {
				_, err := q.Exec(ctx,
					`UPDATE deployments SET state = $2 WHERE id = $1`, probeID, to)
				return err
			})
			if err == nil {
				t.Errorf("the database allowed the illegal edge %s -> %s", from, to)
			}
		}
	}
}

// Two concurrent transitions from the same state: exactly one wins. Without the
// compare-and-set both would appear to succeed and the loser's history row would
// describe a transition that did not happen.
func TestConcurrentTransitionsHaveOneWinner(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	ctx := dbtest.Context(t)

	_, versionID := f.createReadyVersion(t, "race", "v1")
	id := f.createDeployment(t, "race-dep", versionID)

	// Both attempts move pending -> provisioning and pending -> stopping, which are
	// both legal from pending and mutually exclusive.
	var wg sync.WaitGroup
	results := make([]error, 2)
	targets := []models.DeploymentState{models.DeploymentProvisioning, models.DeploymentStopping}
	reasons := []lifecycle.Reason{lifecycle.ReasonClaimed, lifecycle.ReasonStopRequested}

	wg.Add(2)
	for i := range targets {
		go func(i int) {
			defer wg.Done()
			results[i] = f.Store.InTxForOrg(ctx, f.OrgID, f.actor(), func(tx pgx.Tx) error {
				_, err := f.Store.Deployments.TransitionState(ctx, tx, f.OrgID, id,
					models.DeploymentPending, targets[i], reasons[i], nil)
				return err
			})
		}(i)
	}
	wg.Wait()

	succeeded := 0
	for i, err := range results {
		if err == nil {
			succeeded++
			continue
		}
		// The loser must get a conflict, not an opaque failure: its caller can re-read
		// and decide what to do.
		if !errors.Is(err, store.ErrConflict) && !isSerializationFailure(err) {
			t.Errorf("attempt %d failed with %v; want ErrConflict or a serialization failure", i, err)
		}
	}
	if succeeded != 1 {
		t.Fatalf("%d of 2 concurrent transitions succeeded, want exactly 1", succeeded)
	}

	// Exactly one history row was added.
	var transitions int
	if err := f.Pool.QueryRow(ctx,
		`SELECT count(*) FROM deployment_state_transitions WHERE deployment_id = $1`, id).Scan(&transitions); err != nil {
		t.Fatalf("counting transitions: %v", err)
	}
	if transitions != 2 { // creation + one winner
		t.Errorf("got %d transition rows, want 2", transitions)
	}
}

// A stale generation must be refused rather than silently overwriting a change made
// in between. Two operators editing the same deployment from two terminals is
// ordinary; a lost update is not acceptable.
func TestSpecUpdateRefusesAStaleGeneration(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	_, versionID := f.createReadyVersion(t, "generation", "v1")
	id := f.createDeployment(t, "generation-dep", versionID)

	var before struct {
		Generation      int64 `json:"generation"`
		CurrentRevision int32 `json:"current_revision"`
	}
	if code := f.do(t, http.MethodGet, "/v1/deployments/"+id.String(), nil, &before); code != http.StatusOK {
		t.Fatalf("GET deployment = %d", code)
	}
	if before.Generation != 1 || before.CurrentRevision != 1 {
		t.Fatalf("new deployment has generation %d, revision %d; want 1/1",
			before.Generation, before.CurrentRevision)
	}

	var after struct {
		Generation      int64 `json:"generation"`
		CurrentRevision int32 `json:"current_revision"`
		Spec            struct {
			DesiredReplicas int32 `json:"desired_replicas"`
		} `json:"spec"`
	}
	if code := f.do(t, http.MethodPatch, "/v1/deployments/"+id.String(), map[string]any{
		"generation": before.Generation,
		"replicas":   3,
	}, &after); code != http.StatusOK {
		t.Fatalf("PATCH deployment = %d, want 200", code)
	}
	// generation and current_revision advance together: "what spec is generation 2"
	// must always have an answer.
	if after.Generation != before.Generation+1 {
		t.Errorf("generation = %d, want %d", after.Generation, before.Generation+1)
	}
	if after.CurrentRevision != before.CurrentRevision+1 {
		t.Errorf("current_revision = %d, want %d", after.CurrentRevision, before.CurrentRevision+1)
	}
	if after.Spec.DesiredReplicas != 3 {
		t.Errorf("desired_replicas = %d, want 3", after.Spec.DesiredReplicas)
	}

	// The second operator, still holding the old generation, is refused.
	env := f.expectError(t, http.StatusConflict, http.MethodPatch, "/v1/deployments/"+id.String(),
		map[string]any{"generation": before.Generation, "replicas": 4})
	if !contains(env.Error.Message, "generation") {
		t.Errorf("the conflict does not mention the generation: %q", env.Error.Message)
	}

	// And the first operator's change survived.
	var check struct {
		Spec struct {
			DesiredReplicas int32 `json:"desired_replicas"`
		} `json:"spec"`
	}
	f.do(t, http.MethodGet, "/v1/deployments/"+id.String(), nil, &check)
	if check.Spec.DesiredReplicas != 3 {
		t.Errorf("desired_replicas = %d after a refused update, want 3", check.Spec.DesiredReplicas)
	}
}

// A transition does not bump the generation. That single rule is what makes
// generation != observed_generation a trustworthy "there is work to do" predicate:
// if a status write moved it, the controller would reconcile forever.
func TestStatusWritesDoNotBumpGeneration(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	_, versionID := f.createReadyVersion(t, "generation-status", "v1")
	id := f.createDeployment(t, "generation-status-dep", versionID)

	var before struct {
		Generation int64 `json:"generation"`
	}
	f.do(t, http.MethodGet, "/v1/deployments/"+id.String(), nil, &before)

	if code := f.transition(t, id, models.DeploymentPending, models.DeploymentProvisioning,
		"Claimed"); code != http.StatusOK {
		t.Fatalf("transition = %d", code)
	}

	var after struct {
		Generation      int64 `json:"generation"`
		CurrentRevision int32 `json:"current_revision"`
	}
	f.do(t, http.MethodGet, "/v1/deployments/"+id.String(), nil, &after)
	if after.Generation != before.Generation {
		t.Errorf("a state transition moved the generation from %d to %d",
			before.Generation, after.Generation)
	}
	if after.CurrentRevision != 1 {
		t.Errorf("a state transition wrote a revision: current_revision = %d", after.CurrentRevision)
	}
}

// Rollback writes a NEW revision whose spec equals an older one. History is never
// rewritten, so "we rolled back at 14:02" is a fact in the table rather than an
// absence (ADR-0010).
func TestRollbackAppendsRatherThanRewrites(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	_, versionID := f.createReadyVersion(t, "rollback", "v1")
	id := f.createDeployment(t, "rollback-dep", versionID)

	var v1 struct {
		Generation int64 `json:"generation"`
	}
	f.do(t, http.MethodGet, "/v1/deployments/"+id.String(), nil, &v1)

	// Revision 2: three replicas.
	var v2 struct {
		Generation int64 `json:"generation"`
	}
	if code := f.do(t, http.MethodPatch, "/v1/deployments/"+id.String(),
		map[string]any{"generation": v1.Generation, "replicas": 3}, &v2); code != http.StatusOK {
		t.Fatalf("PATCH = %d", code)
	}

	// Roll back to revision 1.
	var rolled struct {
		Generation           int64 `json:"generation"`
		CurrentRevision      int32 `json:"current_revision"`
		RolledBackToRevision int32 `json:"rolled_back_to_revision"`
		Spec                 struct {
			DesiredReplicas int32 `json:"desired_replicas"`
		} `json:"spec"`
	}
	if code := f.do(t, http.MethodPost, "/v1/deployments/"+id.String()+"/rollback",
		map[string]any{"revision": 1}, &rolled); code != http.StatusAccepted {
		t.Fatalf("rollback = %d, want 202", code)
	}
	if rolled.RolledBackToRevision != 1 {
		t.Errorf("rolled_back_to_revision = %d, want 1", rolled.RolledBackToRevision)
	}
	if rolled.CurrentRevision != 3 {
		t.Errorf("current_revision = %d after a rollback, want 3: history must be appended, not rewound",
			rolled.CurrentRevision)
	}
	if rolled.Spec.DesiredReplicas != 2 {
		t.Errorf("desired_replicas = %d after rolling back, want the original 2",
			rolled.Spec.DesiredReplicas)
	}

	// All three revisions are still there, and revision 3's spec hash equals
	// revision 1's — the proof that the rollback restored the same desired state
	// rather than something approximating it.
	var page struct {
		Data []struct {
			Revision int32  `json:"revision"`
			Reason   string `json:"reason"`
			SpecHash string `json:"spec_hash"`
		} `json:"data"`
	}
	if code := f.do(t, http.MethodGet,
		"/v1/deployments/"+id.String()+"/revisions", nil, &page); code != http.StatusOK {
		t.Fatalf("GET revisions = %d", code)
	}
	if len(page.Data) != 3 {
		t.Fatalf("got %d revisions, want 3", len(page.Data))
	}

	byRevision := map[int32]struct {
		Reason string
		Hash   string
	}{}
	for _, r := range page.Data {
		byRevision[r.Revision] = struct {
			Reason string
			Hash   string
		}{r.Reason, r.SpecHash}
	}
	if byRevision[3].Reason != string(models.ReasonRollback) {
		t.Errorf("revision 3 reason = %q, want rollback", byRevision[3].Reason)
	}
	if byRevision[3].Hash != byRevision[1].Hash {
		t.Errorf("revision 3's spec hash %q does not equal revision 1's %q",
			byRevision[3].Hash, byRevision[1].Hash)
	}
	if byRevision[2].Hash == byRevision[1].Hash {
		t.Error("revisions 1 and 2 have the same spec hash, but their replica counts differ")
	}
}

// Stop records the intent and goes no further. Marking a deployment stopped here
// would be a claim about pods that nothing has observed — and in Phase 2 there are no
// pods to observe, which is exactly why the temptation has to be refused.
func TestStopStopsAtStopping(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	_, versionID := f.createReadyVersion(t, "stopping", "v1")
	id := f.createDeployment(t, "stopping-dep", versionID)

	var out struct {
		Status struct {
			State       string  `json:"state"`
			StateReason *string `json:"state_reason"`
		} `json:"status"`
		Note string `json:"note"`
	}
	if code := f.do(t, http.MethodPost, "/v1/deployments/"+id.String()+"/stop", nil, &out); code != http.StatusAccepted {
		t.Fatalf("stop = %d, want 202", code)
	}
	if out.Status.State != string(models.DeploymentStopping) {
		t.Fatalf("state = %q after stop, want stopping", out.Status.State)
	}
	if out.Status.StateReason == nil || *out.Status.StateReason != string(lifecycle.ReasonStopRequested) {
		t.Errorf("state_reason = %v, want StopRequested", out.Status.StateReason)
	}
	// The response must say that nothing has actually been torn down.
	if out.Note == "" {
		t.Error("the response carries no note; a 202 plus 'stopping' reads as work in progress")
	}

	// Stopping again is a conflict rather than a silent success.
	if code := f.do(t, http.MethodPost, "/v1/deployments/"+id.String()+"/stop", nil, nil); code != http.StatusConflict {
		t.Errorf("stopping an already-stopping deployment = %d, want 409", code)
	}

	// Start is refused from stopping: only a stopped deployment can be started.
	if code := f.do(t, http.MethodPost, "/v1/deployments/"+id.String()+"/start", nil, nil); code != http.StatusConflict {
		t.Errorf("starting a stopping deployment = %d, want 409", code)
	}
}

// A deployment must be resting before it can be deleted, enforced in the handler and
// again by ck_deployments__delete_only_when_resting.
func TestDeleteRequiresARestingState(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	ctx := dbtest.Context(t)

	_, versionID := f.createReadyVersion(t, "delete", "v1")
	id := f.createDeployment(t, "delete-dep", versionID)

	// Drive it to ready, which is not resting.
	for _, s := range []struct {
		from, to models.DeploymentState
		reason   string
	}{
		{models.DeploymentPending, models.DeploymentProvisioning, "Claimed"},
		{models.DeploymentProvisioning, models.DeploymentStarting, "ObjectsApplied"},
		{models.DeploymentStarting, models.DeploymentReady, "ReplicasReady"},
	} {
		if code := f.transition(t, id, s.from, s.to, s.reason); code != http.StatusOK {
			t.Fatalf("%s -> %s = %d", s.from, s.to, code)
		}
	}

	env := f.expectError(t, http.StatusConflict, http.MethodDelete, "/v1/deployments/"+id.String(), nil)
	if !contains(env.Error.Message, "stopped") {
		t.Errorf("the refusal does not say what to do first: %q", env.Error.Message)
	}

	// The constraint refuses it too, so a hand-written UPDATE cannot skip the rule.
	err := f.inTx(t, func(q store.Querier) error {
		_, err := q.Exec(ctx, `UPDATE deployments SET deleted_at = now() WHERE id = $1`, id)
		return err
	})
	if err == nil {
		t.Error("the database allowed a ready deployment to be marked deleted")
	}

	// Stopped, it deletes.
	if code := f.transition(t, id, models.DeploymentReady, models.DeploymentStopping,
		"StopRequested"); code != http.StatusOK {
		t.Fatalf("stopping = %d", code)
	}
	if code := f.transition(t, id, models.DeploymentStopping, models.DeploymentStopped,
		"Drained"); code != http.StatusOK {
		t.Fatalf("draining = %d", code)
	}
	if code := f.do(t, http.MethodDelete, "/v1/deployments/"+id.String(), nil, nil); code != http.StatusNoContent {
		t.Fatalf("deleting a stopped deployment = %d, want 204", code)
	}
	if code := f.do(t, http.MethodGet, "/v1/deployments/"+id.String(), nil, nil); code != http.StatusNotFound {
		t.Errorf("GET a deleted deployment = %d, want 404", code)
	}
}

// Phase 2 reports honestly that nothing has been reconciled. If this test ever fails
// because reconciled became true, it means either the controller shipped (and this
// test should change with it) or something is claiming work it did not do.
func TestStatusReportsNoReconciliation(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	_, versionID := f.createReadyVersion(t, "unreconciled", "v1")
	id := f.createDeployment(t, "unreconciled-dep", versionID)

	var status struct {
		Generation int64 `json:"generation"`
		Status     struct {
			ObservedGeneration int64    `json:"observed_generation"`
			Reconciled         bool     `json:"reconciled"`
			ReadyReplicas      int32    `json:"ready_replicas"`
			Serving            bool     `json:"serving"`
			NextStates         []string `json:"next_states"`
			StateAgeSeconds    int64    `json:"state_age_seconds"`
		} `json:"status"`
	}
	if code := f.do(t, http.MethodGet,
		"/v1/deployments/"+id.String()+"/status", nil, &status); code != http.StatusOK {
		t.Fatalf("GET status = %d", code)
	}

	if status.Status.Reconciled {
		t.Error("status.reconciled is true, but no controller runs in this build")
	}
	if status.Status.ObservedGeneration != 0 {
		t.Errorf("observed_generation = %d, but nothing has observed anything", status.Status.ObservedGeneration)
	}
	if status.Status.ReadyReplicas != 0 {
		t.Errorf("ready_replicas = %d, but no pods exist", status.Status.ReadyReplicas)
	}
	if status.Status.Serving {
		t.Error("a pending deployment is reported as serving")
	}
	if len(status.Status.NextStates) == 0 {
		t.Error("no next states were reported, so a client cannot tell what is possible")
	}
	if status.Status.StateAgeSeconds < 0 {
		t.Errorf("state_age_seconds = %d", status.Status.StateAgeSeconds)
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// driveTo moves a deployment into a state through legal transitions, reporting
// whether it managed to. Used by the exhaustive edge test, where some states need a
// path rather than a single step.
func driveTo(t *testing.T, f *fixture, id uuid.UUID, target models.DeploymentState) bool {
	t.Helper()

	paths := map[models.DeploymentState][]struct {
		from, to models.DeploymentState
		reason   string
	}{
		models.DeploymentPending: nil,
		models.DeploymentProvisioning: {
			{models.DeploymentPending, models.DeploymentProvisioning, "Claimed"},
		},
		models.DeploymentStarting: {
			{models.DeploymentPending, models.DeploymentProvisioning, "Claimed"},
			{models.DeploymentProvisioning, models.DeploymentStarting, "ObjectsApplied"},
		},
		models.DeploymentReady: {
			{models.DeploymentPending, models.DeploymentProvisioning, "Claimed"},
			{models.DeploymentProvisioning, models.DeploymentStarting, "ObjectsApplied"},
			{models.DeploymentStarting, models.DeploymentReady, "ReplicasReady"},
		},
		models.DeploymentDegraded: {
			{models.DeploymentPending, models.DeploymentProvisioning, "Claimed"},
			{models.DeploymentProvisioning, models.DeploymentStarting, "ObjectsApplied"},
			{models.DeploymentStarting, models.DeploymentDegraded, "ReplicasReady"},
		},
		models.DeploymentFailed: {
			{models.DeploymentPending, models.DeploymentFailed, "InsufficientCapacity"},
		},
		models.DeploymentStopping: {
			{models.DeploymentPending, models.DeploymentStopping, "StopRequested"},
		},
		models.DeploymentStopped: {
			{models.DeploymentPending, models.DeploymentStopping, "StopRequested"},
			{models.DeploymentStopping, models.DeploymentStopped, "Drained"},
		},
	}

	steps, ok := paths[target]
	if !ok {
		t.Fatalf("no path defined to %s", target)
	}
	for _, s := range steps {
		if code := f.transition(t, id, s.from, s.to, s.reason); code != http.StatusOK {
			t.Errorf("driving to %s: %s -> %s = %d", target, s.from, s.to, code)
			return false
		}
	}
	return true
}

// shortID is a short unique suffix for deployment names, which are limited to 47
// characters by the Kubernetes object-name rules.
func shortID() string {
	return uuid.NewString()[:8]
}

func contains(haystack, needle string) bool { return strings.Contains(haystack, needle) }

// isSerializationFailure reports whether PostgreSQL rejected a transaction because
// of a concurrent one. Accepted alongside ErrConflict in the race test: either is a
// correct outcome for the loser, and which one occurs depends on timing.
func isSerializationFailure(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "40001") || strings.Contains(msg, "40P01") ||
		strings.Contains(msg, "could not serialize")
}
