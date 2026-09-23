package lifecycle_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/adityasatwar321/nebula/packages/db/models"
	"github.com/adityasatwar321/nebula/packages/lifecycle"
)

// The Go graph and the deployment_state_edges rows in migration 000009 must be the
// same graph. If they differ, the API accepts transitions the database refuses (a
// 500 where a 422 belonged) or refuses transitions it would allow (a controller that
// cannot make progress). This is the test that keeps them honest, and it reads the
// SQL rather than a copy of it.
func TestEdgesMatchMigration(t *testing.T) {
	t.Parallel()

	fromSQL := edgesFromMigration(t)
	fromGo := map[string]string{}
	for _, e := range lifecycle.Edges() {
		fromGo[string(e.From)+"->"+string(e.To)] = e.Note
	}

	for edge := range fromSQL {
		if _, ok := fromGo[edge]; !ok {
			t.Errorf("deployment_state_edges allows %s but packages/lifecycle does not: "+
				"the API would refuse a transition the database permits", edge)
		}
	}
	for edge := range fromGo {
		if _, ok := fromSQL[edge]; !ok {
			t.Errorf("packages/lifecycle allows %s but deployment_state_edges does not: "+
				"the API would accept a transition the trigger raises on", edge)
		}
	}

	// The notes must agree too. They are what an operator reads in
	// GET /v1/lifecycle/deployment-states and in a psql session, and two different
	// explanations of the same edge is how a graph starts drifting.
	for edge, sqlNote := range fromSQL {
		goNote, ok := fromGo[edge]
		if !ok {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(sqlNote), strings.TrimSpace(goNote)) {
			t.Errorf("edge %s is explained differently:\n  SQL: %q\n  Go:  %q", edge, sqlNote, goNote)
		}
	}
}

// edgesFromMigration parses the seeded deployment_state_edges rows.
func edgesFromMigration(t *testing.T) map[string]string {
	t.Helper()

	body, err := os.ReadFile(filepath.Join("..", "..", "migrations",
		"000009_deployment_lifecycle.up.sql"))
	if err != nil {
		t.Fatalf("reading migration 000009: %v", err)
	}
	sql := string(body)

	start := strings.Index(sql, "INSERT INTO deployment_state_edges")
	if start < 0 {
		t.Fatal("migration 000009 no longer seeds deployment_state_edges")
	}
	// The terminating semicolon has to be found outside string literals: the notes
	// contain semicolons ("objects applied; pods are starting"), and stopping at the
	// first one would silently parse a single row and make this test pass for the
	// wrong reason.
	block, ok := statementAt(sql[start:])
	if !ok {
		t.Fatal("the deployment_state_edges INSERT is unterminated")
	}

	// ('from', 'to', 'note')
	row := regexp.MustCompile(`\(\s*'([a-z]+)'\s*,\s*'([a-z]+)'\s*,\s*'((?:[^']|'')*)'\s*\)`)
	matches := row.FindAllStringSubmatch(block, -1)
	if len(matches) == 0 {
		t.Fatal("could not parse any rows out of the deployment_state_edges INSERT")
	}

	out := make(map[string]string, len(matches))
	for _, m := range matches {
		key := m[1] + "->" + m[2]
		if _, dup := out[key]; dup {
			t.Errorf("deployment_state_edges seeds %s twice", key)
		}
		// Undo SQL's doubled single quotes.
		out[key] = strings.ReplaceAll(m[3], "''", "'")
	}
	return out
}

// statementAt returns the first SQL statement in s, respecting single-quoted
// literals and their doubled-quote escape.
func statementAt(s string) (string, bool) {
	inQuote := false
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\'':
			if inQuote && i+1 < len(s) && s[i+1] == '\'' {
				i++ // an escaped quote inside a literal
				continue
			}
			inQuote = !inQuote
		case ';':
			if !inQuote {
				return s[:i], true
			}
		}
	}
	return "", false
}

func TestStatesMatchTheDatabaseEnum(t *testing.T) {
	t.Parallel()

	// packages/lifecycle is the graph; packages/db/models is the mirror of the enum.
	// They must list the same states, or an edge could name a state no column can
	// hold.
	got := lifecycle.States()
	want := models.DeploymentStates()

	if len(got) != len(want) {
		t.Fatalf("lifecycle has %d states, models has %d", len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("state %d: lifecycle has %q, models has %q", i, got[i], want[i])
		}
	}

	for _, s := range got {
		if !lifecycle.ValidState(s) {
			t.Errorf("ValidState(%q) = false for a listed state", s)
		}
	}
	if lifecycle.ValidState(models.DeploymentState("progressing")) {
		t.Error("ValidState accepted 'progressing', which migration 000009 removed")
	}
	if lifecycle.ValidState("") {
		t.Error("ValidState accepted the empty state")
	}
}

// Every state must be reachable from pending and every state must be able to reach
// a resting state. A state satisfying neither is a trap: a deployment that enters it
// can never be deleted, and nothing can create it in the first place.
func TestNoTrapStates(t *testing.T) {
	t.Parallel()

	for _, s := range lifecycle.States() {
		if s == lifecycle.Pending {
			continue
		}
		if !lifecycle.Reachable(lifecycle.Pending, s) {
			t.Errorf("%s is unreachable from pending", s)
		}
	}

	for _, s := range lifecycle.States() {
		if lifecycle.Resting(s) {
			continue
		}
		reachesRest := false
		for _, rest := range lifecycle.States() {
			if lifecycle.Resting(rest) && lifecycle.Reachable(s, rest) {
				reachesRest = true
				break
			}
		}
		if !reachesRest {
			t.Errorf("%s cannot reach any resting state, so a deployment there could never be deleted", s)
		}
	}
}

func TestPredicatesPartitionTheStates(t *testing.T) {
	t.Parallel()

	// serving, resting and in-flight must be mutually exclusive and cover everything.
	// Overlap would make "is it serving" and "can it be deleted" both true at once,
	// which is the combination that deletes a deployment out from under live traffic.
	for _, s := range lifecycle.States() {
		n := 0
		if lifecycle.Serving(s) {
			n++
		}
		if lifecycle.Resting(s) {
			n++
		}
		if lifecycle.InFlight(s) {
			n++
		}
		if n != 1 {
			t.Errorf("%s matches %d of serving/resting/in-flight, want exactly 1", s, n)
		}
	}

	if !lifecycle.Serving(lifecycle.Degraded) {
		// A degraded deployment still has ready replicas. Removing it from rotation
		// turns a partial failure into a total one.
		t.Error("a degraded deployment must still be serving")
	}
	if !lifecycle.Serving(lifecycle.Ready) {
		t.Error("a ready deployment must be serving")
	}

	// Deletable must be exactly the resting set, because that is what the
	// ck_deployments__delete_only_when_resting constraint enforces.
	for _, s := range lifecycle.States() {
		if lifecycle.Deletable(s) != lifecycle.Resting(s) {
			t.Errorf("Deletable(%s) = %v but Resting(%s) = %v; the CHECK constraint uses the resting set",
				s, lifecycle.Deletable(s), s, lifecycle.Resting(s))
		}
	}
}

func TestTransitionSeparatesUnchangedFromIllegal(t *testing.T) {
	t.Parallel()

	// Same state: not an error. A controller re-reporting "still ready" must not
	// restart the state timer or fill history with noise, so the caller needs to be
	// able to tell "nothing to do" from "you cannot do that".
	outcome, err := lifecycle.Transition(lifecycle.Ready, lifecycle.Ready)
	if err != nil {
		t.Fatalf("Transition(ready, ready) returned an error: %v", err)
	}
	if outcome != lifecycle.OutcomeUnchanged {
		t.Errorf("Transition(ready, ready) = %v, want OutcomeUnchanged", outcome)
	}

	outcome, err = lifecycle.Transition(lifecycle.Pending, lifecycle.Provisioning)
	if err != nil {
		t.Fatalf("Transition(pending, provisioning): %v", err)
	}
	if outcome != lifecycle.OutcomeChanged {
		t.Errorf("Transition(pending, provisioning) = %v, want OutcomeChanged", outcome)
	}

	// An illegal edge must name what was allowed instead, so the resulting 422 is
	// actionable rather than just a refusal.
	_, err = lifecycle.Transition(lifecycle.Pending, lifecycle.Ready)
	if err == nil {
		t.Fatal("Transition(pending, ready) succeeded; pods cannot become ready before provisioning")
	}
	var tErr *lifecycle.TransitionError
	if !errors.As(err, &tErr) {
		t.Fatalf("error is %T, want *lifecycle.TransitionError", err)
	}
	if tErr.From != lifecycle.Pending || tErr.To != lifecycle.Ready {
		t.Errorf("TransitionError carries %s -> %s", tErr.From, tErr.To)
	}
	if len(tErr.Allowed) == 0 {
		t.Error("TransitionError lists no allowed states")
	}
	for _, want := range []string{"pending", "ready"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error message %q does not mention %q", err.Error(), want)
		}
	}

	// An unknown state is refused rather than treated as having no outgoing edges,
	// which would silently make it terminal.
	if _, err := lifecycle.Transition(models.DeploymentState("progressing"), lifecycle.Ready); err == nil {
		t.Error("Transition accepted an unknown source state")
	}
	if _, err := lifecycle.Transition(lifecycle.Ready, models.DeploymentState("gone")); err == nil {
		t.Error("Transition accepted an unknown target state")
	}
}

func TestSpecificTransitionRules(t *testing.T) {
	t.Parallel()

	cases := []struct {
		from, to lifecycle.State
		allowed  bool
		why      string
	}{
		{lifecycle.Pending, lifecycle.Provisioning, true, "the controller claims the deployment"},
		{lifecycle.Provisioning, lifecycle.Starting, true, "objects applied, pods starting"},
		{lifecycle.Starting, lifecycle.Ready, true, "all replicas ready"},
		{lifecycle.Starting, lifecycle.Degraded, true, "some replicas ready"},
		{lifecycle.Ready, lifecycle.Degraded, true, "a replica was lost"},
		{lifecycle.Degraded, lifecycle.Ready, true, "replicas recovered"},
		{lifecycle.Stopped, lifecycle.Pending, true, "a stopped deployment can be started again"},
		{lifecycle.Failed, lifecycle.Provisioning, true, "a new revision is applied after a failure"},
		{lifecycle.Stopping, lifecycle.Stopped, true, "the replicas are gone"},

		{lifecycle.Pending, lifecycle.Ready, false, "nothing has been provisioned"},
		{lifecycle.Pending, lifecycle.Starting, false, "objects have not been applied"},
		{lifecycle.Provisioning, lifecycle.Ready, false, "pods have not started"},
		{lifecycle.Ready, lifecycle.Starting, false, "a spec change goes back to provisioning, not starting"},
		{lifecycle.Stopped, lifecycle.Ready, false, "a stopped deployment must be provisioned again first"},
		{lifecycle.Stopped, lifecycle.Stopping, false, "already stopped"},
		{lifecycle.Failed, lifecycle.Ready, false, "failure is not cleared by assertion"},
	}

	for _, c := range cases {
		if got := lifecycle.CanTransition(c.from, c.to); got != c.allowed {
			t.Errorf("CanTransition(%s, %s) = %v, want %v (%s)", c.from, c.to, got, c.allowed, c.why)
		}
	}

	// Every state that work can fail in must be able to reach failed, so a failure
	// is always recordable rather than leaving a deployment stuck mid-flight.
	for _, s := range []lifecycle.State{
		lifecycle.Pending, lifecycle.Provisioning, lifecycle.Starting,
		lifecycle.Ready, lifecycle.Degraded, lifecycle.Stopping,
	} {
		if !lifecycle.CanTransition(s, lifecycle.Failed) {
			t.Errorf("%s cannot move to failed", s)
		}
	}

	// A stop must be possible from anywhere except the already-stopped states, or an
	// operator has no way to wind down a deployment that is stuck.
	for _, s := range lifecycle.States() {
		if s == lifecycle.Stopping || s == lifecycle.Stopped {
			continue
		}
		if !lifecycle.CanTransition(s, lifecycle.Stopping) {
			t.Errorf("a deployment in %s cannot be stopped", s)
		}
	}
}

func TestNextStatesIsStable(t *testing.T) {
	t.Parallel()

	// The list is served by the API and rendered in a UI, so a map-iteration order
	// would make it shuffle between requests.
	for _, s := range lifecycle.States() {
		first := lifecycle.NextStates(s)
		for i := 0; i < 5; i++ {
			again := lifecycle.NextStates(s)
			if fmt.Sprint(first) != fmt.Sprint(again) {
				t.Fatalf("NextStates(%s) is unstable: %v then %v", s, first, again)
			}
		}
		asStrings := make([]string, len(first))
		for i, n := range first {
			asStrings[i] = string(n)
		}
		if !sort.StringsAreSorted(asStrings) {
			t.Errorf("NextStates(%s) = %v is not sorted", s, first)
		}
	}
}

func TestReasons(t *testing.T) {
	t.Parallel()

	reasons := lifecycle.Reasons()
	if len(reasons) == 0 {
		t.Fatal("no transition reasons are defined")
	}

	seen := map[lifecycle.Reason]bool{}
	for _, r := range reasons {
		if seen[r] {
			t.Errorf("reason %q is listed twice", r)
		}
		seen[r] = true
		if !lifecycle.ValidReason(r) {
			t.Errorf("ValidReason(%q) = false for a listed reason", r)
		}
		if r == "" {
			t.Error("the empty string is listed as a reason")
		}
	}

	if lifecycle.ValidReason("whatever") {
		t.Error("ValidReason accepted an unlisted reason")
	}

	// Terminal means "will not improve on its own", which is what a rollout reads to
	// decide whether to abort immediately or wait out its analysis window. A bad
	// checksum or an exhausted quota will not fix itself; a lost replica may well be
	// replaced by the next reconcile, so treating it as terminal would abort healthy
	// rollouts on a single pod eviction.
	for _, r := range []lifecycle.Reason{
		lifecycle.ReasonArtifactChecksumError,
		lifecycle.ReasonModelLoadFailed,
		lifecycle.ReasonInsufficientCapacity,
		lifecycle.ReasonQuotaExceeded,
	} {
		if !r.Terminal() {
			t.Errorf("%s must be terminal: it does not resolve without a change", r)
		}
	}
	for _, r := range []lifecycle.Reason{
		lifecycle.ReasonReplicasReady,
		lifecycle.ReasonReplicaLost,
		lifecycle.ReasonAllReplicasLost,
		lifecycle.ReasonReplicasRecovered,
		lifecycle.ReasonStopRequested,
	} {
		if r.Terminal() {
			t.Errorf("%s must not be terminal: a reconcile can still improve it", r)
		}
	}
}
