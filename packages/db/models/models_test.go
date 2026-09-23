package models_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/adityasatwar321/nebula/packages/db/models"
)

// The Go enums mirror PostgreSQL enums and CHECK constraints. When the two
// disagree the database wins and inserts fail at runtime, so this test compares
// them against the migration SQL directly.
func TestEnumsMatchMigrations(t *testing.T) {
	t.Parallel()

	sql := readMigrations(t)

	t.Run("postgres enum types", func(t *testing.T) {
		t.Parallel()
		cases := []struct {
			typeName string
			got      []string
		}{
			{"user_role", toStrings(models.UserRoles())},
			{"model_version_status", []string{
				string(models.VersionUploading), string(models.VersionVerifying),
				string(models.VersionReady), string(models.VersionFailed), string(models.VersionArchived),
			}},
			{"deployment_state", toStrings(models.DeploymentStates())},
			{"rollout_state", []string{
				string(models.RolloutPending), string(models.RolloutProgressing),
				string(models.RolloutPaused), string(models.RolloutPromoted),
				string(models.RolloutAborted), string(models.RolloutRolledBack),
				string(models.RolloutFailed),
			}},
			{"experiment_state", []string{
				string(models.ExperimentDraft), string(models.ExperimentRunning),
				string(models.ExperimentStopped), string(models.ExperimentConcluded),
			}},
		}

		for _, c := range cases {
			want := enumValuesFromSQL(t, sql, c.typeName)
			if len(want) == 0 {
				t.Errorf("could not find CREATE TYPE %s in the migrations", c.typeName)
				continue
			}
			assertSameSet(t, c.typeName, c.got, want)
		}
	})

	t.Run("check constraint vocabularies", func(t *testing.T) {
		t.Parallel()
		cases := []struct {
			constraint string
			got        []string
		}{
			{"ck_worker_events__type", toStrings(models.WorkerEventTypes())},
			{"ck_autoscaling_events__decision", toStrings(models.AutoscaleDecisions())},
			{"ck_requests__outcome", toStrings(models.RequestOutcomes())},
		}
		for _, c := range cases {
			want := checkValuesFromSQL(t, sql, c.constraint)
			if len(want) == 0 {
				t.Errorf("could not find constraint %s in the migrations", c.constraint)
				continue
			}
			assertSameSet(t, c.constraint, c.got, want)
		}
	})
}

func readMigrations(t *testing.T) string {
	t.Helper()
	dir := filepath.Join("..", "..", "..", "migrations")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading migrations directory: %v", err)
	}
	var b strings.Builder
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".up.sql") {
			continue
		}
		body, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatalf("reading %s: %v", e.Name(), err)
		}
		b.Write(body)
		b.WriteString("\n")
	}
	return b.String()
}

var quoted = regexp.MustCompile(`'([a-z0-9_.]+)'`)

// enumValuesFromSQL returns the values of the LAST definition of an enum type in
// migration order.
//
// The last one, not the first: a migration may replace an enum (000009 does, for
// the deployment lifecycle), and the current schema is what the final definition
// says. Reading the first would compare the Go enums against a type that no longer
// exists.
func enumValuesFromSQL(t *testing.T, sql, typeName string) []string {
	t.Helper()
	re := regexp.MustCompile(`(?is)CREATE TYPE\s+` + regexp.QuoteMeta(typeName) + `\s+AS ENUM\s*\(([^)]*)\)`)
	all := re.FindAllStringSubmatch(sql, -1)
	if len(all) == 0 {
		return nil
	}
	return extractQuoted(all[len(all)-1][1])
}

func checkValuesFromSQL(t *testing.T, sql, constraint string) []string {
	t.Helper()
	re := regexp.MustCompile(`(?is)CONSTRAINT\s+` + regexp.QuoteMeta(constraint) + `\s+CHECK\s*\((.*?)\n\s*\)`)
	m := re.FindStringSubmatch(sql)
	if m == nil {
		// Single-line form.
		re = regexp.MustCompile(`(?is)CONSTRAINT\s+` + regexp.QuoteMeta(constraint) + `\s+CHECK\s*\(([^;]*?)\)\s*[,)]`)
		m = re.FindStringSubmatch(sql)
		if m == nil {
			return nil
		}
	}
	return extractQuoted(m[1])
}

func extractQuoted(s string) []string {
	var out []string
	for _, m := range quoted.FindAllStringSubmatch(s, -1) {
		out = append(out, m[1])
	}
	return out
}

func toStrings[T ~string](in []T) []string {
	out := make([]string, len(in))
	for i, v := range in {
		out[i] = string(v)
	}
	return out
}

func assertSameSet(t *testing.T, what string, got, want []string) {
	t.Helper()
	g, w := map[string]bool{}, map[string]bool{}
	for _, v := range got {
		g[v] = true
	}
	for _, v := range want {
		w[v] = true
	}
	for v := range w {
		if !g[v] {
			t.Errorf("%s: SQL allows %q but the Go enum does not", what, v)
		}
	}
	for v := range g {
		if !w[v] {
			t.Errorf("%s: the Go enum allows %q but SQL does not; an insert would fail", what, v)
		}
	}
}

func TestModelVersionStateMachine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		from, to models.ModelVersionStatus
		allowed  bool
	}{
		{models.VersionUploading, models.VersionVerifying, true},
		{models.VersionUploading, models.VersionFailed, true},
		{models.VersionUploading, models.VersionReady, false}, // must be verified first
		{models.VersionVerifying, models.VersionReady, true},
		{models.VersionVerifying, models.VersionFailed, true},
		{models.VersionReady, models.VersionArchived, true},
		{models.VersionReady, models.VersionFailed, false}, // ready is immutable
		{models.VersionReady, models.VersionUploading, false},
		{models.VersionArchived, models.VersionReady, false},
		{models.VersionFailed, models.VersionReady, false},
	}

	for _, tt := range tests {
		if got := tt.from.CanTransitionTo(tt.to); got != tt.allowed {
			t.Errorf("%s -> %s allowed = %v, want %v", tt.from, tt.to, got, tt.allowed)
		}
	}

	if !models.VersionReady.Immutable() || !models.VersionArchived.Immutable() {
		t.Error("ready and archived versions must be immutable")
	}
	if models.VersionUploading.Immutable() {
		t.Error("an uploading version must not be immutable")
	}
}

func TestDeploymentPredicates(t *testing.T) {
	t.Parallel()

	d := models.Deployment{Generation: 7, ObservedGeneration: 5, DesiredReplicas: 3, ReadyReplicas: 3}
	if !d.NeedsReconcile() {
		t.Error("NeedsReconcile() = false with generation 7 != observed 5")
	}
	if d.Converged() {
		t.Error("Converged() = true while generation differs from observed")
	}

	d.ObservedGeneration = 7
	if d.NeedsReconcile() {
		t.Error("NeedsReconcile() = true when generations match")
	}
	if !d.Converged() {
		t.Error("Converged() = false when generations match and replicas are ready")
	}

	d.ReadyReplicas = 2
	if d.Converged() {
		t.Error("Converged() = true with 2 of 3 replicas ready")
	}

	// Degraded must still serve: removing a whole deployment from rotation
	// because one pod died turns a partial failure into a total one.
	if !models.DeploymentDegraded.Serving() {
		t.Error("a degraded deployment must still serve traffic")
	}
	if models.DeploymentPending.Serving() || models.DeploymentFailed.Serving() {
		t.Error("pending and failed deployments must not serve")
	}
}

func TestRolloutActiveSetMatchesPartialIndex(t *testing.T) {
	t.Parallel()

	// The database enforces one active rollout per route with a partial unique
	// index over exactly this set; the two must agree.
	active := map[models.RolloutState]bool{
		models.RolloutPending: true, models.RolloutProgressing: true, models.RolloutPaused: true,
		models.RolloutPromoted: false, models.RolloutAborted: false,
		models.RolloutRolledBack: false, models.RolloutFailed: false,
	}
	for state, want := range active {
		if got := state.Active(); got != want {
			t.Errorf("%s.Active() = %v, want %v", state, got, want)
		}
	}
}

// A client hanging up has not experienced an outage; counting it as one trains
// operators to ignore the alert (docs/observability.md §7.1).
func TestClientCancellationIsNotAnAvailabilityError(t *testing.T) {
	t.Parallel()

	if models.OutcomeClientCancelled.CountsAgainstAvailability() {
		t.Error("client cancellation must not count against availability")
	}
	if models.OutcomeCompleted.CountsAgainstAvailability() {
		t.Error("a completed request must not count as an error")
	}
	for _, o := range []models.RequestOutcome{
		models.OutcomeStreamInterrupted, models.OutcomeDeadlineExceeded,
		models.OutcomeQueueTimeout, models.OutcomeRejected,
	} {
		if !o.CountsAgainstAvailability() {
			t.Errorf("%s must count against availability", o)
		}
	}
}

func TestPriorityRanking(t *testing.T) {
	t.Parallel()

	if models.PriorityHigh.Rank() <= models.PriorityNormal.Rank() {
		t.Error("HIGH must outrank NORMAL")
	}
	if models.PriorityNormal.Rank() <= models.PriorityLow.Rank() {
		t.Error("NORMAL must outrank LOW")
	}
	for _, p := range []models.Priority{models.PriorityLow, models.PriorityNormal, models.PriorityHigh} {
		if !p.Valid() {
			t.Errorf("%q reported invalid", p)
		}
	}
	if models.Priority("URGENT").Valid() {
		t.Error("an unknown priority was reported valid")
	}
}

func TestAPIKeyUsable(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	past := now.Add(-time.Hour)
	future := now.Add(time.Hour)

	tests := []struct {
		name string
		key  models.APIKey
		want bool
	}{
		{"fresh", models.APIKey{}, true},
		{"revoked", models.APIKey{RevokedAt: &past}, false},
		{"expired", models.APIKey{ExpiresAt: &past}, false},
		{"expires later", models.APIKey{ExpiresAt: &future}, true},
		{"expires exactly now", models.APIKey{ExpiresAt: &now}, false},
		{"revoked in the future still counts as revoked", models.APIKey{RevokedAt: &future}, false},
	}
	for _, tt := range tests {
		if got := tt.key.Usable(now); got != tt.want {
			t.Errorf("%s: Usable() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

// A User must never serialise its password hash, and an APIKey must never
// serialise its key hash.
func TestSecretFieldsAreNotSerialised(t *testing.T) {
	t.Parallel()

	hash := "argon2id$verysecret"
	u := models.User{Email: "a@b.c", PasswordHash: &hash}
	b, err := json.Marshal(u)
	if err != nil {
		t.Fatalf("marshalling User: %v", err)
	}
	if strings.Contains(string(b), "verysecret") || strings.Contains(string(b), "password") {
		t.Errorf("User JSON leaked the password hash: %s", b)
	}

	k := models.APIKey{Prefix: "nbk_abc1234", KeyHash: []byte("raw-hmac-bytes")}
	b, err = json.Marshal(k)
	if err != nil {
		t.Fatalf("marshalling APIKey: %v", err)
	}
	if strings.Contains(string(b), "raw-hmac-bytes") || strings.Contains(string(b), "key_hash") {
		t.Errorf("APIKey JSON leaked the key hash: %s", b)
	}
	if !strings.Contains(string(b), "nbk_abc1234") {
		t.Errorf("APIKey JSON should include the displayable prefix: %s", b)
	}
}

func TestPricingProfileZeroedIsDetectable(t *testing.T) {
	t.Parallel()

	// The seeded default prices everything at zero so no cost is ever fabricated.
	// A response built from it must be able to say so.
	if !(models.PricingProfile{}).Zeroed() {
		t.Error("an all-zero pricing profile must report Zeroed() = true")
	}
	if (models.PricingProfile{CPUCoreHourMicros: 1}).Zeroed() {
		t.Error("a profile with a non-zero price must report Zeroed() = false")
	}
}

func TestNodeStaleness(t *testing.T) {
	t.Parallel()

	now := time.Now()
	fresh := models.Node{SyncedAt: now.Add(-time.Second)}
	old := models.Node{SyncedAt: now.Add(-time.Hour)}

	if fresh.Stale(now, time.Minute) {
		t.Error("a node synced a second ago is not stale at a one-minute threshold")
	}
	if !old.Stale(now, time.Minute) {
		t.Error("a node synced an hour ago is stale at a one-minute threshold")
	}
}

func TestMockRuntimeIsMarkedDevelopmentOnly(t *testing.T) {
	t.Parallel()

	if !models.RuntimeMock.DevelopmentOnly() {
		t.Error("the mock runtime must be marked development-only (axiom A9)")
	}
	if models.RuntimeLlamaCPP.DevelopmentOnly() || models.RuntimeVLLM.DevelopmentOnly() {
		t.Error("a real runtime must not be marked development-only")
	}
}
