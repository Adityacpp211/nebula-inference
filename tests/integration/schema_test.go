package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adityasatwar321/nebula/packages/db"
)

// fixture identifiers and helpers -------------------------------------------

type org struct {
	ID   uuid.UUID
	Slug string
}

func newOrg(ctx context.Context, t *testing.T, q db.Querier, slug string) org {
	t.Helper()
	o := org{ID: db.MustNewID(), Slug: slug}
	_, err := q.Exec(ctx,
		`INSERT INTO organizations (id, slug, name) VALUES ($1, $2, $3)`,
		o.ID, o.Slug, strings.ToUpper(slug))
	if err != nil {
		t.Fatalf("creating organization %s: %v", slug, err)
	}
	return o
}

func newModel(ctx context.Context, t *testing.T, q db.Querier, o org, name string) uuid.UUID {
	t.Helper()
	id := db.MustNewID()
	_, err := q.Exec(ctx,
		`INSERT INTO models (id, org_id, name) VALUES ($1, $2, $3)`, id, o.ID, name)
	if err != nil {
		t.Fatalf("creating model %s: %v", name, err)
	}
	return id
}

func sum32(s string) []byte {
	h := sha256.Sum256([]byte(s))
	return h[:]
}

func newReadyVersion(ctx context.Context, t *testing.T, q db.Querier, modelID uuid.UUID, version string) uuid.UUID {
	t.Helper()
	id := db.MustNewID()
	_, err := q.Exec(ctx, `
		INSERT INTO model_versions (
			id, model_id, version, format, runtime, size_bytes, checksum_sha256,
			context_window, artifact_uri, hardware_profile, status, ready_at)
		VALUES ($1, $2, $3, 'gguf', 'llamacpp', 1024, $4, 8192, $5, '{"requires_gpu":false}'::jsonb,
		        'ready', now())`,
		id, modelID, version, sum32(version), "s3://nebula-models/sha256/"+version)
	if err != nil {
		t.Fatalf("creating model version %s: %v", version, err)
	}
	return id
}

func newDeployment(ctx context.Context, t *testing.T, q db.Querier, o org, versionID uuid.UUID, name string) uuid.UUID {
	t.Helper()
	id := db.MustNewID()
	_, err := q.Exec(ctx, `
		INSERT INTO deployments (id, org_id, model_version_id, name, namespace, resources)
		VALUES ($1, $2, $3, $4, 'nebula-workloads', '{"cpu_milli":2000,"memory_mib":4096}'::jsonb)`,
		id, o.ID, versionID, name)
	if err != nil {
		t.Fatalf("creating deployment %s: %v", name, err)
	}
	return id
}

// --- UUID round trip --------------------------------------------------------

// google/uuid values must survive a round trip through pgx without a custom
// codec, and UUIDv7 must be time-ordered in the database as well as in Go.
func TestUUIDRoundTripAndOrdering(t *testing.T) {
	pool := migratedDatabase(t)
	ctx := ctxT(t)

	var ids []uuid.UUID
	for i := range 5 {
		o := newOrg(ctx, t, pool, "uuid-org-"+string(rune('a'+i)))
		ids = append(ids, o.ID)
	}

	var got uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT id FROM organizations WHERE id = $1`, ids[0]).Scan(&got); err != nil {
		t.Fatalf("round-tripping a uuid: %v", err)
	}
	if got != ids[0] {
		t.Fatalf("round-tripped uuid = %s, want %s", got, ids[0])
	}

	rows, err := pool.Query(ctx, `SELECT id FROM organizations ORDER BY id`)
	if err != nil {
		t.Fatalf("querying ordered ids: %v", err)
	}
	defer rows.Close()
	var ordered []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scanning: %v", err)
		}
		ordered = append(ordered, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterating: %v", err)
	}
	for i, id := range ordered {
		if id != ids[i] {
			t.Fatalf("database ordering does not match creation order at %d: UUIDv7 must be time-ordered", i)
		}
	}
}

// --- row-level security -----------------------------------------------------

// The test that matters: a query that forgets its org filter must return nothing
// rather than another tenant's rows (ADR-0022).
func TestRowLevelSecurityIsolatesTenants(t *testing.T) {
	pool := migratedDatabase(t)
	ctx := ctxT(t)

	acme := newOrg(ctx, t, pool, "acme")
	globex := newOrg(ctx, t, pool, "globex")
	newModel(ctx, t, pool, acme, "acme-model")
	newModel(ctx, t, pool, globex, "globex-model")

	// Run as nebula_app, which is not the table owner, so policies apply.
	asApp := func(t *testing.T, orgID uuid.UUID, fn func(tx pgx.Tx)) {
		t.Helper()
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()

		if _, err := tx.Exec(ctx, `SET LOCAL ROLE nebula_app`); err != nil {
			t.Fatalf("SET ROLE nebula_app: %v", err)
		}
		if orgID != uuid.Nil {
			if err := db.SetSessionOrg(ctx, tx, orgID); err != nil {
				t.Fatalf("setting session org: %v", err)
			}
		}
		fn(tx)
	}

	countModels := func(t *testing.T, tx pgx.Tx) int {
		t.Helper()
		var n int
		// Deliberately unfiltered: this is the forgotten-WHERE-clause case.
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM models`).Scan(&n); err != nil {
			t.Fatalf("counting models: %v", err)
		}
		return n
	}

	t.Run("each tenant sees only its own rows", func(t *testing.T) {
		asApp(t, acme.ID, func(tx pgx.Tx) {
			if n := countModels(t, tx); n != 1 {
				t.Errorf("acme sees %d models through an unfiltered query, want 1", n)
			}
			var name string
			if err := tx.QueryRow(ctx, `SELECT name FROM models`).Scan(&name); err != nil {
				t.Fatalf("reading the visible model: %v", err)
			}
			if name != "acme-model" {
				t.Errorf("acme sees %q, want acme-model", name)
			}
		})
		asApp(t, globex.ID, func(tx pgx.Tx) {
			if n := countModels(t, tx); n != 1 {
				t.Errorf("globex sees %d models, want 1", n)
			}
		})
	})

	// With no tenant set, current_setting returns NULL and every predicate is
	// NULL, so nothing is visible. RLS must fail CLOSED.
	t.Run("no tenant set means no rows", func(t *testing.T) {
		asApp(t, uuid.Nil, func(tx pgx.Tx) {
			if n := countModels(t, tx); n != 0 {
				t.Errorf("with no tenant set, %d models are visible; RLS must fail closed", n)
			}
		})
	})

	// Child tables are reached through their parent's org_id.
	t.Run("child tables are isolated too", func(t *testing.T) {
		acmeModel := newModel(ctx, t, pool, acme, "acme-child-model")
		newReadyVersion(ctx, t, pool, acmeModel, "1b-q4")

		asApp(t, globex.ID, func(tx pgx.Tx) {
			var n int
			if err := tx.QueryRow(ctx, `SELECT count(*) FROM model_versions`).Scan(&n); err != nil {
				t.Fatalf("counting model versions: %v", err)
			}
			if n != 0 {
				t.Errorf("globex sees %d of acme's model versions, want 0", n)
			}
		})
		asApp(t, acme.ID, func(tx pgx.Tx) {
			var n int
			if err := tx.QueryRow(ctx, `SELECT count(*) FROM model_versions`).Scan(&n); err != nil {
				t.Fatalf("counting model versions: %v", err)
			}
			if n != 1 {
				t.Errorf("acme sees %d of its own model versions, want 1", n)
			}
		})
	})

	// A tenant must not be able to write a row belonging to another tenant.
	t.Run("cannot insert into another tenant", func(t *testing.T) {
		asApp(t, acme.ID, func(tx pgx.Tx) {
			_, err := tx.Exec(ctx,
				`INSERT INTO models (id, org_id, name) VALUES ($1, $2, $3)`,
				db.MustNewID(), globex.ID, "smuggled")
			if err == nil {
				t.Error("acme inserted a model into globex; the WITH CHECK clause is not working")
			}
		})
	})

	// Built-in rows (org_id IS NULL) are shared and readable by everyone.
	t.Run("built-in policies are visible to all tenants", func(t *testing.T) {
		for _, o := range []org{acme, globex} {
			asApp(t, o.ID, func(tx pgx.Tx) {
				var n int
				if err := tx.QueryRow(ctx,
					`SELECT count(*) FROM routing_policies WHERE org_id IS NULL`).Scan(&n); err != nil {
					t.Fatalf("counting built-in routing policies: %v", err)
				}
				if n == 0 {
					t.Errorf("%s cannot see the built-in routing policies", o.Slug)
				}
			})
		}
	})
}

func TestInTxForOrgSetsTheSessionVariable(t *testing.T) {
	pool := migratedDatabase(t)
	ctx := ctxT(t)

	o := newOrg(ctx, t, pool, "session-org")

	err := db.InTxForOrg(ctx, pool, o.ID, func(tx pgx.Tx) error {
		got, err := db.CurrentSessionOrg(ctx, tx)
		if err != nil {
			return err
		}
		if got != o.ID.String() {
			t.Errorf("session org = %q, want %q", got, o.ID)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("InTxForOrg: %v", err)
	}

	// The setting is transaction-local, so it must not leak to the next one.
	err = db.InTx(ctx, pool, func(tx pgx.Tx) error {
		got, err := db.CurrentSessionOrg(ctx, tx)
		if err != nil {
			return err
		}
		if got != "" {
			t.Errorf("session org leaked into a later transaction: %q", got)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("InTx: %v", err)
	}

	if err := db.InTxForOrg(ctx, pool, uuid.Nil, func(pgx.Tx) error { return nil }); err == nil {
		t.Error("InTxForOrg accepted a nil organization id")
	}
}

func TestInTxRollsBackOnError(t *testing.T) {
	pool := migratedDatabase(t)
	ctx := ctxT(t)

	sentinel := "deliberate failure"
	err := db.InTx(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`INSERT INTO organizations (id, slug, name) VALUES ($1, 'rollback-org', 'X')`,
			db.MustNewID()); err != nil {
			return err
		}
		return &testError{sentinel}
	})
	if err == nil || !strings.Contains(err.Error(), sentinel) {
		t.Fatalf("InTx returned %v, want the callback's error", err)
	}

	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM organizations WHERE slug = 'rollback-org'`).Scan(&n); err != nil {
		t.Fatalf("counting: %v", err)
	}
	if n != 0 {
		t.Error("the failed transaction was not rolled back")
	}
}

type testError struct{ msg string }

func (e *testError) Error() string { return e.msg }

// --- immutability -----------------------------------------------------------

// A ready model version is frozen. Enforced by a trigger, not only in
// application code (ADR-0010).
func TestModelVersionImmutability(t *testing.T) {
	pool := migratedDatabase(t)
	ctx := ctxT(t)

	o := newOrg(ctx, t, pool, "immutable-org")
	modelID := newModel(ctx, t, pool, o, "qwen2.5")
	versionID := newReadyVersion(ctx, t, pool, modelID, "0.5b-q4")

	frozen := []struct {
		field string
		stmt  string
		args  []any
	}{
		{"checksum_sha256", `UPDATE model_versions SET checksum_sha256 = $2 WHERE id = $1`, []any{versionID, sum32("different")}},
		{"artifact_uri", `UPDATE model_versions SET artifact_uri = $2 WHERE id = $1`, []any{versionID, "s3://elsewhere"}},
		{"format", `UPDATE model_versions SET format = 'safetensors' WHERE id = $1`, []any{versionID}},
		{"runtime", `UPDATE model_versions SET runtime = 'vllm' WHERE id = $1`, []any{versionID}},
		{"context_window", `UPDATE model_versions SET context_window = 4096 WHERE id = $1`, []any{versionID}},
		{"size_bytes", `UPDATE model_versions SET size_bytes = 2048 WHERE id = $1`, []any{versionID}},
		{"hardware_profile", `UPDATE model_versions SET hardware_profile = '{"requires_gpu":true}'::jsonb WHERE id = $1`, []any{versionID}},
		{"version", `UPDATE model_versions SET version = 'renamed' WHERE id = $1`, []any{versionID}},
	}
	for _, f := range frozen {
		t.Run("cannot change "+f.field, func(t *testing.T) {
			_, err := pool.Exec(ctx, f.stmt, f.args...)
			if err == nil {
				t.Fatalf("changed %s on a ready model version; it must be immutable", f.field)
			}
			if !strings.Contains(err.Error(), "immutable") {
				t.Errorf("error does not explain the rule: %v", err)
			}
		})
	}

	t.Run("status may only move to archived", func(t *testing.T) {
		if _, err := pool.Exec(ctx,
			`UPDATE model_versions SET status = 'failed' WHERE id = $1`, versionID); err == nil {
			t.Error("moved a ready version to failed; only archived is permitted")
		}
		if _, err := pool.Exec(ctx,
			`UPDATE model_versions SET status = 'archived' WHERE id = $1`, versionID); err != nil {
			t.Errorf("could not archive a ready version: %v", err)
		}
	})

	t.Run("a version that is not yet ready is mutable", func(t *testing.T) {
		id := db.MustNewID()
		_, err := pool.Exec(ctx, `
			INSERT INTO model_versions (id, model_id, version, format, runtime, size_bytes,
				checksum_sha256, context_window, artifact_uri, hardware_profile, status)
			VALUES ($1, $2, 'draft', 'gguf', 'llamacpp', 1, $3, 100, 'uri', '{}'::jsonb, 'uploading')`,
			id, modelID, sum32("draft"))
		if err != nil {
			t.Fatalf("creating an uploading version: %v", err)
		}
		if _, err := pool.Exec(ctx,
			`UPDATE model_versions SET size_bytes = 99 WHERE id = $1`, id); err != nil {
			t.Errorf("could not modify an uploading version: %v", err)
		}
	})
}

func TestAppendOnlyTables(t *testing.T) {
	pool := migratedDatabase(t)
	ctx := ctxT(t)

	o := newOrg(ctx, t, pool, "append-only-org")

	t.Run("audit_logs cannot be updated", func(t *testing.T) {
		id := db.MustNewID()
		_, err := pool.Exec(ctx, `
			INSERT INTO audit_logs (id, org_id, created_at, actor_type, action, resource_type)
			VALUES ($1, $2, now(), 'system', 'deployment.created', 'deployment')`, id, o.ID)
		if err != nil {
			t.Fatalf("inserting an audit row: %v", err)
		}
		_, err = pool.Exec(ctx, `UPDATE audit_logs SET action = 'tampered' WHERE id = $1`, id)
		if err == nil {
			t.Fatal("an audit log row was updated; audit history must be append-only")
		}
		if !strings.Contains(err.Error(), "append-only") {
			t.Errorf("error does not explain the rule: %v", err)
		}
	})

	t.Run("pricing profiles cannot be updated", func(t *testing.T) {
		_, err := pool.Exec(ctx,
			`UPDATE pricing_profiles SET cpu_core_hour_micros = 999 WHERE org_id IS NULL`)
		if err == nil {
			t.Fatal("a pricing profile was updated; prices must be versioned, not edited")
		}
	})

	t.Run("deployment revisions cannot be updated", func(t *testing.T) {
		versionID := newReadyVersion(ctx, t, pool, newModel(ctx, t, pool, o, "rev-model"), "1b")
		depID := newDeployment(ctx, t, pool, o, versionID, "rev-deploy")

		revID := db.MustNewID()
		spec := json.RawMessage(`{"replicas":1}`)
		_, err := pool.Exec(ctx, `
			INSERT INTO deployment_revisions (id, deployment_id, revision, model_version_id, spec, spec_hash, reason)
			VALUES ($1, $2, 1, $3, $4, $5, 'create')`,
			revID, depID, versionID, spec, sum32("spec"))
		if err != nil {
			t.Fatalf("inserting a revision: %v", err)
		}
		if _, err := pool.Exec(ctx,
			`UPDATE deployment_revisions SET reason = 'update' WHERE id = $1`, revID); err == nil {
			t.Error("a deployment revision was updated; history must never be rewritten")
		}
	})
}

// --- constraints ------------------------------------------------------------

// A route summing to anything but 100 would silently drop traffic, so it must be
// impossible rather than caught in review.
func TestRouteWeightsMustTotal100(t *testing.T) {
	pool := migratedDatabase(t)
	ctx := ctxT(t)

	o := newOrg(ctx, t, pool, "weights-org")
	modelID := newModel(ctx, t, pool, o, "weights-model")
	v1 := newReadyVersion(ctx, t, pool, modelID, "v1")
	v2 := newReadyVersion(ctx, t, pool, modelID, "v2")
	d1 := newDeployment(ctx, t, pool, o, v1, "baseline")
	d2 := newDeployment(ctx, t, pool, o, v2, "canary")

	routeID := db.MustNewID()
	if _, err := pool.Exec(ctx,
		`INSERT INTO routes (id, org_id, model_name) VALUES ($1, $2, 'qwen2.5-chat')`,
		routeID, o.ID); err != nil {
		t.Fatalf("creating a route: %v", err)
	}

	t.Run("a single target at 100 is accepted", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()

		if _, err := tx.Exec(ctx, `
			INSERT INTO route_targets (id, route_id, deployment_id, weight, is_baseline)
			VALUES ($1, $2, $3, 100, true)`, db.MustNewID(), routeID, d1); err != nil {
			t.Fatalf("inserting a 100%% target: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit of a valid weighting failed: %v", err)
		}
	})

	t.Run("weights not totalling 100 are rejected at commit", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()

		// The route already totals 100; adding a 10-weight target takes it to 110.
		if _, err := tx.Exec(ctx, `
			INSERT INTO route_targets (id, route_id, deployment_id, weight)
			VALUES ($1, $2, $3, 10)`, db.MustNewID(), routeID, d2); err != nil {
			t.Fatalf("insert inside the transaction failed early: %v", err)
		}
		err = tx.Commit(ctx)
		if err == nil {
			t.Fatal("committed a route whose weights total 110")
		}
		if !strings.Contains(err.Error(), "must total 100") {
			t.Errorf("error does not explain the rule: %v", err)
		}
	})

	// The constraint is DEFERRED, so a transaction may pass through an
	// inconsistent intermediate state while shifting weight between targets.
	t.Run("a valid split may pass through an invalid intermediate state", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()

		// Momentarily 90 total...
		if _, err := tx.Exec(ctx,
			`UPDATE route_targets SET weight = 90 WHERE route_id = $1 AND deployment_id = $2`,
			routeID, d1); err != nil {
			t.Fatalf("updating the baseline weight: %v", err)
		}
		// ...then the canary makes it 100 again.
		if _, err := tx.Exec(ctx, `
			INSERT INTO route_targets (id, route_id, deployment_id, weight, label)
			VALUES ($1, $2, $3, 10, 'canary')`, db.MustNewID(), routeID, d2); err != nil {
			t.Fatalf("inserting the canary target: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("a 90/10 split was rejected: %v", err)
		}
	})

	t.Run("deleting a route removes its targets without tripping the check", func(t *testing.T) {
		if _, err := pool.Exec(ctx, `DELETE FROM routes WHERE id = $1`, routeID); err != nil {
			t.Fatalf("deleting the route: %v", err)
		}
	})
}

func TestOneActiveRolloutPerRoute(t *testing.T) {
	pool := migratedDatabase(t)
	ctx := ctxT(t)

	o := newOrg(ctx, t, pool, "rollout-org")
	modelID := newModel(ctx, t, pool, o, "rollout-model")
	v1 := newReadyVersion(ctx, t, pool, modelID, "v1")
	v2 := newReadyVersion(ctx, t, pool, modelID, "v2")
	d1 := newDeployment(ctx, t, pool, o, v1, "baseline")
	d2 := newDeployment(ctx, t, pool, o, v2, "canary")

	routeID := db.MustNewID()
	if _, err := pool.Exec(ctx,
		`INSERT INTO routes (id, org_id, model_name) VALUES ($1, $2, 'rollout-route')`,
		routeID, o.ID); err != nil {
		t.Fatalf("creating a route: %v", err)
	}

	insert := func(state string) error {
		// $6 is cast explicitly: PostgreSQL cannot deduce one type for a parameter
		// used both as a rollout_state and inside a text comparison.
		_, err := pool.Exec(ctx, `
			INSERT INTO canary_rollouts (
				id, org_id, route_id, baseline_deployment_id, canary_deployment_id,
				steps, analysis, state, abort_reason, finished_at)
			VALUES ($1, $2, $3, $4, $5,
			        '[{"weight":10,"hold_seconds":300}]'::jsonb, '{"min_requests":200}'::jsonb,
			        $6::rollout_state,
			        CASE WHEN $6::text IN ('aborted','rolled_back','failed') THEN 'test' END,
			        CASE WHEN $6::text IN ('promoted','aborted','rolled_back','failed') THEN now() END)`,
			db.MustNewID(), o.ID, routeID, d1, d2, state)
		return err
	}

	if err := insert("progressing"); err != nil {
		t.Fatalf("creating the first rollout: %v", err)
	}
	if err := insert("paused"); err == nil {
		t.Fatal("a second active rollout was created for the same route")
	}
	// A finished rollout does not hold the lock.
	if err := insert("promoted"); err != nil {
		t.Errorf("a completed rollout was rejected: %v", err)
	}
}

func TestDeploymentReplicaBounds(t *testing.T) {
	pool := migratedDatabase(t)
	ctx := ctxT(t)

	o := newOrg(ctx, t, pool, "bounds-org")
	versionID := newReadyVersion(ctx, t, pool, newModel(ctx, t, pool, o, "bounds-model"), "v1")
	depID := newDeployment(ctx, t, pool, o, versionID, "bounded")

	tests := []struct {
		name string
		stmt string
	}{
		{"desired above max", `UPDATE deployments SET desired_replicas = 99 WHERE id = $1`},
		{"desired below min", `UPDATE deployments SET min_replicas = 2, desired_replicas = 1 WHERE id = $1`},
		{"max below min", `UPDATE deployments SET min_replicas = 5, max_replicas = 2 WHERE id = $1`},
		{"negative replicas", `UPDATE deployments SET min_replicas = -1 WHERE id = $1`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := pool.Exec(ctx, tt.stmt, depID); err == nil {
				t.Errorf("accepted an out-of-bounds replica count (%s)", tt.name)
			}
		})
	}
}

func TestOrganizationSlugMustBeADNSLabel(t *testing.T) {
	pool := migratedDatabase(t)
	ctx := ctxT(t)

	// The slug becomes part of Kubernetes object names, so rejecting it here is
	// cheaper than a reconcile failure later.
	for _, bad := range []string{"Acme", "acme_corp", "-acme", "acme-", "acme corp", ""} {
		if _, err := pool.Exec(ctx,
			`INSERT INTO organizations (id, slug, name) VALUES ($1, $2, 'X')`,
			db.MustNewID(), bad); err == nil {
			t.Errorf("accepted invalid slug %q", bad)
		}
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO organizations (id, slug, name) VALUES ($1, 'acme-corp-7', 'X')`,
		db.MustNewID()); err != nil {
		t.Errorf("rejected a valid slug: %v", err)
	}
}

func TestAPIKeyConstraints(t *testing.T) {
	pool := migratedDatabase(t)
	ctx := ctxT(t)

	o := newOrg(ctx, t, pool, "keys-org")

	insert := func(prefix string, hashLen int, priority string) error {
		_, err := pool.Exec(ctx, `
			INSERT INTO api_keys (id, org_id, name, prefix, key_hash, priority)
			VALUES ($1, $2, 'test', $3, $4, $5)`,
			db.MustNewID(), o.ID, prefix, make([]byte, hashLen), priority)
		return err
	}

	if err := insert("nbk_abc1234", 32, "NORMAL"); err != nil {
		t.Fatalf("rejected a valid key: %v", err)
	}
	if err := insert("bad_abc1234", 32, "NORMAL"); err == nil {
		t.Error("accepted a key whose prefix is not nbk_...")
	}
	if err := insert("nbk_zzz9999", 16, "NORMAL"); err == nil {
		t.Error("accepted a key hash that is not 32 bytes (HMAC-SHA256)")
	}
	if err := insert("nbk_yyy8888", 32, "URGENT"); err == nil {
		t.Error("accepted an unknown priority")
	}
	if err := insert("nbk_abc1234", 32, "NORMAL"); err == nil {
		t.Error("accepted a duplicate key prefix; lookup depends on it being unique")
	}
}

// Requests must land in the partition covering their timestamp.
func TestRequestPartitionRouting(t *testing.T) {
	pool := migratedDatabase(t)
	ctx := ctxT(t)

	o := newOrg(ctx, t, pool, "requests-org")
	now := time.Now().UTC()
	id := db.MustNewID()

	if _, err := pool.Exec(ctx, `
		INSERT INTO requests (id, org_id, created_at, status_code, duration_ms)
		VALUES ($1, $2, $3, 200, 120)`, id, o.ID, now); err != nil {
		t.Fatalf("inserting a request: %v", err)
	}

	var partition string
	if err := pool.QueryRow(ctx,
		`SELECT tableoid::regclass::text FROM requests WHERE id = $1`, id).Scan(&partition); err != nil {
		t.Fatalf("finding the partition: %v", err)
	}
	want := "requests_" + now.Format("200601")
	if partition != want {
		t.Errorf("row landed in %s, want %s", partition, want)
	}

	// Beyond the pre-created horizon the insert must fail loudly rather than
	// silently accumulating in a default partition.
	if _, err := pool.Exec(ctx, `
		INSERT INTO requests (id, org_id, created_at, status_code, duration_ms)
		VALUES ($1, $2, $3, 200, 120)`,
		db.MustNewID(), o.ID, now.AddDate(0, 18, 0)); err == nil {
		t.Error("an insert past the partition horizon succeeded; it must fail loudly")
	}
}

func TestUsageRecordUpsertIsIdempotent(t *testing.T) {
	pool := migratedDatabase(t)
	ctx := ctxT(t)

	o := newOrg(ctx, t, pool, "usage-org")
	var profileID uuid.UUID
	if err := pool.QueryRow(ctx,
		`SELECT id FROM pricing_profiles WHERE org_id IS NULL AND version = 1`).Scan(&profileID); err != nil {
		t.Fatalf("reading the default pricing profile: %v", err)
	}

	bucket := time.Now().UTC().Truncate(time.Hour)
	upsert := func(count int64) error {
		_, err := pool.Exec(ctx, `
			INSERT INTO usage_records (id, org_id, bucket_hour, request_count, pricing_profile_id)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (org_id, bucket_hour, deployment_id, api_key_id)
			DO UPDATE SET request_count = EXCLUDED.request_count, computed_at = now()`,
			db.MustNewID(), o.ID, bucket, count, profileID)
		return err
	}

	// NULL deployment_id and api_key_id: with the default NULLS DISTINCT this
	// would insert duplicates instead of merging, and at-least-once delivery
	// guarantees the rollup runs twice eventually.
	if err := upsert(10); err != nil {
		t.Fatalf("first upsert: %v", err)
	}
	if err := upsert(20); err != nil {
		t.Fatalf("second upsert: %v", err)
	}

	var rows int
	var count int64
	if err := pool.QueryRow(ctx,
		`SELECT count(*), max(request_count) FROM usage_records WHERE org_id = $1`, o.ID).
		Scan(&rows, &count); err != nil {
		t.Fatalf("counting usage records: %v", err)
	}
	if rows != 1 {
		t.Errorf("%d usage rows after two upserts of the same bucket, want 1", rows)
	}
	if count != 20 {
		t.Errorf("request_count = %d, want the merged value 20", count)
	}
}

func TestPoolAppliesSessionSettings(t *testing.T) {
	pool := migratedDatabase(t)
	ctx := ctxT(t)

	var appName, statementTimeout string
	if err := pool.QueryRow(ctx,
		`SELECT current_setting('application_name'), current_setting('statement_timeout')`).
		Scan(&appName, &statementTimeout); err != nil {
		t.Fatalf("reading session settings: %v", err)
	}
	if appName != "nebula-integration-test" {
		t.Errorf("application_name = %q; pg_stat_activity would be unreadable during an incident", appName)
	}
	if statementTimeout == "0" || statementTimeout == "" {
		t.Errorf("statement_timeout = %q, want a finite value", statementTimeout)
	}
}

func TestHealthChecker(t *testing.T) {
	pool := migratedDatabase(t)
	ctx := ctxT(t)

	checker := db.HealthChecker{Pool: pool, Timeout: 2 * time.Second}
	if err := checker.Check(ctx); err != nil {
		t.Fatalf("health check against a live database failed: %v", err)
	}
	if checker.Name() != "postgres" || !checker.Critical() {
		t.Error("the database checker must be named postgres and be critical")
	}

	// A wrong expected schema must make the pod unready.
	bad := db.HealthChecker{Pool: pool, Timeout: 2 * time.Second, ExpectedSchema: 9999}
	if err := bad.Check(ctx); err == nil {
		t.Error("health check passed against the wrong schema version")
	}

	var empty db.HealthChecker
	if err := empty.Check(ctx); err == nil {
		t.Error("health check passed with no pool configured")
	}
}

func TestPoolStats(t *testing.T) {
	pool := migratedDatabase(t)
	ctx := ctxT(t)

	var one int
	if err := pool.QueryRow(ctx, `SELECT 1`).Scan(&one); err != nil {
		t.Fatalf("query: %v", err)
	}
	s := db.PoolStats(pool)
	if s.MaxConns == 0 {
		t.Error("PoolStats reports MaxConns = 0")
	}
	if s.AcquireCount == 0 {
		t.Error("PoolStats reports no acquisitions after a query")
	}
}

var _ = pgxpool.Pool{}
