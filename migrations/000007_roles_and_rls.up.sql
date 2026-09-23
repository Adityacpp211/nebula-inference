-- 000007 database roles, grants and row-level security
--
-- Tenant isolation is enforced TWICE (ADR-0022): every application query filters
-- on org_id, and these policies filter again in the database. A forgotten WHERE
-- clause then returns an empty result set instead of another tenant's rows.
--
-- Two notes on scope:
--
--  * Roles are CLUSTER-level objects, not schema objects. They are created here
--    idempotently so a fresh database is usable, but the down migration does NOT
--    drop them: another database in the same cluster may be using them. Dropping
--    a table automatically drops its grants, so nothing is left dangling.
--  * Roles are created NOLOGIN. Granting login and a password is an operator
--    decision made out of band, so no migration ever creates a usable credential.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'nebula_app') THEN
        CREATE ROLE nebula_app NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'nebula_controller') THEN
        CREATE ROLE nebula_controller NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'nebula_readonly') THEN
        CREATE ROLE nebula_readonly NOLOGIN;
    END IF;
END;
$$;

GRANT USAGE ON SCHEMA public TO nebula_app, nebula_controller, nebula_readonly;

-- ---------------------------------------------------------------------------
-- nebula_app: the control plane. The only role that writes desired state.
-- ---------------------------------------------------------------------------
GRANT SELECT, INSERT, UPDATE, DELETE ON
    organizations, users, api_keys, models, model_versions, model_artifacts,
    deployments, routes, route_targets, routing_policies, rate_limit_policies,
    canary_rollouts, experiments
TO nebula_app;

-- Append-only and immutable tables: INSERT and SELECT only, at the grant level.
-- An immutability rule expressed only as a trigger is one careless migration away
-- from not existing; a missing UPDATE grant is not.
GRANT SELECT, INSERT ON deployment_revisions, pricing_profiles, audit_logs TO nebula_app;

-- Read-only from the control plane's point of view: these are written by the
-- controller from observed state.
GRANT SELECT ON nodes, worker_events, autoscaling_events, requests, usage_records,
    schema_migrations TO nebula_app;

-- ---------------------------------------------------------------------------
-- nebula_controller: reconcilers, the usage ingester and the autoscaler.
--
-- Column-level UPDATE grants, not table-level: the controller owns the OBSERVED
-- columns and the autoscaler owns desired_replicas, and neither may rewrite a
-- user's spec. This is axiom A1 expressed as a database permission.
-- ---------------------------------------------------------------------------
GRANT SELECT ON ALL TABLES IN SCHEMA public TO nebula_controller;

GRANT UPDATE (
    observed_generation, state, ready_replicas, updated_replicas,
    conditions, last_error, last_synced_at,
    desired_replicas,   -- the autoscaler's single writable field
    updated_at
) ON deployments TO nebula_controller;

-- The rollout reconciler shifts traffic weights.
GRANT UPDATE (weight, is_baseline, label, updated_at) ON route_targets TO nebula_controller;
GRANT INSERT, DELETE ON route_targets TO nebula_controller;

-- The rollout state machine.
GRANT UPDATE (
    state, current_step, last_analysis, abort_reason,
    started_at, step_entered_at, finished_at
) ON canary_rollouts TO nebula_controller;

-- The inventory reconciler owns the node cache outright: it is rebuildable.
GRANT INSERT, UPDATE, DELETE ON nodes TO nebula_controller;

-- Event and telemetry ingestion.
GRANT INSERT ON worker_events, autoscaling_events, requests, audit_logs TO nebula_controller;
-- usage_records is an idempotent upsert, so it needs UPDATE as well as INSERT.
GRANT INSERT, UPDATE ON usage_records TO nebula_controller;

-- ---------------------------------------------------------------------------
-- nebula_readonly: analytics and anything that must never write.
-- ---------------------------------------------------------------------------
GRANT SELECT ON ALL TABLES IN SCHEMA public TO nebula_readonly;

-- ---------------------------------------------------------------------------
-- Row-level security
-- ---------------------------------------------------------------------------

-- Reads the tenant for the current transaction. Returns NULL when unset, so every
-- policy predicate evaluates to NULL and no rows are visible: RLS fails CLOSED.
CREATE FUNCTION nebula_current_org() RETURNS uuid
LANGUAGE sql STABLE AS $$
    SELECT nullif(current_setting('app.current_org', true), '')::uuid
$$;

COMMENT ON FUNCTION nebula_current_org() IS
    'Tenant for the current transaction, from the app.current_org session variable. '
    'NULL when unset, which makes every RLS policy deny.';

-- Tables carrying org_id directly.
ALTER TABLE organizations       ENABLE ROW LEVEL SECURITY;
ALTER TABLE users               ENABLE ROW LEVEL SECURITY;
ALTER TABLE api_keys            ENABLE ROW LEVEL SECURITY;
ALTER TABLE models              ENABLE ROW LEVEL SECURITY;
ALTER TABLE deployments         ENABLE ROW LEVEL SECURITY;
ALTER TABLE routes              ENABLE ROW LEVEL SECURITY;
ALTER TABLE canary_rollouts     ENABLE ROW LEVEL SECURITY;
ALTER TABLE experiments         ENABLE ROW LEVEL SECURITY;
ALTER TABLE worker_events       ENABLE ROW LEVEL SECURITY;
ALTER TABLE autoscaling_events  ENABLE ROW LEVEL SECURITY;
ALTER TABLE requests            ENABLE ROW LEVEL SECURITY;
ALTER TABLE usage_records       ENABLE ROW LEVEL SECURITY;
ALTER TABLE audit_logs          ENABLE ROW LEVEL SECURITY;
-- Tables with a nullable org_id, where NULL means a built-in shared row.
ALTER TABLE pricing_profiles    ENABLE ROW LEVEL SECURITY;
ALTER TABLE routing_policies    ENABLE ROW LEVEL SECURITY;
ALTER TABLE rate_limit_policies ENABLE ROW LEVEL SECURITY;
-- Child tables reached through a parent's org_id.
ALTER TABLE model_versions       ENABLE ROW LEVEL SECURITY;
ALTER TABLE model_artifacts      ENABLE ROW LEVEL SECURITY;
ALTER TABLE deployment_revisions ENABLE ROW LEVEL SECURITY;
ALTER TABLE route_targets        ENABLE ROW LEVEL SECURITY;

-- nodes has no org_id: node inventory is cluster-wide infrastructure, not tenant
-- data, so it is deliberately not RLS-protected. It is exposed only to the admin
-- scope at the API layer.

CREATE POLICY org_isolation ON organizations FOR ALL TO nebula_app
    USING (id = nebula_current_org()) WITH CHECK (id = nebula_current_org());

CREATE POLICY org_isolation ON users FOR ALL TO nebula_app
    USING (org_id = nebula_current_org()) WITH CHECK (org_id = nebula_current_org());

CREATE POLICY org_isolation ON api_keys FOR ALL TO nebula_app
    USING (org_id = nebula_current_org()) WITH CHECK (org_id = nebula_current_org());

CREATE POLICY org_isolation ON models FOR ALL TO nebula_app
    USING (org_id = nebula_current_org()) WITH CHECK (org_id = nebula_current_org());

CREATE POLICY org_isolation ON deployments FOR ALL TO nebula_app
    USING (org_id = nebula_current_org()) WITH CHECK (org_id = nebula_current_org());

CREATE POLICY org_isolation ON routes FOR ALL TO nebula_app
    USING (org_id = nebula_current_org()) WITH CHECK (org_id = nebula_current_org());

CREATE POLICY org_isolation ON canary_rollouts FOR ALL TO nebula_app
    USING (org_id = nebula_current_org()) WITH CHECK (org_id = nebula_current_org());

CREATE POLICY org_isolation ON experiments FOR ALL TO nebula_app
    USING (org_id = nebula_current_org()) WITH CHECK (org_id = nebula_current_org());

CREATE POLICY org_isolation ON worker_events FOR ALL TO nebula_app
    USING (org_id = nebula_current_org()) WITH CHECK (org_id = nebula_current_org());

CREATE POLICY org_isolation ON autoscaling_events FOR ALL TO nebula_app
    USING (org_id = nebula_current_org()) WITH CHECK (org_id = nebula_current_org());

CREATE POLICY org_isolation ON requests FOR ALL TO nebula_app
    USING (org_id = nebula_current_org()) WITH CHECK (org_id = nebula_current_org());

CREATE POLICY org_isolation ON usage_records FOR ALL TO nebula_app
    USING (org_id = nebula_current_org()) WITH CHECK (org_id = nebula_current_org());

CREATE POLICY org_isolation ON audit_logs FOR ALL TO nebula_app
    USING (org_id = nebula_current_org()) WITH CHECK (org_id = nebula_current_org());

-- Built-in rows (org_id IS NULL) are readable by every tenant, but a tenant may
-- only create rows owned by itself: the WITH CHECK is deliberately stricter than
-- the USING clause.
CREATE POLICY org_isolation ON pricing_profiles FOR ALL TO nebula_app
    USING (org_id IS NULL OR org_id = nebula_current_org())
    WITH CHECK (org_id = nebula_current_org());

CREATE POLICY org_isolation ON routing_policies FOR ALL TO nebula_app
    USING (org_id IS NULL OR org_id = nebula_current_org())
    WITH CHECK (org_id = nebula_current_org());

CREATE POLICY org_isolation ON rate_limit_policies FOR ALL TO nebula_app
    USING (org_id IS NULL OR org_id = nebula_current_org())
    WITH CHECK (org_id = nebula_current_org());

-- Child tables: reached through the parent's org_id. The subquery is itself
-- subject to the parent's policy, so this is belt and braces; being explicit
-- keeps the predicate obvious to a reviewer.
--
-- TODO(NEB-104): if profiling shows these EXISTS predicates cost too much at
-- volume, denormalise org_id onto these tables with a composite foreign key to
-- the parent (org_id, id) so the predicate becomes a plain column comparison.
CREATE POLICY org_isolation ON model_versions FOR ALL TO nebula_app
    USING (EXISTS (SELECT 1 FROM models m
                    WHERE m.id = model_versions.model_id AND m.org_id = nebula_current_org()))
    WITH CHECK (EXISTS (SELECT 1 FROM models m
                    WHERE m.id = model_versions.model_id AND m.org_id = nebula_current_org()));

CREATE POLICY org_isolation ON model_artifacts FOR ALL TO nebula_app
    USING (EXISTS (SELECT 1 FROM model_versions mv JOIN models m ON m.id = mv.model_id
                    WHERE mv.id = model_artifacts.model_version_id AND m.org_id = nebula_current_org()))
    WITH CHECK (EXISTS (SELECT 1 FROM model_versions mv JOIN models m ON m.id = mv.model_id
                    WHERE mv.id = model_artifacts.model_version_id AND m.org_id = nebula_current_org()));

CREATE POLICY org_isolation ON deployment_revisions FOR ALL TO nebula_app
    USING (EXISTS (SELECT 1 FROM deployments d
                    WHERE d.id = deployment_revisions.deployment_id AND d.org_id = nebula_current_org()))
    WITH CHECK (EXISTS (SELECT 1 FROM deployments d
                    WHERE d.id = deployment_revisions.deployment_id AND d.org_id = nebula_current_org()));

CREATE POLICY org_isolation ON route_targets FOR ALL TO nebula_app
    USING (EXISTS (SELECT 1 FROM routes r
                    WHERE r.id = route_targets.route_id AND r.org_id = nebula_current_org()))
    WITH CHECK (EXISTS (SELECT 1 FROM routes r
                    WHERE r.id = route_targets.route_id AND r.org_id = nebula_current_org()));

-- The controller and the read-only role work across tenants by design: the
-- controller reconciles the whole cluster, and analytics spans organizations.
-- This is an EXPLICIT, visible bypass rather than the BYPASSRLS role attribute,
-- which is invisible in the schema and unavailable on some managed databases.
DO $$
DECLARE
    t text;
    rls_tables text[] := ARRAY[
        'organizations', 'users', 'api_keys', 'models', 'model_versions', 'model_artifacts',
        'deployments', 'deployment_revisions', 'routes', 'route_targets',
        'routing_policies', 'rate_limit_policies', 'pricing_profiles',
        'canary_rollouts', 'experiments',
        'worker_events', 'autoscaling_events', 'requests', 'usage_records', 'audit_logs'
    ];
BEGIN
    FOREACH t IN ARRAY rls_tables LOOP
        EXECUTE format(
            'CREATE POLICY controller_access ON %I FOR ALL TO nebula_controller USING (true) WITH CHECK (true)', t);
        EXECUTE format(
            'CREATE POLICY readonly_access ON %I FOR SELECT TO nebula_readonly USING (true)', t);
    END LOOP;
END;
$$;
