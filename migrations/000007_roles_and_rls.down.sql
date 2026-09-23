-- 000007 down
--
-- Roles are deliberately NOT dropped: they are cluster-level objects that another
-- database in the same cluster may be using. Dropping a table drops its grants
-- automatically, and role creation in the up migration is idempotent, so a
-- down/up cycle is clean.

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
        EXECUTE format('DROP POLICY IF EXISTS org_isolation ON %I', t);
        EXECUTE format('DROP POLICY IF EXISTS controller_access ON %I', t);
        EXECUTE format('DROP POLICY IF EXISTS readonly_access ON %I', t);
        EXECUTE format('ALTER TABLE %I DISABLE ROW LEVEL SECURITY', t);
    END LOOP;
END;
$$;

DROP FUNCTION IF EXISTS nebula_current_org();

-- Revoke what was granted, so the roles are left with no residual access to a
-- schema that is about to be torn down by earlier down migrations.
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM nebula_app, nebula_controller, nebula_readonly;
REVOKE USAGE ON SCHEMA public FROM nebula_app, nebula_controller, nebula_readonly;
