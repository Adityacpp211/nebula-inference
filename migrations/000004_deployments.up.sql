-- 000004 deployments, revisions, routes and traffic targets
--
-- Two decisions are load-bearing here:
--
--  * generation changes on SPEC changes only, never on a status write. That one
--    rule is what makes `generation <> observed_generation` a trustworthy
--    "there is work to do" predicate for the controller (ADR-0008).
--  * Clients address a ROUTE, not a deployment. Routes fan out to weighted
--    targets, which is what makes canary, A/B and rollback ordinary weight
--    updates rather than special cases in the request path (ADR-0009).

CREATE TABLE deployments (
    id                uuid             PRIMARY KEY,
    org_id            uuid             NOT NULL REFERENCES organizations (id) ON DELETE CASCADE,
    model_version_id  uuid             NOT NULL REFERENCES model_versions (id) ON DELETE RESTRICT,
    name              text             NOT NULL,
    namespace         text             NOT NULL,

    -- desired state, written by users and by the autoscaler
    desired_replicas  integer          NOT NULL DEFAULT 1,
    min_replicas      integer          NOT NULL DEFAULT 1,
    max_replicas      integer          NOT NULL DEFAULT 5,
    resources         jsonb            NOT NULL,
    runtime_overrides jsonb            NOT NULL DEFAULT '{}'::jsonb,
    autoscaling       jsonb            NOT NULL DEFAULT '{}'::jsonb,
    queue_config      jsonb            NOT NULL DEFAULT '{}'::jsonb,
    routing_policy_id uuid             REFERENCES routing_policies (id) ON DELETE SET NULL,

    generation        bigint           NOT NULL DEFAULT 1,
    current_revision  integer          NOT NULL DEFAULT 1,

    -- OBSERVED columns: written only by the controller, derived from Kubernetes.
    -- They are a read convenience, never a decision input (axiom A1). Live pod
    -- state is deliberately absent: that is Kubernetes' truth, cached in Redis.
    observed_generation bigint         NOT NULL DEFAULT 0,
    state               deployment_state NOT NULL DEFAULT 'pending',
    ready_replicas      integer        NOT NULL DEFAULT 0,
    updated_replicas    integer        NOT NULL DEFAULT 0,
    conditions          jsonb          NOT NULL DEFAULT '[]'::jsonb,
    last_error          text,
    last_synced_at      timestamptz,

    created_by uuid        REFERENCES users (id) ON DELETE SET NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    deleted_at timestamptz,

    CONSTRAINT uq_deployments__org_name UNIQUE (org_id, name),
    CONSTRAINT ck_deployments__name_format CHECK (name ~ '^[a-z0-9]([a-z0-9-]{0,45}[a-z0-9])?$'),
    CONSTRAINT ck_deployments__namespace_format CHECK (namespace ~ '^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$'),
    CONSTRAINT ck_deployments__replica_bounds CHECK (
        min_replicas >= 0
        AND max_replicas >= min_replicas
        AND desired_replicas BETWEEN min_replicas AND max_replicas
    ),
    CONSTRAINT ck_deployments__observed_counts CHECK (
        ready_replicas >= 0 AND updated_replicas >= 0
    ),
    CONSTRAINT ck_deployments__generation CHECK (generation > 0 AND observed_generation >= 0),
    CONSTRAINT ck_deployments__resources_object CHECK (jsonb_typeof(resources) = 'object'),
    CONSTRAINT ck_deployments__conditions_array CHECK (jsonb_typeof(conditions) = 'array')
);

CREATE INDEX ix_deployments__org ON deployments (org_id) WHERE deleted_at IS NULL;
CREATE INDEX ix_deployments__model_version ON deployments (model_version_id);
CREATE INDEX ix_deployments__state ON deployments (state) WHERE deleted_at IS NULL;

-- The controller's work queue predicate.
CREATE INDEX ix_deployments__needs_reconcile
    ON deployments (updated_at)
    WHERE generation <> observed_generation;

CREATE TRIGGER trg_deployments__updated_at
    BEFORE UPDATE ON deployments
    FOR EACH ROW EXECUTE FUNCTION nebula_set_updated_at();

-- Immutable spec snapshots. A rollback writes a NEW revision whose spec equals an
-- older one; history is never rewritten, which is what makes an incident
-- reviewable afterwards (ADR-0010).
CREATE TABLE deployment_revisions (
    id               uuid        PRIMARY KEY,
    deployment_id    uuid        NOT NULL REFERENCES deployments (id) ON DELETE CASCADE,
    revision         integer     NOT NULL,
    model_version_id uuid        NOT NULL REFERENCES model_versions (id) ON DELETE RESTRICT,
    spec             jsonb       NOT NULL,
    spec_hash        bytea       NOT NULL,
    reason           text        NOT NULL,
    created_by       uuid        REFERENCES users (id) ON DELETE SET NULL,
    created_at       timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT uq_deployment_revisions__dep_rev UNIQUE (deployment_id, revision),
    CONSTRAINT ck_deployment_revisions__revision CHECK (revision > 0),
    CONSTRAINT ck_deployment_revisions__reason CHECK (
        reason IN ('create', 'update', 'scale', 'rollback', 'rollout')
    ),
    CONSTRAINT ck_deployment_revisions__spec_object CHECK (jsonb_typeof(spec) = 'object'),
    CONSTRAINT ck_deployment_revisions__spec_hash_length CHECK (octet_length(spec_hash) = 32)
);

CREATE TRIGGER trg_deployment_revisions__immutable
    BEFORE UPDATE ON deployment_revisions
    FOR EACH ROW EXECUTE FUNCTION nebula_forbid_update();

-- A route is the stable public identity of a capability: the value clients put in
-- {"model": "..."}.
CREATE TABLE routes (
    id                uuid        PRIMARY KEY,
    org_id            uuid        NOT NULL REFERENCES organizations (id) ON DELETE CASCADE,
    model_name        text        NOT NULL,
    routing_policy_id uuid        REFERENCES routing_policies (id) ON DELETE SET NULL,
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT uq_routes__org_model_name UNIQUE (org_id, model_name),
    CONSTRAINT ck_routes__model_name_format CHECK (model_name ~ '^[A-Za-z0-9]([A-Za-z0-9._:-]{0,94}[A-Za-z0-9])?$')
);

CREATE TRIGGER trg_routes__updated_at
    BEFORE UPDATE ON routes
    FOR EACH ROW EXECUTE FUNCTION nebula_set_updated_at();

CREATE TABLE route_targets (
    id            uuid        PRIMARY KEY,
    route_id      uuid        NOT NULL REFERENCES routes (id) ON DELETE CASCADE,
    deployment_id uuid        NOT NULL REFERENCES deployments (id) ON DELETE RESTRICT,
    weight        integer     NOT NULL,
    is_baseline   boolean     NOT NULL DEFAULT false,
    label         text,
    updated_at    timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT uq_route_targets__route_dep UNIQUE (route_id, deployment_id),
    CONSTRAINT ck_route_targets__weight CHECK (weight BETWEEN 0 AND 100)
);

CREATE INDEX ix_route_targets__route ON route_targets (route_id);
CREATE INDEX ix_route_targets__deployment ON route_targets (deployment_id);

CREATE TRIGGER trg_route_targets__updated_at
    BEFORE UPDATE ON route_targets
    FOR EACH ROW EXECUTE FUNCTION nebula_set_updated_at();

-- At most one baseline per route.
CREATE UNIQUE INDEX uq_route_targets__one_baseline
    ON route_targets (route_id) WHERE is_baseline;

-- Weights must total exactly 100 for any route that has targets at all.
--
-- A DEFERRABLE constraint trigger, so a transaction may legitimately pass through
-- an inconsistent intermediate state while shifting weight from one target to
-- another, and is only checked at COMMIT. A route summing to 90 would silently
-- drop 10% of traffic, which is exactly the class of bug that should be
-- impossible rather than caught in review.
CREATE FUNCTION nebula_check_route_weights() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    rid   uuid := COALESCE(NEW.route_id, OLD.route_id);
    cnt   integer;
    total integer;
BEGIN
    -- The route itself may have been deleted in this transaction, in which case
    -- its targets went with it and there is nothing to check.
    IF NOT EXISTS (SELECT 1 FROM routes WHERE id = rid) THEN
        RETURN NULL;
    END IF;

    SELECT count(*), COALESCE(sum(weight), 0) INTO cnt, total
      FROM route_targets WHERE route_id = rid;

    IF cnt > 0 AND total <> 100 THEN
        RAISE EXCEPTION
            'route % has % target(s) with weights totalling %; they must total 100', rid, cnt, total
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER trg_route_targets__weights_total_100
    AFTER INSERT OR UPDATE OR DELETE ON route_targets
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION nebula_check_route_weights();
