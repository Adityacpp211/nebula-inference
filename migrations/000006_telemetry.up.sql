-- 000006 telemetry, usage and history
--
-- Three different kinds of table live here, and the difference matters:
--
--  * OBSERVED   (nodes)              a rebuildable cache of Kubernetes state
--  * APPEND-ONLY (worker_events, autoscaling_events, audit_logs, requests)
--                                    historical facts; no UPDATE, no DELETE
--  * ROLLUP    (usage_records)       derived, idempotent, retained indefinitely

-- A cache of Kubernetes Node objects for the dashboard, capacity admission and
-- historical capacity questions. Marked OBSERVED: it carries synced_at, it is
-- safe to truncate, and it is never consulted for a decision the live informer
-- could answer (axiom A1). It has no foreign keys because nodes are not
-- tenant-scoped.
CREATE TABLE nodes (
    id              uuid        PRIMARY KEY,
    name            text        NOT NULL,
    provider_id     text,
    labels          jsonb       NOT NULL DEFAULT '{}'::jsonb,
    taints          jsonb       NOT NULL DEFAULT '[]'::jsonb,
    capacity        jsonb       NOT NULL,
    allocatable     jsonb       NOT NULL,
    conditions      jsonb       NOT NULL DEFAULT '[]'::jsonb,
    schedulable     boolean     NOT NULL DEFAULT true,
    kubelet_version text,
    first_seen_at   timestamptz NOT NULL DEFAULT now(),
    synced_at       timestamptz NOT NULL DEFAULT now(),
    removed_at      timestamptz,

    CONSTRAINT uq_nodes__name UNIQUE (name),
    CONSTRAINT ck_nodes__capacity_object CHECK (jsonb_typeof(capacity) = 'object'),
    CONSTRAINT ck_nodes__allocatable_object CHECK (jsonb_typeof(allocatable) = 'object')
);

CREATE INDEX ix_nodes__present ON nodes (name) WHERE removed_at IS NULL;

-- Why a deployment is degraded, answerable a day later. Kubernetes Events expire
-- in about an hour by default, so without this the explanation evaporates before
-- the next stand-up.
CREATE TABLE worker_events (
    id            uuid        PRIMARY KEY,
    org_id        uuid        NOT NULL,
    deployment_id uuid        NOT NULL,
    pod_name      text        NOT NULL,
    node_name     text,
    event_type    text        NOT NULL,
    detail        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    occurred_at   timestamptz NOT NULL,

    CONSTRAINT ck_worker_events__type CHECK (event_type IN (
        'scheduled', 'artifact_pulling', 'artifact_cached', 'model_loading', 'model_loaded',
        'model_load_failed', 'ready', 'unhealthy', 'engine_crashed', 'crashed', 'oom_killed',
        'evicted', 'draining', 'terminated'
    )),
    CONSTRAINT ck_worker_events__detail_object CHECK (jsonb_typeof(detail) = 'object')
);

CREATE INDEX ix_worker_events__dep_time ON worker_events (deployment_id, occurred_at DESC);
CREATE INDEX ix_worker_events__org_time ON worker_events (org_id, occurred_at DESC);

CREATE TRIGGER trg_worker_events__immutable
    BEFORE UPDATE ON worker_events
    FOR EACH ROW EXECUTE FUNCTION nebula_forbid_update();

-- Every autoscaling decision, INCLUDING the decision not to act. Recording
-- suppressed decisions is what turns "why didn't it scale?" from an argument into
-- a query, and it is the diagnostic that makes oscillation tractable.
CREATE TABLE autoscaling_events (
    id            uuid        PRIMARY KEY,
    org_id        uuid        NOT NULL,
    deployment_id uuid        NOT NULL REFERENCES deployments (id) ON DELETE CASCADE,
    from_replicas integer     NOT NULL,
    to_replicas   integer     NOT NULL,
    direction     text        NOT NULL,
    signals       jsonb       NOT NULL,
    decision      text        NOT NULL,
    reason        text        NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT ck_autoscaling_events__direction CHECK (direction IN ('up', 'down', 'hold')),
    CONSTRAINT ck_autoscaling_events__decision CHECK (decision IN (
        'scaled', 'suppressed_cooldown', 'suppressed_stabilization', 'suppressed_rollout',
        'suppressed_no_signal', 'clamped_max', 'clamped_min', 'disabled'
    )),
    CONSTRAINT ck_autoscaling_events__replicas CHECK (from_replicas >= 0 AND to_replicas >= 0),
    CONSTRAINT ck_autoscaling_events__signals_array CHECK (jsonb_typeof(signals) = 'array')
);

CREATE INDEX ix_autoscaling_events__dep_time ON autoscaling_events (deployment_id, created_at DESC);

CREATE TRIGGER trg_autoscaling_events__immutable
    BEFORE UPDATE ON autoscaling_events
    FOR EACH ROW EXECUTE FUNCTION nebula_forbid_update();

-- One row per inference request.
--
-- Deliberately has NO foreign keys: FK checks cost throughput on a high-rate
-- insert path, and they would block dropping a parent row. Referential integrity
-- here is accepted as eventual, and the columns are historical snapshots — a
-- deployment may later be deleted while its request rows remain (ADR-0017).
--
-- Partitioned monthly with bounded retention, because a table that grows forever
-- is an outage with a delay fuse.
CREATE TABLE requests (
    id                    uuid        NOT NULL,
    org_id                uuid        NOT NULL,
    created_at            timestamptz NOT NULL,
    route_id              uuid,
    deployment_id         uuid,
    model_version_id      uuid,
    api_key_id            uuid,
    experiment_id         uuid,
    variant               text,
    endpoint              text        NOT NULL DEFAULT 'chat.completions',
    priority              text        NOT NULL DEFAULT 'NORMAL',
    status_code           smallint    NOT NULL,
    error_class           text,
    outcome               text        NOT NULL DEFAULT 'completed',
    streamed              boolean     NOT NULL DEFAULT false,
    queue_wait_ms         integer     NOT NULL DEFAULT 0,
    ttft_ms               integer,
    duration_ms           integer     NOT NULL,
    compute_ms            integer     NOT NULL DEFAULT 0,
    attempts              smallint    NOT NULL DEFAULT 1,
    prompt_tokens         integer     NOT NULL DEFAULT 0,
    completion_tokens     integer     NOT NULL DEFAULT 0,
    worker_pod            text,
    node_name             text,
    trace_id              text,
    sample_rate           real        NOT NULL DEFAULT 1.0,
    estimated_cost_micros bigint      NOT NULL DEFAULT 0,

    PRIMARY KEY (id, created_at),
    CONSTRAINT ck_requests__priority CHECK (priority IN ('LOW', 'NORMAL', 'HIGH')),
    CONSTRAINT ck_requests__outcome CHECK (outcome IN (
        'completed', 'client_cancelled', 'stream_interrupted', 'deadline_exceeded',
        'queue_timeout', 'rejected'
    )),
    CONSTRAINT ck_requests__non_negative CHECK (
        queue_wait_ms >= 0 AND duration_ms >= 0 AND compute_ms >= 0
        AND prompt_tokens >= 0 AND completion_tokens >= 0
        AND attempts >= 1 AND estimated_cost_micros >= 0
        AND (ttft_ms IS NULL OR ttft_ms >= 0)
    ),
    CONSTRAINT ck_requests__sample_rate CHECK (sample_rate > 0 AND sample_rate <= 1)
) PARTITION BY RANGE (created_at);

CREATE INDEX ix_requests__org_time ON requests (org_id, created_at DESC);
CREATE INDEX ix_requests__deployment_time ON requests (deployment_id, created_at DESC);
CREATE INDEX ix_requests__errors ON requests (created_at DESC) WHERE error_class IS NOT NULL;
CREATE INDEX ix_requests__trace ON requests (trace_id) WHERE trace_id IS NOT NULL;

-- Hourly rollups. This, not requests, is what the cost and usage APIs read, and
-- it is retained indefinitely.
--
-- UNIQUE ... NULLS NOT DISTINCT is essential: deployment_id and api_key_id are
-- nullable, and with the default NULLS DISTINCT the ON CONFLICT upsert that makes
-- the rollup idempotent would silently insert duplicates instead of merging.
-- At-least-once delivery guarantees the rollup runs twice eventually.
CREATE TABLE usage_records (
    id                    uuid          PRIMARY KEY,
    org_id                uuid          NOT NULL REFERENCES organizations (id) ON DELETE RESTRICT,
    bucket_hour           timestamptz   NOT NULL,
    deployment_id         uuid,
    model_version_id      uuid,
    api_key_id            uuid,
    request_count         bigint        NOT NULL,
    error_count           bigint        NOT NULL DEFAULT 0,
    prompt_tokens         bigint        NOT NULL DEFAULT 0,
    completion_tokens     bigint        NOT NULL DEFAULT 0,
    compute_seconds       numeric(14,3) NOT NULL DEFAULT 0,
    gpu_seconds           numeric(14,3) NOT NULL DEFAULT 0,
    sample_rate           real          NOT NULL DEFAULT 1.0,
    pricing_profile_id    uuid          NOT NULL REFERENCES pricing_profiles (id) ON DELETE RESTRICT,
    estimated_cost_micros bigint        NOT NULL DEFAULT 0,
    computed_at           timestamptz   NOT NULL DEFAULT now(),

    CONSTRAINT uq_usage_records__bucket
        UNIQUE NULLS NOT DISTINCT (org_id, bucket_hour, deployment_id, api_key_id),
    CONSTRAINT ck_usage_records__bucket_aligned CHECK (bucket_hour = date_trunc('hour', bucket_hour)),
    CONSTRAINT ck_usage_records__non_negative CHECK (
        request_count >= 0 AND error_count >= 0 AND prompt_tokens >= 0
        AND completion_tokens >= 0 AND compute_seconds >= 0 AND gpu_seconds >= 0
        AND estimated_cost_micros >= 0
    ),
    CONSTRAINT ck_usage_records__errors_bounded CHECK (error_count <= request_count),
    CONSTRAINT ck_usage_records__sample_rate CHECK (sample_rate > 0 AND sample_rate <= 1)
);

CREATE INDEX ix_usage_records__org_bucket ON usage_records (org_id, bucket_hour DESC);
CREATE INDEX ix_usage_records__deployment_bucket ON usage_records (deployment_id, bucket_hour DESC);

-- Append-only audit trail. Enforced by grants in 000007 as well as this trigger:
-- the application role receives INSERT and SELECT only.
CREATE TABLE audit_logs (
    id            uuid        NOT NULL,
    org_id        uuid        NOT NULL,
    created_at    timestamptz NOT NULL,
    actor_type    text        NOT NULL,
    actor_id      uuid,
    actor_label   text,
    action        text        NOT NULL,
    resource_type text        NOT NULL,
    resource_id   uuid,
    before        jsonb,
    after         jsonb,
    request_id    uuid,
    ip            inet,
    user_agent    text,

    PRIMARY KEY (id, created_at),
    CONSTRAINT ck_audit_logs__actor_type CHECK (actor_type IN ('user', 'api_key', 'system')),
    CONSTRAINT ck_audit_logs__action_nonempty CHECK (length(action) > 0)
) PARTITION BY RANGE (created_at);

CREATE INDEX ix_audit_logs__org_time ON audit_logs (org_id, created_at DESC);
CREATE INDEX ix_audit_logs__action_time ON audit_logs (action, created_at DESC);
CREATE INDEX ix_audit_logs__resource ON audit_logs (resource_type, resource_id);

CREATE TRIGGER trg_audit_logs__immutable
    BEFORE UPDATE ON audit_logs
    FOR EACH ROW EXECUTE FUNCTION nebula_forbid_update();

-- Initial partitions: the current month plus the next two, giving roughly a
-- quarter of runway. The Phase 13 maintenance job extends this horizon and drops
-- expired partitions.
--
-- There is deliberately NO default partition: an insert past the horizon fails
-- loudly, which is recoverable, whereas rows landing in a default partition
-- silently block attaching the proper partition later.
DO $$
DECLARE
    m date;
BEGIN
    FOR i IN 0..2 LOOP
        m := (date_trunc('month', now()) + (i || ' month')::interval)::date;
        PERFORM nebula_ensure_month_partition('requests'::regclass, m);
        PERFORM nebula_ensure_month_partition('audit_logs'::regclass, m);
    END LOOP;
END;
$$;
