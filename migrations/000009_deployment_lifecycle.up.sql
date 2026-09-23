-- 000009 deployment lifecycle: explicit states, transition validation, transition history
--
-- Phase 1 shipped deployment_state as
--   pending | progressing | ready | degraded | failed | deleting | deleted
--
-- Phase 2 replaces it with the eight states the lifecycle actually has:
--   pending | provisioning | starting | ready | degraded | failed | stopping | stopped
--
-- Two changes of substance, recorded in ADR-0027:
--
--  * progressing splits into provisioning and starting. They fail for different
--    reasons and need different timeouts: provisioning is NEBULA applying objects
--    (fails fast, on admission or RBAC), starting is a pod pulling a multi-gigabyte
--    artifact and loading it (slow, and a five-minute wait is normal). One state
--    covering both cannot express "stuck" usefully.
--  * deleting/deleted become stopping/stopped, and deletion moves to deleted_at.
--    Stopping a deployment and deleting its record are different operations: a
--    stopped deployment keeps its history, its revisions and its cost records, and
--    can be restarted. Conflating them meant "deleted" was the only way to express
--    "not running", which loses the record you need for the next investigation.
--
-- This migration is editable only because no NEBULA instance has run in production
-- yet. The checksum drift detection in the migration runner is what makes editing
-- 000004 in place impossible instead of merely inadvisable.

-- ---------------------------------------------------------------------------
-- 1. Replace the enum
-- ---------------------------------------------------------------------------
ALTER TYPE deployment_state RENAME TO deployment_state_v1;

CREATE TYPE deployment_state AS ENUM (
    'pending',       -- record exists; nothing has been provisioned
    'provisioning',  -- controller is creating or updating Kubernetes objects
    'starting',      -- objects exist; pods are pulling artifacts and loading models
    'ready',         -- desired replicas are ready and serving
    'degraded',      -- serving, but below desired replicas
    'failed',        -- cannot proceed without intervention
    'stopping',      -- draining and scaling to zero
    'stopped'        -- no replicas running; record, history and revisions retained
);

ALTER TABLE deployments ALTER COLUMN state DROP DEFAULT;

ALTER TABLE deployments
    ALTER COLUMN state TYPE deployment_state
    USING (
        CASE state::text
            WHEN 'progressing' THEN 'provisioning'
            WHEN 'deleting'    THEN 'stopping'
            WHEN 'deleted'     THEN 'stopped'
            ELSE state::text
        END
    )::deployment_state;

ALTER TABLE deployments ALTER COLUMN state SET DEFAULT 'pending';

DROP TYPE deployment_state_v1;

-- ---------------------------------------------------------------------------
-- 2. State metadata on the deployment
-- ---------------------------------------------------------------------------

-- How long the deployment has been in its current state. This is what makes
-- "stuck in starting for 20 minutes" a query rather than a guess, and it is
-- maintained by the trigger below so no writer can forget it.
ALTER TABLE deployments
    ADD COLUMN state_entered_at timestamptz NOT NULL DEFAULT now();

-- Machine-matchable reason in Kubernetes' CamelCase condition-reason convention
-- (ModelLoadFailed, InsufficientCapacity, ...) and the human sentence beside it.
ALTER TABLE deployments ADD COLUMN state_reason  text;
ALTER TABLE deployments ADD COLUMN state_message text;

-- A deployment may only be soft-deleted from a resting state. Deleting a running
-- deployment would orphan its Kubernetes objects, so stopping it first is not a
-- convention to remember but a constraint.
ALTER TABLE deployments
    ADD CONSTRAINT ck_deployments__delete_only_when_resting
    CHECK (deleted_at IS NULL OR state IN ('pending', 'failed', 'stopped'));

-- ---------------------------------------------------------------------------
-- 3. Transition history
-- ---------------------------------------------------------------------------

-- Every state change, append-only. Written by the trigger rather than by the
-- application, so a transition cannot happen without being recorded — including
-- one made by hand in psql during an incident.
CREATE TABLE deployment_state_transitions (
    id            uuid             PRIMARY KEY,
    org_id        uuid             NOT NULL,
    deployment_id uuid             NOT NULL REFERENCES deployments (id) ON DELETE CASCADE,
    from_state    deployment_state,           -- NULL for the initial row
    to_state      deployment_state NOT NULL,
    reason        text,
    message       text,
    generation    bigint           NOT NULL,
    actor_type    text             NOT NULL DEFAULT 'system',
    actor_id      uuid,
    occurred_at   timestamptz      NOT NULL DEFAULT now(),

    CONSTRAINT ck_dst__actor_type CHECK (actor_type IN ('user', 'api_key', 'system')),
    CONSTRAINT ck_dst__distinct_states CHECK (from_state IS NULL OR from_state <> to_state)
);

CREATE INDEX ix_dst__deployment_time ON deployment_state_transitions (deployment_id, occurred_at DESC);
CREATE INDEX ix_dst__org_time ON deployment_state_transitions (org_id, occurred_at DESC);
CREATE INDEX ix_dst__to_state ON deployment_state_transitions (to_state, occurred_at DESC);

CREATE TRIGGER trg_dst__immutable
    BEFORE UPDATE ON deployment_state_transitions
    FOR EACH ROW EXECUTE FUNCTION nebula_forbid_update();

-- ---------------------------------------------------------------------------
-- 4. Transition validation
-- ---------------------------------------------------------------------------

-- The legal transitions, as data. This table is the same graph encoded in
-- packages/lifecycle, and a test asserts the two agree; if they ever diverge the
-- database wins and the application would start failing writes, so the test
-- exists to catch it before that happens.
CREATE TABLE deployment_state_edges (
    from_state deployment_state NOT NULL,
    to_state   deployment_state NOT NULL,
    note       text             NOT NULL,
    PRIMARY KEY (from_state, to_state),
    CONSTRAINT ck_dse__distinct CHECK (from_state <> to_state)
);

INSERT INTO deployment_state_edges (from_state, to_state, note) VALUES
    -- forward path
    ('pending',      'provisioning', 'controller claimed the deployment and began applying objects'),
    ('provisioning', 'starting',     'objects applied; pods are starting'),
    ('starting',     'ready',        'all desired replicas became ready'),
    ('starting',     'degraded',     'some replicas became ready; others have not'),
    ('ready',        'degraded',     'a replica was lost'),
    ('degraded',     'ready',        'replicas recovered'),

    -- re-reconciliation after a spec change: back to provisioning, never straight
    -- to starting, because new objects have to be applied first
    ('ready',        'provisioning', 'spec changed; reconciling the new revision'),
    ('degraded',     'provisioning', 'spec changed; reconciling the new revision'),
    ('starting',     'provisioning', 'spec changed while starting; reconciling the new revision'),

    -- failure, from anywhere work can fail
    ('pending',      'failed',       'admission rejected the deployment'),
    ('provisioning', 'failed',       'applying objects failed'),
    ('starting',     'failed',       'no replica could start'),
    ('ready',        'failed',       'every replica was lost'),
    ('degraded',     'failed',       'the remaining replicas were lost'),
    ('stopping',     'failed',       'the deployment could not be stopped'),

    -- recovery: a new revision may be retried
    ('failed',       'provisioning', 'a new revision was applied after the failure'),

    -- stopping, from anywhere
    ('pending',      'stopping',     'stopped before provisioning began'),
    ('provisioning', 'stopping',     'stop requested during provisioning'),
    ('starting',     'stopping',     'stop requested while starting'),
    ('ready',        'stopping',     'stop requested'),
    ('degraded',     'stopping',     'stop requested'),
    ('failed',       'stopping',     'stop requested after failure'),
    ('stopping',     'stopped',      'all replicas are gone'),

    -- restart
    ('stopped',      'pending',      'start requested');

-- Validates the transition, stamps state_entered_at, and records history.
--
-- Doing all three in one trigger means they cannot come apart: there is no path
-- that changes state without validation, without updating the timer, or without
-- an audit row. The actor comes from session variables set alongside the RLS
-- tenant, so an application write is attributed and a manual one is recorded as
-- 'system'.
CREATE FUNCTION nebula_deployment_state_transition() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    v_actor_id   uuid;
    v_actor_type text;
BEGIN
    IF NEW.state = OLD.state THEN
        -- Not a transition. Reason and message may still be refreshed (for
        -- example a degraded deployment losing another replica), but the timer
        -- must not restart or "stuck for 20 minutes" becomes unmeasurable.
        RETURN NEW;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM deployment_state_edges
         WHERE from_state = OLD.state AND to_state = NEW.state
    ) THEN
        RAISE EXCEPTION 'illegal deployment state transition % -> % for deployment %',
            OLD.state, NEW.state, OLD.id
            USING ERRCODE = 'check_violation',
                  HINT = 'see deployment_state_edges for the legal transitions';
    END IF;

    NEW.state_entered_at := now();

    v_actor_id := nullif(current_setting('app.current_actor', true), '')::uuid;
    v_actor_type := coalesce(nullif(current_setting('app.current_actor_type', true), ''), 'system');

    INSERT INTO deployment_state_transitions (
        id, org_id, deployment_id, from_state, to_state,
        reason, message, generation, actor_type, actor_id
    ) VALUES (
        gen_random_uuid(), NEW.org_id, NEW.id, OLD.state, NEW.state,
        NEW.state_reason, NEW.state_message, NEW.generation, v_actor_type, v_actor_id
    );

    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_deployments__state_transition
    BEFORE UPDATE OF state ON deployments
    FOR EACH ROW EXECUTE FUNCTION nebula_deployment_state_transition();

-- Records the initial state so every deployment's history starts at its creation
-- rather than at its first change.
CREATE FUNCTION nebula_deployment_initial_state() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO deployment_state_transitions (
        id, org_id, deployment_id, from_state, to_state,
        reason, message, generation, actor_type, actor_id
    ) VALUES (
        gen_random_uuid(), NEW.org_id, NEW.id, NULL, NEW.state,
        coalesce(NEW.state_reason, 'Created'), NEW.state_message, NEW.generation,
        coalesce(nullif(current_setting('app.current_actor_type', true), ''), 'system'),
        nullif(current_setting('app.current_actor', true), '')::uuid
    );
    RETURN NULL;
END;
$$;

CREATE TRIGGER trg_deployments__initial_state
    AFTER INSERT ON deployments
    FOR EACH ROW EXECUTE FUNCTION nebula_deployment_initial_state();

-- gen_random_uuid() lives in pgcrypto before PostgreSQL 13 and in core from 13
-- onwards. NEBULA requires 16, so no extension is needed; the triggers use it
-- only because a trigger cannot ask the application for a UUIDv7.
-- TODO(NEB-121): pass an application-generated UUIDv7 through a session variable
-- so transition rows share the time-ordered key property of every other table.

-- ---------------------------------------------------------------------------
-- 5. Row-level security for the new table
-- ---------------------------------------------------------------------------
ALTER TABLE deployment_state_transitions ENABLE ROW LEVEL SECURITY;

CREATE POLICY org_isolation ON deployment_state_transitions FOR ALL TO nebula_app
    USING (org_id = nebula_current_org()) WITH CHECK (org_id = nebula_current_org());

CREATE POLICY controller_access ON deployment_state_transitions FOR ALL TO nebula_controller
    USING (true) WITH CHECK (true);

CREATE POLICY readonly_access ON deployment_state_transitions FOR SELECT TO nebula_readonly
    USING (true);

-- The edge table is reference data: readable by everyone, writable by nobody at
-- the application level.
GRANT SELECT ON deployment_state_edges TO nebula_app, nebula_controller, nebula_readonly;
GRANT SELECT, INSERT ON deployment_state_transitions TO nebula_app, nebula_controller;

-- The controller holds column-level UPDATE grants on deployments (000007), which
-- is axiom A1 expressed as a permission: it owns the OBSERVED columns and may not
-- rewrite a user's spec. The two new explanatory columns belong to it for the same
-- reason it owns `state` — it is the thing that knows why a state changed — so
-- they are added to that grant here rather than left out and discovered missing in
-- Phase 5 as a permission denied at 3 a.m.
--
-- state_entered_at is deliberately NOT granted: it is stamped by the trigger from
-- now(), and no role may set it by hand. PostgreSQL checks column privileges
-- against the statement's SET list rather than what a BEFORE trigger assigns, so
-- withholding the grant makes the timestamp unforgeable while leaving the trigger
-- free to maintain it.
GRANT UPDATE (state_reason, state_message) ON deployments TO nebula_controller;
