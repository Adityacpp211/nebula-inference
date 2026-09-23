-- 000009 down

DROP TRIGGER IF EXISTS trg_deployments__initial_state ON deployments;
DROP FUNCTION IF EXISTS nebula_deployment_initial_state();
DROP TRIGGER IF EXISTS trg_deployments__state_transition ON deployments;
DROP FUNCTION IF EXISTS nebula_deployment_state_transition();

DROP TABLE IF EXISTS deployment_state_edges;
DROP TABLE IF EXISTS deployment_state_transitions;

-- The grant is revoked before the columns go: dropping a column removes its
-- privileges with it, so the order matters only for the reverse case, and being
-- explicit here keeps pg_attribute free of the entry either way.
REVOKE UPDATE (state_reason, state_message) ON deployments FROM nebula_controller;

ALTER TABLE deployments DROP CONSTRAINT IF EXISTS ck_deployments__delete_only_when_resting;
ALTER TABLE deployments DROP COLUMN IF EXISTS state_message;
ALTER TABLE deployments DROP COLUMN IF EXISTS state_reason;
ALTER TABLE deployments DROP COLUMN IF EXISTS state_entered_at;

-- Restore the Phase 1 enum. The mapping is lossy in one direction: provisioning
-- and starting both came from progressing, and both go back to it.
ALTER TYPE deployment_state RENAME TO deployment_state_v2;

CREATE TYPE deployment_state AS ENUM
    ('pending', 'progressing', 'ready', 'degraded', 'failed', 'deleting', 'deleted');

ALTER TABLE deployments ALTER COLUMN state DROP DEFAULT;

ALTER TABLE deployments
    ALTER COLUMN state TYPE deployment_state
    USING (
        CASE state::text
            WHEN 'provisioning' THEN 'progressing'
            WHEN 'starting'     THEN 'progressing'
            WHEN 'stopping'     THEN 'deleting'
            WHEN 'stopped'      THEN 'deleted'
            ELSE state::text
        END
    )::deployment_state;

ALTER TABLE deployments ALTER COLUMN state SET DEFAULT 'pending';

DROP TYPE deployment_state_v2;
