-- 000005 progressive delivery: canary rollouts and experiments
--
-- Both ride on the same weighting machinery in route_targets. A rollout has
-- promotion intent and an analysis gate; an experiment has fixed weights and a
-- comparison read model. Neither needs its own traffic mechanism.

CREATE TABLE canary_rollouts (
    id                     uuid          PRIMARY KEY,
    org_id                 uuid          NOT NULL REFERENCES organizations (id) ON DELETE CASCADE,
    route_id               uuid          NOT NULL REFERENCES routes (id) ON DELETE CASCADE,
    baseline_deployment_id uuid          NOT NULL REFERENCES deployments (id) ON DELETE RESTRICT,
    canary_deployment_id   uuid          NOT NULL REFERENCES deployments (id) ON DELETE RESTRICT,
    steps                  jsonb         NOT NULL,
    current_step           integer       NOT NULL DEFAULT 0,
    analysis               jsonb         NOT NULL,
    state                  rollout_state NOT NULL DEFAULT 'pending',
    last_analysis          jsonb,
    abort_reason           text,
    created_by             uuid          REFERENCES users (id) ON DELETE SET NULL,
    started_at             timestamptz,
    step_entered_at        timestamptz,
    finished_at            timestamptz,
    created_at             timestamptz   NOT NULL DEFAULT now(),

    CONSTRAINT ck_canary_rollouts__distinct_arms CHECK (baseline_deployment_id <> canary_deployment_id),
    CONSTRAINT ck_canary_rollouts__steps_array CHECK (jsonb_typeof(steps) = 'array' AND jsonb_array_length(steps) > 0),
    CONSTRAINT ck_canary_rollouts__analysis_object CHECK (jsonb_typeof(analysis) = 'object'),
    CONSTRAINT ck_canary_rollouts__current_step CHECK (current_step >= 0),
    CONSTRAINT ck_canary_rollouts__abort_reason CHECK (
        state NOT IN ('aborted', 'rolled_back', 'failed') OR abort_reason IS NOT NULL
    ),
    CONSTRAINT ck_canary_rollouts__finished CHECK (
        state NOT IN ('promoted', 'aborted', 'rolled_back', 'failed') OR finished_at IS NOT NULL
    )
);

CREATE INDEX ix_canary_rollouts__route ON canary_rollouts (route_id, created_at DESC);
CREATE INDEX ix_canary_rollouts__org_state ON canary_rollouts (org_id, state);

-- At most one rollout in flight per route. A partial unique index makes
-- concurrent rollouts on one route impossible at the database level rather than
-- relying on the API to check first, which is racy by construction.
CREATE UNIQUE INDEX uq_canary_rollouts__one_active_per_route
    ON canary_rollouts (route_id)
    WHERE state IN ('pending', 'progressing', 'paused');

CREATE TABLE experiments (
    id         uuid             PRIMARY KEY,
    org_id     uuid             NOT NULL REFERENCES organizations (id) ON DELETE CASCADE,
    route_id   uuid             NOT NULL REFERENCES routes (id) ON DELETE CASCADE,
    name       text             NOT NULL,
    hypothesis text,
    -- Variant assignment is COMPUTED from a hash of the bucketing key, never
    -- stored: an assignment table would be a per-request write on the hot path
    -- for something deterministically derivable. The chosen variant is recorded
    -- once on the requests row, which is where analysis needs it.
    bucketing  text             NOT NULL DEFAULT 'request',
    state      experiment_state NOT NULL DEFAULT 'draft',
    started_at timestamptz,
    stopped_at timestamptz,
    conclusion text,
    created_at timestamptz      NOT NULL DEFAULT now(),

    CONSTRAINT uq_experiments__org_name UNIQUE (org_id, name),
    CONSTRAINT ck_experiments__bucketing CHECK (bucketing IN ('request', 'sticky_user', 'sticky_org')),
    CONSTRAINT ck_experiments__started CHECK (state <> 'running' OR started_at IS NOT NULL)
);

CREATE INDEX ix_experiments__route ON experiments (route_id);

-- One running experiment per route: two overlapping traffic splits on the same
-- route would make both sets of results uninterpretable.
CREATE UNIQUE INDEX uq_experiments__one_running_per_route
    ON experiments (route_id) WHERE state = 'running';
