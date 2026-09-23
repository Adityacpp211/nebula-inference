# NEBULA — Data Model

**Status:** Phase 0 design. DDL below is the design contract; the authoritative artifacts will be
the migration files under `migrations/` created in Phase 1.

PostgreSQL 16+. Accessed from Go via `pgx` with `sqlc`-generated query code. Migrations are applied by
NEBULA's own runner in `packages/db/migrate`, forward-only in CI, with a reversible `down` required for
every migration so local development can rewind. The runner is in-repo rather than a library so that it
can verify the checksum of already-applied migrations
([ADR-0025](./architecture-decisions/0025-own-the-migration-runner.md)).

---

## 1. Conventions

| Convention | Decision | Reason |
|------------|----------|--------|
| Primary keys | `uuid` holding **UUIDv7**, generated in the application | Time-ordered, so B-tree inserts stay at the right edge of the index — important for the high-volume `requests` and `audit_logs` tables. Avoids the index fragmentation of UUIDv4 and the cross-tenant enumeration of bigserial. See [ADR-0023](./architecture-decisions/README.md#adr-0023). |
| Timestamps | `timestamptz`, UTC, `created_at`/`updated_at` on every mutable table | No naive timestamps anywhere. |
| Soft delete | `deleted_at timestamptz` only where history matters (models, deployments, keys); hard delete elsewhere | Audit and cost reconstruction need the row to survive. |
| Money | `bigint` micros of the profile's currency | No floats for money, ever. |
| Sizes | `bigint` bytes; memory in MiB as `integer` | Explicit units in the column name (`size_bytes`, `vram_mib`). |
| Enums | PostgreSQL `enum` types for closed, slow-changing sets; `text` + `CHECK` for sets likely to grow | Enums give database-level validation; adding a value is a migration, which is the point. |
| Flexible config | `jsonb` with a documented shape and a Go struct + JSON Schema validated at the API boundary | Config shapes change per runtime; a wide sparse table would be worse. Never used for anything queried in a hot path without a GIN or expression index. |
| Tenancy | `org_id` on every tenant-owned table + row-level security | Defence in depth; see §7. |
| Naming | snake_case, plural tables, `<table>_id` foreign keys, indexes `ix_<table>__<cols>`, uniques `uq_<table>__<cols>`, checks `ck_<table>__<rule>` | Predictable, greppable. |

### Derived vs authoritative

A recurring source of bugs in control planes is persisting state that something else owns. Every
table is labelled:

- **AUTHORITATIVE** — PostgreSQL is the source of truth. Users write it; controllers read it.
- **OBSERVED** — written by a controller from Kubernetes/telemetry. Carries `synced_at`. Safe to
  truncate; it will be rebuilt. Never used to make a decision that Kubernetes could answer directly.
- **APPEND-ONLY** — historical fact. No UPDATE, no DELETE (enforced by role grants).

---

## 2. Entity map

```
organizations ─┬─ users ────────────┬─ api_keys
               │                    │
               ├─ models ── model_versions ── model_artifacts
               │                │
               │                └────────────┐
               ├─ routes ── route_targets ───┤
               │     │                       │
               │     ├─ canary_rollouts      │
               │     └─ experiments          │
               │                             │
               ├─ deployments ───────────────┘   (deployments pin ONE model_version)
               │     ├─ deployment_revisions  (immutable spec snapshots)
               │     ├─ autoscaling_events
               │     └─ worker_events
               │
               ├─ routing_policies
               ├─ rate_limit_policies
               ├─ requests (partitioned) ── usage_records (hourly rollup)
               ├─ audit_logs (partitioned, append-only)
               └─ pricing_profiles (immutable, versioned)

nodes            (OBSERVED cache of Kubernetes Node objects — not org-scoped)
schema_migrations
```

---

## 3. Identity and tenancy

### organizations — AUTHORITATIVE

```sql
CREATE TABLE organizations (
    id                  uuid PRIMARY KEY,
    slug                text NOT NULL,
    name                text NOT NULL,
    pricing_profile_id  uuid REFERENCES pricing_profiles(id),
    default_namespace   text NOT NULL DEFAULT 'nebula-workloads',
    quota               jsonb NOT NULL DEFAULT '{}'::jsonb,  -- max_deployments, max_replicas, max_gpu
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    deleted_at          timestamptz,
    CONSTRAINT uq_organizations__slug UNIQUE (slug),
    CONSTRAINT ck_organizations__slug_format CHECK (slug ~ '^[a-z0-9]([a-z0-9-]{0,38}[a-z0-9])?$')
);
```

The slug check is the DNS-label rule, because the slug appears in Kubernetes object names.

### users — AUTHORITATIVE

```sql
CREATE TYPE user_role AS ENUM ('owner', 'admin', 'developer', 'viewer');

CREATE TABLE users (
    id             uuid PRIMARY KEY,
    org_id         uuid NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
    email          text NOT NULL,
    name           text,
    role           user_role NOT NULL DEFAULT 'developer',
    password_hash  text,            -- argon2id; NULL when the user is OIDC-only
    last_login_at  timestamptz,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    deleted_at     timestamptz,
    CONSTRAINT uq_users__email UNIQUE (email)
);
```

Roles: `owner` (billing + org deletion), `admin` (all resources, keys), `developer` (deploy, scale,
rollback; no key management for other users), `viewer` (read + metrics). Enforced in the control
plane and mirrored into API-key scopes.

### api_keys — AUTHORITATIVE

```sql
CREATE TABLE api_keys (
    id            uuid PRIMARY KEY,
    org_id        uuid NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    user_id       uuid REFERENCES users(id) ON DELETE SET NULL,
    name          text NOT NULL,
    prefix        char(11) NOT NULL,        -- 'nbk_' + 7 chars, safe to display and log
    key_hash      bytea NOT NULL,           -- HMAC-SHA256(pepper, full_key)
    scopes        text[] NOT NULL DEFAULT ARRAY['inference:invoke'],
    priority      text NOT NULL DEFAULT 'NORMAL',
    rate_limit_policy_id uuid REFERENCES rate_limit_policies(id),
    last_used_at  timestamptz,
    expires_at    timestamptz,
    revoked_at    timestamptz,
    created_at    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_api_keys__prefix UNIQUE (prefix),
    CONSTRAINT ck_api_keys__priority CHECK (priority IN ('LOW','NORMAL','HIGH'))
);
CREATE INDEX ix_api_keys__org_active ON api_keys (org_id) WHERE revoked_at IS NULL;
```

Lookup is by `prefix` (unique, indexed), then constant-time compare of the HMAC. The full key is
returned exactly once, at creation, and never stored. `last_used_at` is updated asynchronously
(batched, at most once per key per minute) so authentication never blocks on a write.

Request **priority** lives on the key, not in the request body — callers cannot promote themselves.

---

## 4. Model registry

### models — AUTHORITATIVE

```sql
CREATE TABLE models (
    id          uuid PRIMARY KEY,
    org_id      uuid NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    name        text NOT NULL,                 -- 'qwen2.5'
    family      text,                          -- 'qwen2'
    task        text NOT NULL DEFAULT 'chat',  -- chat | completion | embedding
    description text,
    created_by  uuid REFERENCES users(id),
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    deleted_at  timestamptz,
    CONSTRAINT uq_models__org_name UNIQUE (org_id, name),
    CONSTRAINT ck_models__name_format CHECK (name ~ '^[a-z0-9]([a-z0-9._-]{0,62}[a-z0-9])?$')
);
```

### model_versions — AUTHORITATIVE, immutable once `ready`

```sql
CREATE TYPE model_version_status AS ENUM
    ('uploading','verifying','ready','failed','archived');

CREATE TABLE model_versions (
    id               uuid PRIMARY KEY,
    model_id         uuid NOT NULL REFERENCES models(id) ON DELETE RESTRICT,
    version          text NOT NULL,             -- '7b-q4_k_m', '0.5b-q4'
    format           text NOT NULL,             -- gguf | safetensors | mock
    runtime          text NOT NULL,             -- llamacpp | vllm | mock
    quantization     text,                      -- q4_k_m, awq, fp16, NULL
    parameter_count  bigint,
    size_bytes       bigint NOT NULL,
    checksum_sha256  bytea NOT NULL,
    context_window   integer NOT NULL,
    artifact_uri     text NOT NULL,             -- s3://nebula-models/sha256/<hex>
    hardware_profile jsonb NOT NULL,            -- see shape below
    runtime_config   jsonb NOT NULL DEFAULT '{}'::jsonb,
    status           model_version_status NOT NULL DEFAULT 'uploading',
    failure_reason   text,
    created_by       uuid REFERENCES users(id),
    created_at       timestamptz NOT NULL DEFAULT now(),
    ready_at         timestamptz,
    CONSTRAINT uq_model_versions__model_version UNIQUE (model_id, version),
    CONSTRAINT ck_model_versions__context_window CHECK (context_window > 0),
    CONSTRAINT ck_model_versions__ready_fields
        CHECK (status <> 'ready' OR (ready_at IS NOT NULL AND checksum_sha256 IS NOT NULL))
);
CREATE INDEX ix_model_versions__checksum ON model_versions (checksum_sha256);
```

`hardware_profile` shape (validated by JSON Schema at the API boundary):

```json
{
  "requires_gpu": false,
  "min_vram_mib": 0,
  "min_ram_mib": 2048,
  "min_cpu_milli": 2000,
  "recommended_cpu_milli": 4000,
  "gpu_compute_capability_min": null,
  "supported_accelerators": ["cpu"],
  "parallel_slots": 4
}
```

**Immutability** is enforced in the database, not only in code:

```sql
CREATE FUNCTION forbid_ready_model_version_mutation() RETURNS trigger AS $$
BEGIN
  IF OLD.status = 'ready' AND (
       NEW.checksum_sha256  <> OLD.checksum_sha256  OR
       NEW.artifact_uri     <> OLD.artifact_uri     OR
       NEW.format           <> OLD.format           OR
       NEW.runtime          <> OLD.runtime          OR
       NEW.quantization IS DISTINCT FROM OLD.quantization OR
       NEW.context_window   <> OLD.context_window   OR
       NEW.size_bytes       <> OLD.size_bytes       OR
       NEW.hardware_profile <> OLD.hardware_profile
     ) THEN
    RAISE EXCEPTION 'model_version % is ready and immutable', OLD.id
      USING ERRCODE = 'restrict_violation';
  END IF;
  RETURN NEW;
END $$ LANGUAGE plpgsql;

CREATE TRIGGER trg_model_versions__immutable
  BEFORE UPDATE ON model_versions
  FOR EACH ROW EXECUTE FUNCTION forbid_ready_model_version_mutation();
```

Only `status` (→ `archived`) and `failure_reason` may change after `ready`. A new artifact means a
new row, always.

### model_artifacts — AUTHORITATIVE

Multi-file models (sharded safetensors, tokenizer, config) need per-file integrity, so the artifact
is a set, not a blob.

```sql
CREATE TABLE model_artifacts (
    id               uuid PRIMARY KEY,
    model_version_id uuid NOT NULL REFERENCES model_versions(id) ON DELETE CASCADE,
    path             text NOT NULL,            -- 'model-00001-of-00002.safetensors'
    size_bytes       bigint NOT NULL,
    checksum_sha256  bytea NOT NULL,
    CONSTRAINT uq_model_artifacts__version_path UNIQUE (model_version_id, path)
);
```

---

## 5. Deployments and routing

### deployments — AUTHORITATIVE (desired state)

```sql
CREATE TYPE deployment_state AS ENUM
    ('pending','progressing','ready','degraded','failed','deleting','deleted');

CREATE TABLE deployments (
    id                 uuid PRIMARY KEY,
    org_id             uuid NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    model_version_id   uuid NOT NULL REFERENCES model_versions(id) ON DELETE RESTRICT,
    name               text NOT NULL,
    namespace          text NOT NULL,
    desired_replicas   integer NOT NULL DEFAULT 1,
    min_replicas       integer NOT NULL DEFAULT 1,
    max_replicas       integer NOT NULL DEFAULT 5,
    resources          jsonb NOT NULL,   -- {cpu_milli, memory_mib, gpu_count, gpu_type}
    runtime_overrides  jsonb NOT NULL DEFAULT '{}'::jsonb,
    autoscaling        jsonb NOT NULL DEFAULT '{}'::jsonb,  -- targets, windows, enabled
    queue_config       jsonb NOT NULL DEFAULT '{}'::jsonb,  -- max_depth, default_timeout_ms
    routing_policy_id  uuid REFERENCES routing_policies(id),
    -- desired-state bookkeeping
    generation         bigint NOT NULL DEFAULT 1,
    current_revision   integer NOT NULL DEFAULT 1,
    -- OBSERVED columns, written only by the controller
    observed_generation bigint NOT NULL DEFAULT 0,
    state              deployment_state NOT NULL DEFAULT 'pending',
    ready_replicas     integer NOT NULL DEFAULT 0,
    updated_replicas   integer NOT NULL DEFAULT 0,
    conditions         jsonb NOT NULL DEFAULT '[]'::jsonb,
    last_error         text,
    last_synced_at     timestamptz,
    created_by         uuid REFERENCES users(id),
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now(),
    deleted_at         timestamptz,
    CONSTRAINT uq_deployments__org_name UNIQUE (org_id, name),
    CONSTRAINT ck_deployments__replica_bounds
        CHECK (min_replicas >= 0 AND max_replicas >= min_replicas
               AND desired_replicas BETWEEN min_replicas AND max_replicas)
);
CREATE INDEX ix_deployments__needs_reconcile
    ON deployments (updated_at) WHERE generation <> observed_generation;
```

`generation` increments on every *spec* change and never on a status write; that single rule is what
makes `generation <> observed_generation` a trustworthy "work to do" predicate. The OBSERVED columns
live on this table rather than in a separate one purely for read convenience — they are still
controller-owned, and a grant separates the writers.

**Live per-pod state is deliberately absent.** Pod names, IPs, and readiness are Kubernetes' truth,
cached in Redis for the router. Persisting them here would create two sources of truth for the same
fact (axiom A1). What *is* persisted is history: `worker_events`.

### deployment_revisions — APPEND-ONLY

```sql
CREATE TABLE deployment_revisions (
    id               uuid PRIMARY KEY,
    deployment_id    uuid NOT NULL REFERENCES deployments(id) ON DELETE CASCADE,
    revision         integer NOT NULL,
    model_version_id uuid NOT NULL REFERENCES model_versions(id) ON DELETE RESTRICT,
    spec             jsonb NOT NULL,     -- complete snapshot: resources, runtime, queue, autoscale
    spec_hash        bytea NOT NULL,     -- sha256 of canonicalized spec; dedupes no-op updates
    reason           text NOT NULL,      -- create | update | scale | rollback | rollout
    created_by       uuid REFERENCES users(id),
    created_at       timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_deployment_revisions__dep_rev UNIQUE (deployment_id, revision)
);
```

Rollback = find the target revision, write a **new** revision whose spec equals the old one (reason
`rollback`), bump `generation`. History is never rewritten. This is the same model Kubernetes uses
for ReplicaSet history, and it means `nebula rollback` is a normal write, not a special path.

### routes and route_targets — AUTHORITATIVE

The keystone of progressive delivery: the public model name is a **route**, not a deployment.

```sql
CREATE TABLE routes (
    id                uuid PRIMARY KEY,
    org_id            uuid NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    model_name        text NOT NULL,     -- what clients put in {"model": "..."}
    routing_policy_id uuid REFERENCES routing_policies(id),
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_routes__org_model_name UNIQUE (org_id, model_name)
);

CREATE TABLE route_targets (
    id            uuid PRIMARY KEY,
    route_id      uuid NOT NULL REFERENCES routes(id) ON DELETE CASCADE,
    deployment_id uuid NOT NULL REFERENCES deployments(id) ON DELETE RESTRICT,
    weight        integer NOT NULL,
    is_baseline   boolean NOT NULL DEFAULT false,
    label         text,                  -- 'baseline' | 'canary' | 'variant-b'
    updated_at    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_route_targets__route_dep UNIQUE (route_id, deployment_id),
    CONSTRAINT ck_route_targets__weight CHECK (weight BETWEEN 0 AND 100)
);
```

Weights must total 100 per route. Enforced in a transaction by the control plane and verified by a
deferred constraint trigger, because a route that sums to 90 would silently drop 10% of traffic —
exactly the kind of bug that must be impossible rather than caught in review.

### routing_policies / rate_limit_policies — AUTHORITATIVE

```sql
CREATE TABLE routing_policies (
    id         uuid PRIMARY KEY,
    org_id     uuid REFERENCES organizations(id) ON DELETE CASCADE,  -- NULL = built-in default
    name       text NOT NULL,
    strategy   text NOT NULL,   -- round_robin|least_loaded|latency_aware|cost_aware|capability|failover
    config     jsonb NOT NULL DEFAULT '{}'::jsonb,
    is_default boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_routing_policies__org_name UNIQUE (org_id, name)
);

CREATE TABLE rate_limit_policies (
    id                    uuid PRIMARY KEY,
    org_id                uuid REFERENCES organizations(id) ON DELETE CASCADE,
    name                  text NOT NULL,
    requests_per_minute   integer,
    tokens_per_minute     integer,
    max_concurrency       integer,
    max_queue_depth       integer,
    created_at            timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_rate_limit_policies__org_name UNIQUE (org_id, name)
);
```

`strategy` is `text` + application-side registry rather than an enum: strategies are plugins, and a
new one should not require a migration.

---

## 6. Progressive delivery

### canary_rollouts — AUTHORITATIVE (state machine)

```sql
CREATE TYPE rollout_state AS ENUM
    ('pending','progressing','paused','promoted','aborted','rolled_back','failed');

CREATE TABLE canary_rollouts (
    id                   uuid PRIMARY KEY,
    org_id               uuid NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    route_id             uuid NOT NULL REFERENCES routes(id) ON DELETE CASCADE,
    baseline_deployment_id uuid NOT NULL REFERENCES deployments(id) ON DELETE RESTRICT,
    canary_deployment_id   uuid NOT NULL REFERENCES deployments(id) ON DELETE RESTRICT,
    steps                jsonb NOT NULL,   -- [{"weight":10,"hold_seconds":300}, ...]
    current_step         integer NOT NULL DEFAULT 0,
    analysis             jsonb NOT NULL,   -- thresholds + min_requests + on_failure
    state                rollout_state NOT NULL DEFAULT 'pending',
    last_analysis        jsonb,            -- observed values per arm, per step
    abort_reason         text,
    created_by           uuid REFERENCES users(id),
    started_at           timestamptz,
    step_entered_at      timestamptz,
    finished_at          timestamptz,
    created_at           timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_canary_rollouts__distinct_arms
        CHECK (baseline_deployment_id <> canary_deployment_id)
);
CREATE UNIQUE INDEX uq_canary_rollouts__one_active_per_route
    ON canary_rollouts (route_id)
    WHERE state IN ('pending','progressing','paused');
```

The partial unique index is the interlock that makes concurrent rollouts on one route impossible at
the database level rather than by hoping the API checks.

`analysis` shape:

```json
{
  "max_error_rate_delta_pp": 1.0,
  "max_p95_latency_ratio": 1.2,
  "min_requests": 200,
  "on_insufficient_data": "extend",   // extend | pause | fail
  "on_failure": "rollback"            // rollback | pause
}
```

### experiments — AUTHORITATIVE

```sql
CREATE TYPE experiment_state AS ENUM ('draft','running','stopped','concluded');

CREATE TABLE experiments (
    id           uuid PRIMARY KEY,
    org_id       uuid NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    route_id     uuid NOT NULL REFERENCES routes(id) ON DELETE CASCADE,
    name         text NOT NULL,
    hypothesis   text,
    bucketing    text NOT NULL DEFAULT 'request',  -- request | sticky_user | sticky_org
    state        experiment_state NOT NULL DEFAULT 'draft',
    started_at   timestamptz,
    stopped_at   timestamptz,
    conclusion   text,
    created_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_experiments__org_name UNIQUE (org_id, name)
);
```

Variant assignment is **computed, not stored**: `variant = weighted_pick(hash(bucketing_key), route_targets)`.
An assignment table would be a per-request write on the hot path for information that is
deterministically derivable. The chosen variant is recorded once, on the `requests` row, which is
where it is actually needed for analysis.

---

## 7. Telemetry, usage, cost

### requests — APPEND-ONLY, partitioned, sampled

High-volume, retention-bounded, written asynchronously by the usage ingester.

```sql
CREATE TABLE requests (
    id               uuid NOT NULL,           -- == X-Request-Id
    org_id           uuid NOT NULL,
    created_at       timestamptz NOT NULL,
    route_id         uuid,
    deployment_id    uuid,
    model_version_id uuid,
    api_key_id       uuid,
    experiment_id    uuid,
    variant          text,
    priority         text NOT NULL,
    status_code      smallint NOT NULL,
    error_class      text,                    -- NULL on success
    streamed         boolean NOT NULL,
    queue_wait_ms    integer NOT NULL DEFAULT 0,
    ttft_ms          integer,
    duration_ms      integer NOT NULL,
    attempts         smallint NOT NULL DEFAULT 1,
    prompt_tokens    integer NOT NULL DEFAULT 0,
    completion_tokens integer NOT NULL DEFAULT 0,
    compute_ms       integer NOT NULL DEFAULT 0,
    worker_pod       text,
    node_name        text,
    trace_id         text,
    estimated_cost_micros bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);
```

Decisions worth stating:

- **Monthly partitions**, created ahead of time by a maintenance job; old partitions detached and
  dropped per `REQUEST_RETENTION_DAYS` (default 30). A table that grows forever is an outage with a
  delay fuse.
- **No foreign keys.** Deliberate: FK checks on a high-rate insert path cost throughput and would
  block dropping a parent row. Referential integrity for analytics is accepted as eventual, and the
  columns are documented as historical snapshots (a deployment may later be deleted; its request
  rows remain).
- **Sampling** via `REQUEST_SAMPLE_RATE` (default 1.0 in dev, tunable in production). Errors are
  **always** recorded regardless of sample rate. The sample rate is stored alongside the rollup so
  counts can be corrected rather than quietly understated.
- **Batched inserts** using `COPY` from the JetStream consumer, idempotent on `(id, created_at)`.
- Indexes: `(org_id, created_at DESC)`, `(deployment_id, created_at DESC)`,
  `(created_at) WHERE error_class IS NOT NULL`, `(trace_id)`. Per partition.

### usage_records — APPEND-ONLY hourly rollup

This, not `requests`, is what billing and the cost pages read. It is retained indefinitely.

```sql
CREATE TABLE usage_records (
    id                  uuid PRIMARY KEY,
    org_id              uuid NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
    bucket_hour         timestamptz NOT NULL,
    deployment_id       uuid,
    model_version_id    uuid,
    api_key_id          uuid,
    request_count       bigint NOT NULL,
    error_count         bigint NOT NULL,
    prompt_tokens       bigint NOT NULL,
    completion_tokens   bigint NOT NULL,
    compute_seconds     numeric(14,3) NOT NULL,
    gpu_seconds         numeric(14,3) NOT NULL DEFAULT 0,
    sample_rate         real NOT NULL DEFAULT 1.0,
    pricing_profile_id  uuid NOT NULL REFERENCES pricing_profiles(id),
    estimated_cost_micros bigint NOT NULL,
    computed_at         timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_usage_records__bucket
        UNIQUE (org_id, bucket_hour, deployment_id, api_key_id)
);
CREATE INDEX ix_usage_records__org_bucket ON usage_records (org_id, bucket_hour DESC);
```

The unique constraint makes the rollup job idempotent (`ON CONFLICT DO UPDATE`), which matters
because at-least-once delivery guarantees it will run twice eventually.

### pricing_profiles — IMMUTABLE, versioned

```sql
CREATE TABLE pricing_profiles (
    id                     uuid PRIMARY KEY,
    org_id                 uuid REFERENCES organizations(id) ON DELETE CASCADE, -- NULL = global
    name                   text NOT NULL,
    version                integer NOT NULL,
    currency               char(3) NOT NULL DEFAULT 'USD',
    cpu_core_hour_micros   bigint NOT NULL,
    memory_gib_hour_micros bigint NOT NULL,
    gpu_hour_micros        jsonb NOT NULL DEFAULT '{}'::jsonb,  -- {"nvidia-a100-40gb": 1230000}
    storage_gib_month_micros bigint NOT NULL DEFAULT 0,
    egress_gib_micros      bigint NOT NULL DEFAULT 0,
    markup_basis_points    integer NOT NULL DEFAULT 0,
    effective_from         timestamptz NOT NULL,
    created_at             timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_pricing_profiles__name_version UNIQUE (org_id, name, version)
);
```

A price change is a new `version` row with a new `effective_from`, never an UPDATE. Every
`usage_records` row names the profile it used, so any historical cost figure can be recomputed and
audited. Enforced by a trigger that rejects all UPDATEs.

---

## 8. Observed state and history

### nodes — OBSERVED

A cache of Kubernetes `Node` objects for the dashboard, capacity admission, and historical capacity
questions. Never consulted for a decision the live informer can answer.

```sql
CREATE TABLE nodes (
    id                uuid PRIMARY KEY,
    name              text NOT NULL,
    provider_id       text,
    labels            jsonb NOT NULL DEFAULT '{}'::jsonb,
    taints            jsonb NOT NULL DEFAULT '[]'::jsonb,
    capacity          jsonb NOT NULL,      -- {cpu_milli, memory_mib, gpu_count, gpu_model, gpu_memory_mib}
    allocatable       jsonb NOT NULL,
    conditions        jsonb NOT NULL DEFAULT '[]'::jsonb,
    schedulable       boolean NOT NULL DEFAULT true,
    kubelet_version   text,
    first_seen_at     timestamptz NOT NULL DEFAULT now(),
    synced_at         timestamptz NOT NULL DEFAULT now(),
    removed_at        timestamptz,
    CONSTRAINT uq_nodes__name UNIQUE (name)
);
```

### worker_events — APPEND-ONLY

```sql
CREATE TABLE worker_events (
    id            uuid PRIMARY KEY,
    org_id        uuid NOT NULL,
    deployment_id uuid NOT NULL,
    pod_name      text NOT NULL,
    node_name     text,
    event_type    text NOT NULL,   -- scheduled|pulling|model_loading|ready|unhealthy|
                                   -- crashed|oom_killed|evicted|model_load_failed|drained|terminated
    detail        jsonb NOT NULL DEFAULT '{}'::jsonb,
    occurred_at   timestamptz NOT NULL,
    CONSTRAINT ck_worker_events__type CHECK (event_type <> '')
);
CREATE INDEX ix_worker_events__dep_time ON worker_events (deployment_id, occurred_at DESC);
```

This is what makes "show me why this deployment is degraded" answerable a day later, when the pod
and its Kubernetes Events are long gone (Kubernetes Events expire in an hour by default).

### autoscaling_events — APPEND-ONLY

```sql
CREATE TABLE autoscaling_events (
    id            uuid PRIMARY KEY,
    org_id        uuid NOT NULL,
    deployment_id uuid NOT NULL REFERENCES deployments(id) ON DELETE CASCADE,
    from_replicas integer NOT NULL,
    to_replicas   integer NOT NULL,
    direction     text NOT NULL,     -- up | down | hold
    signals       jsonb NOT NULL,    -- full vector: observed, target, ratio per signal
    decision      text NOT NULL,     -- scaled | suppressed_cooldown | suppressed_stabilization
                                     -- | clamped_max | clamped_min | suppressed_rollout
    reason        text NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now()
);
```

Recording *suppressed* decisions, not just changes, is what turns "why didn't it scale?" from a
guess into a query.

### audit_logs — APPEND-ONLY, partitioned

```sql
CREATE TABLE audit_logs (
    id            uuid NOT NULL,
    org_id        uuid NOT NULL,
    created_at    timestamptz NOT NULL,
    actor_type    text NOT NULL,     -- user | api_key | system
    actor_id      uuid,
    actor_label   text,              -- email or key prefix, denormalized for readability
    action        text NOT NULL,     -- model_version.created, deployment.scaled, rollout.aborted…
    resource_type text NOT NULL,
    resource_id   uuid,
    before        jsonb,
    after         jsonb,
    request_id    uuid,
    ip            inet,
    user_agent    text,
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);
```

Append-only is enforced by grants: the application role receives `INSERT, SELECT` only. `before`/
`after` are redacted of secret material by a shared helper, never by each call site.

---

## 9. Row-level security

```sql
ALTER TABLE deployments ENABLE ROW LEVEL SECURITY;
CREATE POLICY org_isolation ON deployments
  USING (org_id = current_setting('app.current_org', true)::uuid);
```

Applied to every org-scoped table. The application sets `app.current_org` with `SET LOCAL` inside
each request transaction; a background job uses a separate role that bypasses RLS explicitly and
visibly. This is defence in depth: a missing `WHERE org_id = $1` becomes an empty result instead of
a cross-tenant leak. Cost is one `SET LOCAL` per transaction, which is negligible next to what it
prevents.

Three database roles:

| Role | Grants |
|------|--------|
| `nebula_app` | CRUD on authoritative tables, INSERT+SELECT on append-only tables, RLS enforced. Used by the control plane. |
| `nebula_controller` | UPDATE on OBSERVED columns, INSERT on event tables, SELECT elsewhere. RLS bypass (it works across orgs by design). |
| `nebula_readonly` | SELECT only. Used by analytics and by anything that should never be able to write. |

---

## 10. Migration policy

- Every schema change is a numbered pair (`000007_add_route_targets.up.sql` / `.down.sql`).
  Hand-written; no auto-generated diffs. Both halves are **mandatory**: the runner refuses to load a
  migration set where any version is missing one.
- Applied migrations are checksummed. Editing a migration that has already run is detected on the next
  run instead of silently diverging environments ([ADR-0025](./architecture-decisions/0025-own-the-migration-runner.md)).
- **Expand/contract** for anything touching a live column: add nullable → backfill in batches →
  start writing both → switch reads → drop old, in separate releases. No single migration both adds
  a NOT NULL column and backfills it.
- `CREATE INDEX CONCURRENTLY` for any table that could be large, which means the migration runner
  must run those outside a transaction — handled explicitly with the `-- nebula:no-transaction`
  directive, not discovered in production.
- Migrations run as a Kubernetes `Job` (Helm pre-install/pre-upgrade hook) that must complete before
  new application pods roll. Application code asserts the expected schema version at startup and
  refuses to serve on mismatch.
- CI verifies: `up` from empty, `down` to empty, `up` again; plus migration-on-a-seeded-snapshot to
  catch backfill errors.

---

## 11. Seed data

Shipped as a migration, because the system is unusable without it: the built-in routing policies
(`round_robin`, `least_loaded`, `latency_aware`, `cost_aware`, `capability`, `failover`), a default
rate-limit policy, a global `pricing_profiles` row (version 1, all prices zero, so no cost is ever
silently fabricated before an operator configures real numbers), and — in development only, gated
behind `NEBULA_DEV_SEED=true` — a `dev` organization, a `dev@nebula.local` owner, one API key
printed to the log, and a registered `mock` model version so the full flow is exercisable before any
real weights exist.
