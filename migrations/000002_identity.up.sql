-- 000002 identity: tenants, users, API keys, and the policy tables they reference
--
-- Creation order is forced by a circular reference: organizations name a default
-- pricing profile, and a pricing profile may belong to an organization. The cycle
-- is broken by creating pricing_profiles first without its org reference, then
-- adding that foreign key at the end.

-- Pricing profiles are IMMUTABLE and versioned. A price change is a new version
-- row, never an UPDATE, so a historical cost figure can always be recomputed
-- (ADR-0018). Money is integer micros: no floating point anywhere near a cost.
CREATE TABLE pricing_profiles (
    id                       uuid        PRIMARY KEY,
    org_id                   uuid,                            -- NULL = global default; FK added below
    name                     text        NOT NULL,
    version                  integer     NOT NULL,
    currency                 char(3)     NOT NULL DEFAULT 'USD',
    cpu_core_hour_micros     bigint      NOT NULL,
    memory_gib_hour_micros   bigint      NOT NULL,
    gpu_hour_micros          jsonb       NOT NULL DEFAULT '{}'::jsonb,
    storage_gib_month_micros bigint      NOT NULL DEFAULT 0,
    egress_gib_micros        bigint      NOT NULL DEFAULT 0,
    markup_basis_points      integer     NOT NULL DEFAULT 0,
    effective_from           timestamptz NOT NULL,
    created_at               timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT uq_pricing_profiles__name_version UNIQUE (org_id, name, version),
    CONSTRAINT ck_pricing_profiles__version_positive CHECK (version > 0),
    CONSTRAINT ck_pricing_profiles__non_negative CHECK (
        cpu_core_hour_micros >= 0 AND memory_gib_hour_micros >= 0
        AND storage_gib_month_micros >= 0 AND egress_gib_micros >= 0
        AND markup_basis_points >= 0
    )
);

CREATE TRIGGER trg_pricing_profiles__immutable
    BEFORE UPDATE ON pricing_profiles
    FOR EACH ROW EXECUTE FUNCTION nebula_forbid_update();

CREATE TABLE organizations (
    id                 uuid        PRIMARY KEY,
    slug               text        NOT NULL,
    name               text        NOT NULL,
    pricing_profile_id uuid        REFERENCES pricing_profiles (id) ON DELETE SET NULL,
    default_namespace  text        NOT NULL DEFAULT 'nebula-workloads',
    quota              jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now(),
    deleted_at         timestamptz,

    CONSTRAINT uq_organizations__slug UNIQUE (slug),
    -- The DNS-label rule, because the slug appears in Kubernetes object names.
    -- Rejecting it here is cheaper than a reconcile failure later.
    CONSTRAINT ck_organizations__slug_format CHECK (slug ~ '^[a-z0-9]([a-z0-9-]{0,38}[a-z0-9])?$'),
    CONSTRAINT ck_organizations__namespace_format CHECK (default_namespace ~ '^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$')
);

ALTER TABLE pricing_profiles
    ADD CONSTRAINT fk_pricing_profiles__org
    FOREIGN KEY (org_id) REFERENCES organizations (id) ON DELETE CASCADE;

CREATE TRIGGER trg_organizations__updated_at
    BEFORE UPDATE ON organizations
    FOR EACH ROW EXECUTE FUNCTION nebula_set_updated_at();

CREATE TABLE users (
    id            uuid        PRIMARY KEY,
    org_id        uuid        NOT NULL REFERENCES organizations (id) ON DELETE RESTRICT,
    email         text        NOT NULL,
    name          text,
    role          user_role   NOT NULL DEFAULT 'developer',
    -- argon2id, correct for a low-entropy human secret verified once per login.
    -- NULL when the user authenticates through an external identity provider.
    password_hash text,
    last_login_at timestamptz,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    deleted_at    timestamptz,

    CONSTRAINT uq_users__email UNIQUE (email),
    CONSTRAINT ck_users__email_lowercase CHECK (email = lower(email)),
    CONSTRAINT ck_users__email_shape CHECK (email ~ '^[^@[:space:]]+@[^@[:space:]]+\.[^@[:space:]]+$')
);

CREATE INDEX ix_users__org ON users (org_id) WHERE deleted_at IS NULL;

CREATE TRIGGER trg_users__updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION nebula_set_updated_at();

CREATE TABLE rate_limit_policies (
    id                  uuid        PRIMARY KEY,
    org_id              uuid        REFERENCES organizations (id) ON DELETE CASCADE, -- NULL = built-in
    name                text        NOT NULL,
    requests_per_minute integer,
    tokens_per_minute   integer,
    max_concurrency     integer,
    max_queue_depth     integer,
    created_at          timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT uq_rate_limit_policies__org_name UNIQUE (org_id, name),
    CONSTRAINT ck_rate_limit_policies__positive CHECK (
        (requests_per_minute IS NULL OR requests_per_minute > 0)
        AND (tokens_per_minute IS NULL OR tokens_per_minute > 0)
        AND (max_concurrency IS NULL OR max_concurrency > 0)
        AND (max_queue_depth IS NULL OR max_queue_depth >= 0)
    )
);

-- Routing strategy is text + CHECK rather than an enum: strategies are plugins
-- registered in packages/routing, and adding one should not need a migration.
CREATE TABLE routing_policies (
    id         uuid        PRIMARY KEY,
    org_id     uuid        REFERENCES organizations (id) ON DELETE CASCADE, -- NULL = built-in
    name       text        NOT NULL,
    strategy   text        NOT NULL,
    config     jsonb       NOT NULL DEFAULT '{}'::jsonb,
    is_default boolean     NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT uq_routing_policies__org_name UNIQUE (org_id, name),
    CONSTRAINT ck_routing_policies__strategy_nonempty CHECK (length(strategy) > 0)
);

-- Exactly one built-in default, enforced by the database rather than by hoping
-- the seed data is right.
CREATE UNIQUE INDEX uq_routing_policies__one_builtin_default
    ON routing_policies ((org_id IS NULL))
    WHERE is_default AND org_id IS NULL;

CREATE TABLE api_keys (
    id                   uuid        PRIMARY KEY,
    org_id               uuid        NOT NULL REFERENCES organizations (id) ON DELETE CASCADE,
    user_id              uuid        REFERENCES users (id) ON DELETE SET NULL,
    name                 text        NOT NULL,
    -- 'nbk_' plus 7 characters. Safe to display, log and audit; indexed so key
    -- lookup is O(1) before the constant-time hash comparison (ADR-0011).
    prefix               char(11)    NOT NULL,
    -- HMAC-SHA256 with a server-side pepper. Not a slow KDF: an API key is 256
    -- bits of CSPRNG output, so brute force is not the threat model, and this
    -- runs on every inference request.
    key_hash             bytea       NOT NULL,
    scopes               text[]      NOT NULL DEFAULT ARRAY['inference:invoke'],
    -- Priority lives on the key, never in the request body, so callers cannot
    -- promote themselves.
    priority             text        NOT NULL DEFAULT 'NORMAL',
    rate_limit_policy_id uuid        REFERENCES rate_limit_policies (id) ON DELETE SET NULL,
    last_used_at         timestamptz,
    expires_at           timestamptz,
    revoked_at           timestamptz,
    created_at           timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT uq_api_keys__prefix UNIQUE (prefix),
    CONSTRAINT ck_api_keys__prefix_format CHECK (prefix ~ '^nbk_[A-Za-z0-9]{7}$'),
    CONSTRAINT ck_api_keys__priority CHECK (priority IN ('LOW', 'NORMAL', 'HIGH')),
    CONSTRAINT ck_api_keys__scopes_nonempty CHECK (array_length(scopes, 1) >= 1),
    CONSTRAINT ck_api_keys__hash_length CHECK (octet_length(key_hash) = 32)
);

CREATE INDEX ix_api_keys__org_active ON api_keys (org_id) WHERE revoked_at IS NULL;
