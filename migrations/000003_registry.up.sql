-- 000003 model registry
--
-- The central rule: a model_version is IMMUTABLE once it reaches 'ready'
-- (ADR-0010). It is enforced by a trigger, not only by application code, because
-- "what exactly was serving at 14:32 last Tuesday" must be answerable and
-- application-level immutability survives until the first ad-hoc UPDATE.

CREATE TABLE models (
    id          uuid        PRIMARY KEY,
    org_id      uuid        NOT NULL REFERENCES organizations (id) ON DELETE CASCADE,
    name        text        NOT NULL,
    family      text,
    task        text        NOT NULL DEFAULT 'chat',
    description text,
    created_by  uuid        REFERENCES users (id) ON DELETE SET NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    deleted_at  timestamptz,

    CONSTRAINT uq_models__org_name UNIQUE (org_id, name),
    CONSTRAINT ck_models__name_format CHECK (name ~ '^[a-z0-9]([a-z0-9._-]{0,62}[a-z0-9])?$'),
    CONSTRAINT ck_models__task CHECK (task IN ('chat', 'completion', 'embedding'))
);

CREATE TRIGGER trg_models__updated_at
    BEFORE UPDATE ON models
    FOR EACH ROW EXECUTE FUNCTION nebula_set_updated_at();

CREATE TABLE model_versions (
    id               uuid                 PRIMARY KEY,
    model_id         uuid                 NOT NULL REFERENCES models (id) ON DELETE RESTRICT,
    version          text                 NOT NULL,
    format           text                 NOT NULL,
    runtime          text                 NOT NULL,
    quantization     text,
    parameter_count  bigint,
    size_bytes       bigint               NOT NULL,
    checksum_sha256  bytea                NOT NULL,
    context_window   integer              NOT NULL,
    artifact_uri     text                 NOT NULL,
    hardware_profile jsonb                NOT NULL,
    runtime_config   jsonb                NOT NULL DEFAULT '{}'::jsonb,
    status           model_version_status NOT NULL DEFAULT 'uploading',
    failure_reason   text,
    created_by       uuid                 REFERENCES users (id) ON DELETE SET NULL,
    created_at       timestamptz          NOT NULL DEFAULT now(),
    ready_at         timestamptz,

    CONSTRAINT uq_model_versions__model_version UNIQUE (model_id, version),
    CONSTRAINT ck_model_versions__version_format CHECK (version ~ '^[a-z0-9]([a-z0-9._-]{0,62}[a-z0-9])?$'),
    CONSTRAINT ck_model_versions__context_window CHECK (context_window > 0),
    CONSTRAINT ck_model_versions__size CHECK (size_bytes > 0),
    CONSTRAINT ck_model_versions__checksum_length CHECK (octet_length(checksum_sha256) = 32),
    CONSTRAINT ck_model_versions__format CHECK (format IN ('gguf', 'safetensors', 'mock')),
    CONSTRAINT ck_model_versions__runtime CHECK (runtime IN ('llamacpp', 'vllm', 'mock')),
    -- A row claiming to be ready must actually carry what readiness means.
    CONSTRAINT ck_model_versions__ready_fields CHECK (
        status <> 'ready' OR ready_at IS NOT NULL
    ),
    CONSTRAINT ck_model_versions__failure_reason CHECK (
        status <> 'failed' OR failure_reason IS NOT NULL
    ),
    CONSTRAINT ck_model_versions__hardware_profile_object CHECK (
        jsonb_typeof(hardware_profile) = 'object'
    ),
    CONSTRAINT ck_model_versions__runtime_config_object CHECK (
        jsonb_typeof(runtime_config) = 'object'
    )
);

CREATE INDEX ix_model_versions__model ON model_versions (model_id);
CREATE INDEX ix_model_versions__checksum ON model_versions (checksum_sha256);
CREATE INDEX ix_model_versions__ready ON model_versions (model_id) WHERE status = 'ready';

-- Immutability. Only status and failure_reason may change once ready, and status
-- may only move ready -> archived. Everything that identifies the artifact or
-- constrains its placement is frozen.
CREATE FUNCTION nebula_model_version_immutable() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.status <> 'ready' THEN
        RETURN NEW;
    END IF;

    IF NEW.checksum_sha256  IS DISTINCT FROM OLD.checksum_sha256
        OR NEW.artifact_uri     IS DISTINCT FROM OLD.artifact_uri
        OR NEW.format           IS DISTINCT FROM OLD.format
        OR NEW.runtime          IS DISTINCT FROM OLD.runtime
        OR NEW.quantization     IS DISTINCT FROM OLD.quantization
        OR NEW.context_window   IS DISTINCT FROM OLD.context_window
        OR NEW.size_bytes       IS DISTINCT FROM OLD.size_bytes
        OR NEW.parameter_count  IS DISTINCT FROM OLD.parameter_count
        OR NEW.hardware_profile IS DISTINCT FROM OLD.hardware_profile
        OR NEW.runtime_config   IS DISTINCT FROM OLD.runtime_config
        OR NEW.model_id         IS DISTINCT FROM OLD.model_id
        OR NEW.version          IS DISTINCT FROM OLD.version
    THEN
        RAISE EXCEPTION
            'model_version % is ready and immutable: register a new version instead', OLD.id
            USING ERRCODE = 'restrict_violation';
    END IF;

    IF NEW.status <> OLD.status AND NEW.status <> 'archived' THEN
        RAISE EXCEPTION
            'model_version % is ready; status may only change to archived (attempted %)', OLD.id, NEW.status
            USING ERRCODE = 'restrict_violation';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_model_versions__immutable
    BEFORE UPDATE ON model_versions
    FOR EACH ROW EXECUTE FUNCTION nebula_model_version_immutable();

-- Multi-file models (sharded safetensors, tokenizer, config) need per-file
-- integrity, so the artifact is a set rather than a single blob.
CREATE TABLE model_artifacts (
    id               uuid   PRIMARY KEY,
    model_version_id uuid   NOT NULL REFERENCES model_versions (id) ON DELETE CASCADE,
    path             text   NOT NULL,
    size_bytes       bigint NOT NULL,
    checksum_sha256  bytea  NOT NULL,

    CONSTRAINT uq_model_artifacts__version_path UNIQUE (model_version_id, path),
    CONSTRAINT ck_model_artifacts__size CHECK (size_bytes >= 0),
    CONSTRAINT ck_model_artifacts__checksum_length CHECK (octet_length(checksum_sha256) = 32),
    -- No absolute paths and no traversal: these become file paths inside a pod.
    CONSTRAINT ck_model_artifacts__relative_path CHECK (path !~ '^/' AND path !~ '\.\.')
);
