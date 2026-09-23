-- 000001 enums and helpers
--
-- Enum types are used for closed, slow-changing sets so the database itself
-- rejects an invalid value. Adding a value is then a migration, which is the
-- point: a new deployment state is a design decision, not a typo.
--
-- Sets expected to grow (routing strategies, event types) are text + CHECK
-- instead, because a new routing strategy should not require a migration.

CREATE TYPE user_role AS ENUM ('owner', 'admin', 'developer', 'viewer');

CREATE TYPE model_version_status AS ENUM ('uploading', 'verifying', 'ready', 'failed', 'archived');

CREATE TYPE deployment_state AS ENUM ('pending', 'progressing', 'ready', 'degraded', 'failed', 'deleting', 'deleted');

CREATE TYPE rollout_state AS ENUM ('pending', 'progressing', 'paused', 'promoted', 'aborted', 'rolled_back', 'failed');

CREATE TYPE experiment_state AS ENUM ('draft', 'running', 'stopped', 'concluded');

-- Keeps updated_at honest. Doing this in the database rather than in application
-- code means every writer is covered, including psql during an incident.
CREATE FUNCTION nebula_set_updated_at() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$;

-- Creates the monthly partition covering `month` for a range-partitioned table,
-- if it does not already exist. Idempotent, so the Phase 13 maintenance job can
-- call it on a schedule without coordination.
CREATE FUNCTION nebula_ensure_month_partition(parent regclass, month date) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
    base       text;
    start_date date := date_trunc('month', month)::date;
    end_date   date := (date_trunc('month', month) + interval '1 month')::date;
    part_name  text;
BEGIN
    SELECT relname INTO base FROM pg_class WHERE oid = parent;
    IF base IS NULL THEN
        RAISE EXCEPTION 'nebula_ensure_month_partition: unknown table %', parent;
    END IF;

    part_name := format('%s_%s', base, to_char(start_date, 'YYYYMM'));

    IF EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = part_name AND n.nspname = current_schema()
    ) THEN
        RETURN part_name;
    END IF;

    EXECUTE format(
        'CREATE TABLE %I PARTITION OF %s FOR VALUES FROM (%L) TO (%L)',
        part_name, parent::text, start_date, end_date
    );
    RETURN part_name;
END;
$$;

-- Rejects any UPDATE. Used for tables whose rows are historical facts: an
-- immutability rule enforced only in application code survives exactly until the
-- first admin console or ad-hoc migration that bypasses it.
CREATE FUNCTION nebula_forbid_update() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'table % is append-only; rows cannot be updated', TG_TABLE_NAME
        USING ERRCODE = 'restrict_violation';
END;
$$;
