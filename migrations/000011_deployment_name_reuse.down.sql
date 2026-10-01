-- 000011 down. Fails if a name has been reused, which is the correct outcome:
-- restoring the old constraint would otherwise require destroying history.

DROP INDEX IF EXISTS uq_deployments__org_name_live;

ALTER TABLE deployments ADD CONSTRAINT uq_deployments__org_name UNIQUE (org_id, name);
