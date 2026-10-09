-- 000011 deployment names are unique among LIVE deployments.
--
-- Deployments are soft-deleted (deleted_at), so the history, revisions and audit
-- trail of a deleted deployment survive. The original constraint made its name
-- unique forever, so `qwen-prod` could never be deployed again once deleted —
-- found by the Phase 5 kind demo, which deletes and recreates a deployment.
--
-- A partial unique index keeps the guarantee that matters: no two live deployments
-- in an organization share a name, because the name is the Kubernetes object name.

ALTER TABLE deployments DROP CONSTRAINT uq_deployments__org_name;

CREATE UNIQUE INDEX uq_deployments__org_name_live
    ON deployments (org_id, name) WHERE deleted_at IS NULL;
