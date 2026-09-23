-- 000003 down

DROP TABLE IF EXISTS model_artifacts;
DROP TRIGGER IF EXISTS trg_model_versions__immutable ON model_versions;
DROP FUNCTION IF EXISTS nebula_model_version_immutable();
DROP TABLE IF EXISTS model_versions;
DROP TABLE IF EXISTS models;
