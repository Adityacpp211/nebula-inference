-- 000012 down.

DROP TRIGGER IF EXISTS trg_deployments__scale_generation ON deployments;
DROP FUNCTION IF EXISTS nebula_bump_generation_on_scale();
