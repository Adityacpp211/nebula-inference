-- 000001 down

DROP FUNCTION IF EXISTS nebula_forbid_update();
DROP FUNCTION IF EXISTS nebula_ensure_month_partition(regclass, date);
DROP FUNCTION IF EXISTS nebula_set_updated_at();

DROP TYPE IF EXISTS experiment_state;
DROP TYPE IF EXISTS rollout_state;
DROP TYPE IF EXISTS deployment_state;
DROP TYPE IF EXISTS model_version_status;
DROP TYPE IF EXISTS user_role;
