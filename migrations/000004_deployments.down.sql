-- 000004 down

DROP TRIGGER IF EXISTS trg_route_targets__weights_total_100 ON route_targets;
DROP FUNCTION IF EXISTS nebula_check_route_weights();
DROP TABLE IF EXISTS route_targets;
DROP TABLE IF EXISTS routes;
DROP TABLE IF EXISTS deployment_revisions;
DROP TABLE IF EXISTS deployments;
