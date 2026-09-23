-- 000006 down
--
-- Dropping a partitioned table drops its partitions with it.

DROP TABLE IF EXISTS audit_logs;
DROP TABLE IF EXISTS usage_records;
DROP TABLE IF EXISTS requests;
DROP TABLE IF EXISTS autoscaling_events;
DROP TABLE IF EXISTS worker_events;
DROP TABLE IF EXISTS nodes;
