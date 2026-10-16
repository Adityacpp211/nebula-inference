-- 000013 a request can fail.
--
-- The gateway's usage record has carried outcome "failed" since Phase 4 — a
-- request the worker or the gateway failed outright, distinct from a stream cut
-- partway ("stream_interrupted") — but requests.outcome refused it, so the usage
-- ingester (Phase 13) could not store one. A failed request is still a request:
-- it is counted, and charged for whatever compute it used.

ALTER TABLE requests DROP CONSTRAINT ck_requests__outcome;

ALTER TABLE requests ADD CONSTRAINT ck_requests__outcome CHECK (outcome IN (
    'completed', 'client_cancelled', 'stream_interrupted', 'deadline_exceeded',
    'queue_timeout', 'rejected', 'failed'
));
