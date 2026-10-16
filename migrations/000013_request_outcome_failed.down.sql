-- 000013 down. Fails if a failed request has been stored, which is the correct
-- outcome: dropping the value would mean deleting usage history.

ALTER TABLE requests DROP CONSTRAINT ck_requests__outcome;

ALTER TABLE requests ADD CONSTRAINT ck_requests__outcome CHECK (outcome IN (
    'completed', 'client_cancelled', 'stream_interrupted', 'deadline_exceeded',
    'queue_timeout', 'rejected'
));
