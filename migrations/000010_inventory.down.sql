-- 000010 down

ALTER TABLE nodes DROP CONSTRAINT IF EXISTS ck_nodes__requested_object;
ALTER TABLE nodes DROP COLUMN IF EXISTS requested;
