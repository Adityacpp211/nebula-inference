-- 000008 down
--
-- Deletes only the seeded rows, by their fixed identifiers, so an operator's own
-- policies and pricing profiles survive a rewind.

DELETE FROM rate_limit_policies WHERE id = '00000000-0000-7000-8000-000000000021';

DELETE FROM routing_policies WHERE id IN (
    '00000000-0000-7000-8000-000000000011',
    '00000000-0000-7000-8000-000000000012',
    '00000000-0000-7000-8000-000000000013',
    '00000000-0000-7000-8000-000000000014',
    '00000000-0000-7000-8000-000000000015',
    '00000000-0000-7000-8000-000000000016'
);

-- pricing_profiles is immutable via trigger, but the trigger is on UPDATE only:
-- deleting a seed row is a legitimate rewind of this migration.
DELETE FROM pricing_profiles WHERE id = '00000000-0000-7000-8000-000000000001';
