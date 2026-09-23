-- 000002 down

DROP TABLE IF EXISTS api_keys;
DROP TABLE IF EXISTS routing_policies;
DROP TABLE IF EXISTS rate_limit_policies;
DROP TABLE IF EXISTS users;

-- Break the organizations <-> pricing_profiles cycle before dropping either.
ALTER TABLE IF EXISTS pricing_profiles DROP CONSTRAINT IF EXISTS fk_pricing_profiles__org;

DROP TABLE IF EXISTS organizations;
DROP TABLE IF EXISTS pricing_profiles;
