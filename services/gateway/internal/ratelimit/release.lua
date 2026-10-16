-- Settle a lease: return the concurrency slots and correct the token reservation.
--
-- KEYS: key_tpm, org_tpm, key_conc, org_conc
-- ARGV: now_ms, key_tpm_cap, org_tpm_cap, key_delta, org_delta, lease_id, bucket_ttl_ms
--
-- delta is reservation minus actual: positive refunds unused tokens, negative
-- charges a request that used more than it reserved. A bucket may go negative, down
-- to minus its capacity, which makes the NEXT requests wait for the overdraft to be
-- repaid instead of letting it disappear.

local now = tonumber(ARGV[1])
local bucket_ttl = tonumber(ARGV[7])

local function settle(key, cap, delta)
  if cap <= 0 then return end
  local v = redis.call('HMGET', key, 't', 'ts')
  local t, ts = tonumber(v[1]), tonumber(v[2])
  if t == nil or ts == nil then
    t = cap
  else
    local elapsed = now - ts
    if elapsed < 0 then elapsed = 0 end
    t = t + elapsed * cap / 60000
    if t > cap then t = cap end
  end
  t = t + delta
  if t > cap then t = cap end
  if t < -cap then t = -cap end
  redis.call('HSET', key, 't', string.format('%.6f', t), 'ts', string.format('%d', now))
  redis.call('PEXPIRE', key, bucket_ttl)
end

settle(KEYS[1], tonumber(ARGV[2]), tonumber(ARGV[4]))
settle(KEYS[2], tonumber(ARGV[3]), tonumber(ARGV[5]))
redis.call('ZREM', KEYS[3], ARGV[6])
redis.call('ZREM', KEYS[4], ARGV[6])
return 1
