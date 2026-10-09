-- Atomic admission: six limits checked, then six charged, or none.
--
-- KEYS: key_rpm, key_tpm, key_conc, org_rpm, org_tpm, org_conc
-- ARGV: now_ms, key_rpm, key_tpm, key_conc, org_rpm, org_tpm, org_conc,
--       tokens, lease_id, lease_expiry_ms, bucket_ttl_ms
-- A limit of 0 means unlimited and its key is never touched.
--
-- Reply: {allowed, reason, scope, retry_ms,
--         key_rpm_remaining, key_rpm_reset_ms, key_tpm_remaining, key_tpm_reset_ms, 0, 0}
-- reason: 0 none, 1 rpm, 2 tpm, 3 concurrency. scope: 0 none, 1 key, 2 org.
--
-- Buckets are hashes {t = tokens, ts = last update ms}. A missing bucket is full.
-- Numbers are written with string.format so no precision is lost to Redis's own
-- float formatting, and every reply value is an integer, because Redis truncates a
-- Lua float in a reply.

local now = tonumber(ARGV[1])
local tokens = tonumber(ARGV[8])
local lease_id = ARGV[9]
local lease_expiry = tonumber(ARGV[10])
local bucket_ttl = tonumber(ARGV[11])

local function level(key, cap)
  local v = redis.call('HMGET', key, 't', 'ts')
  local t, ts = tonumber(v[1]), tonumber(v[2])
  if t == nil or ts == nil then
    return cap
  end
  local elapsed = now - ts
  if elapsed < 0 then elapsed = 0 end
  t = t + elapsed * cap / 60000
  if t > cap then t = cap end
  return t
end

local function wait_ms(deficit, cap)
  return math.ceil(deficit * 60000 / cap)
end

local function reserve(cap)
  if cap > 0 and tokens > cap then return cap end
  return tokens
end

-- index into KEYS, capacity, cost, reason, scope
local buckets = {
  {1, tonumber(ARGV[2]), 1, 1, 1},
  {2, tonumber(ARGV[3]), reserve(tonumber(ARGV[3])), 2, 1},
  {4, tonumber(ARGV[5]), 1, 1, 2},
  {5, tonumber(ARGV[6]), reserve(tonumber(ARGV[6])), 2, 2},
}
local levels = {}

for i, b in ipairs(buckets) do
  local cap = b[2]
  if cap > 0 then
    levels[i] = level(KEYS[b[1]], cap)
    if levels[i] < b[3] then
      return {0, b[4], b[5], wait_ms(b[3] - levels[i], cap), 0, 0, 0, 0, 0, 0}
    end
  end
end

local conc = {{3, tonumber(ARGV[4]), 1}, {6, tonumber(ARGV[7]), 2}}
for _, c in ipairs(conc) do
  if c[2] > 0 then
    redis.call('ZREMRANGEBYSCORE', KEYS[c[1]], '-inf', now)
    if redis.call('ZCARD', KEYS[c[1]]) >= c[2] then
      return {0, 3, c[3], 1000, 0, 0, 0, 0, 0, 0}
    end
  end
end

-- Every check passed: charge everything.
local reply = {1, 0, 0, 0, 0, 0, 0, 0, 0, 0}
for i, b in ipairs(buckets) do
  local cap = b[2]
  if cap > 0 then
    local left = levels[i] - b[3]
    redis.call('HSET', KEYS[b[1]], 't', string.format('%.6f', left), 'ts', string.format('%d', now))
    redis.call('PEXPIRE', KEYS[b[1]], bucket_ttl)
    if b[1] == 1 then
      reply[5] = math.floor(left)
      reply[6] = math.ceil((cap - left) * 60000 / cap)
    elseif b[1] == 2 then
      reply[7] = math.floor(left)
      reply[8] = math.ceil((cap - left) * 60000 / cap)
    end
  end
end
for _, c in ipairs(conc) do
  if c[2] > 0 then
    redis.call('ZADD', KEYS[c[1]], lease_expiry, lease_id)
    redis.call('PEXPIREAT', KEYS[c[1]], lease_expiry + 60000)
  end
end
return reply
