-- KEYS[1]: the active workers sorted set
--
-- ARGV[1]: current time
-- ARGV[2]: worker name
local now = tonumber(ARGV[1])
local worker = ARGV[2]

local last_seen = redis.call("zscore", KEYS[1], worker)

-- Worker is not registered
if not last_seen then
    error("Heartbeat failed: worker not registered")
end

-- Update the worker's heartbeat
redis.call("zadd", KEYS[1], now, worker)

return true
