-- KEYS[1]: the active workers set
-- KEYS[2]: metadata prefix key (e.g. "worker:")
-- ARGV[1]: current time
-- ARGV[2]: worker name
-- ARGV[3]: threshold
-- ARGV[4]: backend_name
-- ARGV[5]: service
local now = tonumber(ARGV[1])
local worker = ARGV[2]
local threshold = tonumber(ARGV[3])
local backend_name = ARGV[4]
local service = ARGV[5]

local last_seen = redis.call("zscore", KEYS[1], worker)
if last_seen then
    if now - tonumber(last_seen) < threshold then
        error("worker is still active within threshold")
    end
end

-- Update the active workers sorted set
redis.call("zadd", KEYS[1], now, worker)

-- Register as a queue if missing
redis.call("zadd", "core::apalis::queues::list", 'GT', now, KEYS[1])

-- Store or update worker metadata
local meta_key = KEYS[2] .. worker
redis.call("hmset", meta_key, "backend_name", backend_name, "service", service)

return true
