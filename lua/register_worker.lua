-- KEYS[1]: the active workers set
-- KEYS[2]: worker metadata_key
-- ARGV[1]: current time
-- ARGV[2]: worker name
-- ARGV[3]: threshold
-- ARGV[4]: backend_name
-- ARGV[5]: service
local now = tonumber(ARGV[1])
local worker_name = ARGV[2]
local threshold = tonumber(ARGV[3])
local backend_name = ARGV[4]
local service = ARGV[5]
local worker_metadata_key = KEYS[2]

local last_seen = redis.call("zscore", KEYS[1], worker_name)
if last_seen then
    if now - tonumber(last_seen) < threshold then
        error("worker is still active within threshold")
    end
end

-- Update the active workers sorted set
redis.call("zadd", KEYS[1], now, worker_name)

-- Register as a queue if missing
redis.call("zadd", "core:apalis:queues:list", 'GT', now, KEYS[1])

-- Store or update worker metadata
redis.call("hmset", worker_metadata_key, "backend_name", backend_name, "service", service)

return true
