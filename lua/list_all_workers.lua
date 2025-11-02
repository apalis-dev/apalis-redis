-- KEYS: list of active workers sorted sets (one per queue)
-- ARGV[1]: metadata prefix key (e.g. "worker:")
-- Returns: JSON array of objects:
-- [
--   {
--     queue: <queue_key>,
--     worker: <worker_name>,
--     last_seen: <number>,
--     backend_name: <string>,
--     service: <string>
--   },
--   ...
-- ]
local meta_prefix = ARGV[1]
local result = {}

for _, queue_key in ipairs(KEYS) do
    local workers = redis.call("zrange", queue_key, 0, -1, "WITHSCORES")

    for i = 1, #workers, 2 do
        local name = workers[i]
        local last_seen = tonumber(workers[i + 1])
        local meta_key = meta_prefix .. name

        local meta = redis.call("hmget", meta_key, "backend_name", "service")

        table.insert(result, {
            queue = queue_key,
            id = name,
            last_heartbeat = last_seen,
            started_at = 0,
            backend = meta[1],
            layers = meta[2]
        })
    end
end

return cjson.encode(result)
