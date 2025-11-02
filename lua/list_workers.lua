-- KEYS[1]: active workers sorted set
-- KEYS[2]: metadata prefix key (e.g. "worker:")

-- Returns: an array of tables, each containing:
-- {
--   worker = <string>,
--   last_seen = <number>,
--   backend_name = <string>,
--   service = <string>
-- }

local workers = redis.call("zrange", KEYS[1], 0, -1, "WITHSCORES")
local result = {}

for i = 1, #workers, 2 do
    local name = workers[i]
    local last_seen = tonumber(workers[i + 1])
    local meta_key = KEYS[2] .. name

    local meta = redis.call("hmget", meta_key, "backend_name", "service")
    table.insert(result, {
        id = name,
        queue = KEYS[1],
        last_heartbeat = last_seen,
        started_at = 0,
        backend = meta[1],
        layers = meta[2]
    })
end

return cjson.encode(result)
