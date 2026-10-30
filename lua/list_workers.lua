-- KEYS[1]: the queue's active workers sorted set (e.g. "queue:workers")
-- KEYS[2]: worker metadata prefix (e.g. "queue:workers:metadata:")
--
-- Returns: a JSON array of worker objects. Empty results must be "[]" so the
-- client can deserialize them as a sequence instead of a map.

local workers = redis.call("ZRANGE", KEYS[1], 0, -1, "WITHSCORES")
local queue = string.gsub(KEYS[1], ":workers$", "")
local result = {}

for i = 1, #workers, 2 do
    local name = workers[i]
    local last_seen = tonumber(workers[i + 1])
    local meta = redis.call("HMGET", KEYS[2] .. name, "backend_name", "service")

    table.insert(result, {
        id = name,
        queue = queue,
        last_heartbeat = last_seen,
        started_at = 0,
        backend = meta[1] or "",
        layers = meta[2] or ""
    })
end

if #result == 0 then
    return "[]"
end

return cjson.encode(result)
