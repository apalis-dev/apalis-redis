-- Returns every registered worker across the queues in the global registry.
-- Registry members are the queues' "<queue>:workers" sorted sets.

local queues = redis.call("ZRANGE", "core:apalis:queues:list", 0, -1)
local result = {}

for _, workers_key in ipairs(queues) do
    local queue = string.gsub(workers_key, ":workers$", "")
    local metadata_prefix = queue .. ":workers:metadata:"
    local workers = redis.call("ZRANGE", workers_key, 0, -1, "WITHSCORES")

    for i = 1, #workers, 2 do
        local name = workers[i]
        local last_seen = tonumber(workers[i + 1])
        local meta = redis.call("HMGET", metadata_prefix .. name, "backend_name", "service")

        table.insert(result, {
            queue = queue,
            id = name,
            last_heartbeat = last_seen,
            started_at = 0,
            backend = meta[1] or "",
            layers = meta[2] or ""
        })
    end
end

if #result == 0 then
    return "[]"
end

return cjson.encode(result)
