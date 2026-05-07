local queues = redis.call("zrange", "core::apalis::queues::list", 0, -1)
local result = {}

for _, queue_key in ipairs(queues) do
    local workers = redis.call("zrange", queue_key, 0, -1, "WITHSCORES")
    -- Derive metadata key from queue key, same pattern as registration
    local meta_key = queue_key .. ":workers:metadata"

    for i = 1, #workers, 2 do
        local name = workers[i]
        local last_seen = tonumber(workers[i + 1])

        local meta_json = redis.call("hget", meta_key, name)
        local backend = ""
        local service = ""
        if meta_json then
            local ok, decoded = pcall(cjson.decode, meta_json)
            if ok then
                backend = decoded.storage or ""
                service = decoded.service or ""
            end
        end

        table.insert(result, {
            queue = queue_key,
            id = name,
            last_heartbeat = last_seen,
            started_at = 0,
            backend = backend,
            layers = service,
        })
    end
end

return cjson.encode(result)
