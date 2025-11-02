-- KEYS: alternating data hash key and meta prefix per queue
-- Example:
--   KEYS = [ "queueA:data", "queueA:meta",
--             "queueB:data", "queueB:meta", ... ]
-- ARGV[1]: status filter (empty string for none)
-- ARGV[2]: page number
-- ARGV[3]: page size
local status_filter = ARGV[1]
local page = tonumber(ARGV[2])
local page_size = tonumber(ARGV[3])

if not page or not page_size then
    return redis.error_reply("missing pagination parameters")
end

local start = (page - 1) * page_size
local stop = start + page_size - 1

local task_entries = {}

-- Collect all task IDs across all queues
for i = 1, #KEYS, 2 do
    local data_key = KEYS[i]
    local meta_prefix = KEYS[i + 1]

    local ids = redis.call("hkeys", data_key)
    for _, id in ipairs(ids) do
        table.insert(task_entries, {id, data_key, meta_prefix})
    end
end

if #task_entries == 0 then
    return {{}, {}}
end

-- Sort globally by task_id
table.sort(task_entries, function(a, b)
    return a[1] < b[1]
end)

-- Apply pagination
local paginated = {}
for i = start + 1, math.min(stop + 1, #task_entries) do
    table.insert(paginated, task_entries[i])
end

local job_data_list = {}
local meta_list = {}

for _, entry in ipairs(paginated) do
    local task_id = entry[1]
    local data_key = entry[2]
    local meta_prefix = entry[3]

    local meta_key = meta_prefix .. ":" .. task_id
    local include = true

    if status_filter and status_filter ~= "" then
        local status = redis.call("hget", meta_key, "status")
        include = (status == status_filter)
    end

    if include then
        local data = redis.call("hmget", data_key, task_id)
        local meta_fields = redis.call("hgetall", meta_key)
        table.insert(job_data_list, data[1])
        table.insert(meta_fields, 1, task_id)
        table.insert(meta_list, meta_fields)
    end
end

return {job_data_list, meta_list}
