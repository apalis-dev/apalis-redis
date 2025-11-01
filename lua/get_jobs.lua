-- KEYS[1]: the active workers set
-- KEYS[2]: the active task list
-- KEYS[3]: this worker's inflight set
-- KEYS[4]: the task data hash
-- KEYS[5]: the signal list
-- KEYS[6]: the task meta prefix (e.g. "task_meta")
-- ARGV[1]: the max number of tasks to get
-- ARGV[2]: this worker's inflight set

-- Ensure the worker is registered
local registered = redis.call("zscore", KEYS[1], ARGV[2])
if not registered then
    error("worker not registered")
end

-- Get up to N task IDs from the active list
local task_ids = redis.call("lrange", KEYS[2], 0, ARGV[1] - 1)
local count = #task_ids
local results = {}
local meta = {}

if count > 0 then
    -- Mark tasks as inflight
    redis.call("sadd", KEYS[3], unpack(task_ids))
    -- Trim them from the active queue
    redis.call("ltrim", KEYS[2], count, -1)
    -- Get task data
    results = redis.call("hmget", KEYS[4], unpack(task_ids))

    -- Fetch metadata for each task dynamically
    for i, task_id in ipairs(task_ids) do
        local meta_key = KEYS[6] .. ':' .. task_id
        local fields = redis.call("hgetall", meta_key)
        -- Insert task_id as first element for context
        table.insert(fields, 1, task_id)
        table.insert(meta, fields)
    end
end

-- If fewer tasks were returned than requested, signal idle workers
if count < tonumber(ARGV[1]) then
    redis.call("del", KEYS[5])
end

return {results, meta}
