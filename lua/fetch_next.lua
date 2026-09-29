-- KEYS[1]: the active workers sorted set
-- KEYS[2]: the active job list
-- KEYS[3]: this workers inflight set
-- KEYS[4]: the task data hash
-- KEYS[5]: the signal list
-- KEYS[6]: the task metadata prefix
-- KEYS[7]: the scheduled jobs sorted set

-- ARGV[1]: current timestamp
-- ARGV[2]: maximum number of jobs to fetch
-- ARGV[3]: worker name

-- Returns: {results, meta}

local worker_inflight_set = KEYS[3]
local worker_name = ARGV[3]


-- Ensure the worker is registered
local registered = redis.call("zscore", KEYS[1], worker_name)
if not registered then
    error("Cant fetch next: worker not registered")
end

-- 1. Promote any due scheduled jobs
local scheduled_ids = redis.call(
    "zrangebyscore",
    KEYS[7],
    0,
    ARGV[1]
)

if #scheduled_ids > 0 then
    -- Push scheduled jobs onto the active queue
    redis.call("rpush", KEYS[2], unpack(scheduled_ids))

    -- Remove promoted jobs from the scheduled set
    redis.call("zrem", KEYS[7], unpack(scheduled_ids))

    -- Signal that jobs are available
    redis.call("del", KEYS[5])
    redis.call("lpush", KEYS[5], 1)
end

-- 2. Fetch jobs from the active queue
local task_ids = redis.call(
    "lrange",
    KEYS[2],
    0,
    tonumber(ARGV[2]) - 1
)

local count = #task_ids
local results = {}
local meta = {}

if count > 0 then
    -- Mark jobs as inflight
    redis.call("sadd", worker_inflight_set, unpack(task_ids))

    -- Remove fetched jobs from the active queue
    redis.call("ltrim", KEYS[2], count, -1)

    -- Fetch job data
    results = redis.call(
        "hmget",
        KEYS[4],
        unpack(task_ids)
    )

    -- Fetch metadata
    for _, task_id in ipairs(task_ids) do
        local meta_key = KEYS[6] .. ":" .. task_id
        local fields = redis.call("hgetall", meta_key)

        -- Insert task ID as the first element
        table.insert(fields, 1, task_id)
        table.insert(meta, fields)
    end
end

-- 3. Clear the signal if fewer jobs were fetched
-- than requested
if count < tonumber(ARGV[2]) then
    redis.call("del", KEYS[5])
end

return {results, meta}
