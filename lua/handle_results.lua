-- KEYS[1] =  the worker inflight_set
-- KEYS[2] = done_jobs_set
-- KEYS[3] = dead_jobs_set
-- KEYS[4] = scheduled_jobs_set
-- KEYS[5] = job_meta_hash
-- ARGV[1] = emit events
-- ARGV[2..] = repeating groups of: task_id, timestamp, result_data, status, attempt
local inflight_set = KEYS[1]
local done_jobs_set = KEYS[2]
local dead_jobs_set = KEYS[3]
local scheduled_jobs_set = KEYS[4]
local job_meta_hash = KEYS[5]
local emit_events = ARGV[1] == "1"

local args_per_task = 5
local num_tasks = (#ARGV - 1) / args_per_task
local acked = 0

-- Terminal states are stored as sorted sets scored by completion time. Older
-- deployments stored them as plain sets, so append to whichever shape the key
-- already has instead of failing with WRONGTYPE.
local function add_terminal_state(key, task_id, timestamp)
    local key_type = redis.call('TYPE', key)
    if type(key_type) == 'table' then
        key_type = key_type['ok']
    end

    if key_type == 'none' or key_type == 'zset' then
        redis.call('ZADD', key, timestamp, task_id)
    elseif key_type == 'set' then
        redis.call('SADD', key, task_id)
    else
        error('Unsupported data type for ' .. key .. ': ' .. key_type)
    end
end

-- Parses a retry delay out of result_data, returning it in seconds, or nil if absent.
-- Expects serde's Result encoding: {"Ok": ...} or {"Err": "RetryError: ..., duration: 1.5s"}
local unit_to_seconds = {
    s = 1,
    ms = 1e-3,
    ["µs"] = 1e-6, -- multibyte, so match on the raw UTF-8 string
    ns = 1e-9
}

local function try_parse_retry_delay(result_data)
    -- Cheap prefilter to avoid decoding JSON for ordinary failures
    if not result_data or not string.find(result_data, 'RetryAfterError:', 1, true) then
        return nil
    end

    local ok, decoded = pcall(cjson.decode, result_data)
    if not ok or type(decoded) ~= 'table' or type(decoded.Err) ~= 'string' then
        return nil
    end

    local amount, unit = string.match(decoded.Err, ', duration: ([%d%.]+)([^%d%.]+)$')
    local factor = unit and unit_to_seconds[unit]
    if not amount or not factor then
        return nil
    end

    local value = tonumber(amount)
    if not value then
        return nil
    end
    return math.ceil(value * factor)
end

for i = 0, num_tasks - 1 do
    local base = (i * args_per_task + 1)
    local task_id = ARGV[base + 1]
    local timestamp = ARGV[base + 2]
    local result_data = ARGV[base + 3]
    local status = ARGV[base + 4]
    local attempt = ARGV[base + 5]

    local removed = redis.call('SREM', inflight_set, task_id)
    if removed == 0 then
        error("task not executed by worker")
    end

    local meta_key = job_meta_hash .. ':' .. task_id

    -- Read max_attempts as stored at enqueue time. HGET returns false if the
    -- field is missing (e.g. tasks enqueued by an older version), so fall back
    -- to the previous default of 25 in that case.
    local max_attempts = tonumber(redis.call('HGET', meta_key, 'max_attempts')) or 25

    if status == 'Done' then
        add_terminal_state(done_jobs_set, task_id, timestamp)
        if emit_events then
            redis.call("publish", "tasks:" .. done_jobs_set, task_id)
        end

    elseif (status == 'Failed' or status == 'Pending') and (tonumber(attempt) or 0) < max_attempts then
        -- Prefer the handler-requested delay (RetryAfterError), else simple backoff
        local delay = try_parse_retry_delay(result_data) or (tonumber(attempt) or 0)
        redis.call("zadd", scheduled_jobs_set, tonumber(timestamp) + delay, task_id)
        if emit_events then
            redis.call("publish", "tasks:" .. scheduled_jobs_set .. ':retry', task_id)
        end
    else
        add_terminal_state(dead_jobs_set, task_id, timestamp)
        if emit_events then
            redis.call("publish", "tasks:" .. dead_jobs_set, task_id)
        end
    end

    redis.call('HSET', meta_key, 'result', result_data, 'status', status, 'attempts', attempt, 'done_at', timestamp)

    acked = acked + 1
end

return acked
