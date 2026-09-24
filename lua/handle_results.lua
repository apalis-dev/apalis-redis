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

for i = 0, num_tasks - 1 do
    local base = (i * args_per_task + 1)
    local task_id = ARGV[base + 1]
    local timestamp = ARGV[base + 2]
    local result_data = ARGV[base + 3]
    local status = ARGV[base + 4]
    local attempt = ARGV[base + 5]

    local removed = redis.call('SREM', inflight_set, task_id)
    if not removed then
        error("task not executed by worker")
    end

    if status == 'Done' then
        redis.call('SADD', done_jobs_set, task_id)
        if emit_events then
            redis.call("publish", "tasks:" .. done_jobs_set, task_id)
        end

    elseif (status == 'Failed' or status == 'Pending') and (tonumber(attempt) or 0) < 25 then
        -- TODO: @geofmureithi: Update lifecycle to check max_attempt so we hardcode 25
        -- We might also want to handle RetryAfter
        -- e if e.downcast_ref::<AbortError>().is_some() => Status::Killed,
        -- we also for now simulate a basic backoff timestamp + attempt
        redis.call("zadd", scheduled_jobs_set, tonumber(timestamp) + tonumber(attempt), task_id)
        if emit_events then
            redis.call("publish", "tasks:" .. scheduled_jobs_set .. ':retry', task_id)
        end
    else
        redis.call('SADD', dead_jobs_set, task_id)
        if emit_events then
            redis.call("publish", "tasks:" .. dead_jobs_set, task_id)
        end
    end

    local meta_key = job_meta_hash .. ':' .. task_id
    redis.call('HSET', meta_key, 'result', result_data, 'status', status, 'attempts', attempt, 'done_at', timestamp)

    acked = acked + 1
end

return acked
