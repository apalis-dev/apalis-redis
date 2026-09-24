-- KEYS[1]: this worker's inflight set
-- KEYS[2]: the active jobs list
-- KEYS[3]: the signal list
-- KEYS[4]: the active workers sorted set
-- KEYS[5]: the task metadata prefix
-- ARGV[1]: emit event bool ("1" or "0")
-- ARGV[2..]: specific job IDs to release. If empty, release everything in KEYS[1].
-- Returns: number of jobs reenqueued
local worker_inflight_set = KEYS[1]
local worker_name = ARGV[1]

local emit_events = ARGV[2] == "1"

local job_ids = {}
for i = 2, #ARGV do
    table.insert(job_ids, ARGV[i])
end

local reenqueued = 0

if #job_ids > 0 then
    -- Release only the specified jobs
    for _, job_id in ipairs(job_ids) do
        local removed = redis.call("srem", worker_inflight_set, job_id)
        if removed == 1 then
            redis.call("rpush", KEYS[2], job_id)

            local meta_key = KEYS[5] .. ":" .. job_id
            redis.call("hset", meta_key, "status", "Pending")
            redis.call("hdel", meta_key, "locked_at", "locked_by")
            if emit_events then
                redis.call("publish", "tasks:" .. KEYS[2] .. ':available', job_id)
            end
            reenqueued = reenqueued + 1
        end
    end
else
    -- Release everything currently held by this worker
    local all_jobs = redis.call("smembers", inflight_set)
    reenqueued = #all_jobs

    if reenqueued > 0 then
        redis.call("rpush", KEYS[2], unpack(all_jobs))
        redis.call("del", worker_inflight_set)

        for _, job_id in ipairs(all_jobs) do
            local meta_key = KEYS[5] .. ":" .. job_id
            redis.call("hset", meta_key, "status", "Pending")
            redis.call("hdel", meta_key, "locked_at", "locked_by")
            if emit_events then
                redis.call("publish", "tasks:" .. KEYS[2] .. ':available', job_id)
            end
        end
    end
end

if reenqueued > 0 then
    redis.call("del", KEYS[3])
    redis.call("lpush", KEYS[3], 1)
end

return reenqueued
