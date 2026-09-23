-- KEYS[1]: this worker's inflight set
-- KEYS[2]: the active workers sorted set
-- KEYS[3]: the task metadata prefix
-- KEYS[4]: the active jobs list (jobs must currently be queued, not already inflight elsewhere)
-- ARGV[1]: worker name
-- ARGV[2]: current timestamp
-- ARGV[3..]: task_ids to acquire
-- Returns: number of leases acquired (== #task_ids, or the call errors)
local worker_id = KEYS[1]
local worker_name = ARGV[1]
local now = ARGV[2]

-- Confirm the worker itself is registered/alive
local registered = redis.call("zscore", KEYS[2], worker_name)
if not registered then
    error("Cant acquire lease: worker not registered")
end

local task_ids = {}
for i = 3, #ARGV do
    table.insert(task_ids, ARGV[i])
end

if #task_ids == 0 then
    return 0
end

-- Validate first: every task must already be in this worker's inflight set
for _, task_id in ipairs(task_ids) do
    local is_member = redis.call("sismember", worker_id, task_id)
    if is_member == 0 then
        error("task not held by this worker: " .. task_id)
    end
end

-- All validated — now mutate
for _, task_id in ipairs(task_ids) do
    redis.call("lrem", KEYS[4], 1, task_id)
    redis.call("sadd", KEYS[1], task_id)

    local meta_key = KEYS[3] .. ":" .. task_id
    redis.call("hset", meta_key, "locked_at", now, "locked_by", worker_name, "status", "Running")
end

return #task_ids
