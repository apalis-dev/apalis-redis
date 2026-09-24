-- KEYS[1]: this worker's inflight set
-- KEYS[2]: the active workers sorted set
-- KEYS[3]: the task metadata prefix
-- KEYS[4]: the active task list
--
-- ARGV[1]: worker name
-- ARGV[2]: current timestamp
-- ARGV[3]: emit event bool ("1" or "0")
-- ARGV[4..]: task_ids to renew
--
-- Returns: number of leases renewed (== #task_ids, or the call errors)
local worker_name = ARGV[1]
local worker_inflight_set = KEYS[1]
local now = ARGV[2]
local emit_events = ARGV[3] == "1"

-- Confirm the worker itself is registered/alive
local registered = redis.call("zscore", KEYS[2], worker_name)
if not registered then
    error("Cant renew leases: worker not registered")
end

local task_ids = {}
for i = 4, #ARGV do
    table.insert(task_ids, ARGV[i])
end

if #task_ids == 0 then
    return 0
end

-- Validate first: every task must currently be held by this worker
for _, task_id in ipairs(task_ids) do
    local is_member = redis.call("sismember", worker_inflight_set, task_id)
    if is_member == 0 then
        error("lease not held for task: " .. task_id)
    end
end

-- All validated — now renew
for _, task_id in ipairs(task_ids) do
    local meta_key = KEYS[3] .. ":" .. task_id

    redis.call("hset", meta_key, "locked_at", now)

    if emit_events then
        redis.call("publish", "tasks:" .. KEYS[4] .. ":heartbeat", cjson.encode({
            task_id = task_id,
            worker = worker_name
        }))
    end
end

return #task_ids
