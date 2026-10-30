--!df flags=allow-undeclared-keys
-- FIXME: The previous line enables undeclared keys for dragonflydb. This is
-- required because keys get programatically generated in the for loop counting
-- running jobs. However, accessing undeclared keys is considered a bad practice
-- by redis:
-- https://redis.io/docs/latest/commands/eval/#:~:text=Important%3A%20to,in%20the%20database

-- KEYS[1]: the pending jobs set ( aka active job list )
-- KEYS[2]: the worker set
-- KEYS[3]: the dead jobs set
-- KEYS[4]: the failed jobs set
-- KEYS[5]: the success jobs set

local function key_type(key)
    local key_type = redis.call('TYPE', key)
    if type(key_type) == 'table' then
        return key_type['ok']
    end
    return key_type
end

local function state_count(key)
    local kind = key_type(key)
    if kind == 'zset' then
        return redis.call('ZCARD', key)
    elseif kind == 'set' then
        return redis.call('SCARD', key)
    end
    return 0
end

local workers = redis.call("ZRANGEBYSCORE", KEYS[2], 0, "+inf")

local running_count = 0
for _, worker_inflight_set in ipairs(workers) do
    running_count = running_count + redis.call("SCARD", worker_inflight_set)
end

local pending_count = redis.call('LLEN', KEYS[1])
local dead_count = state_count(KEYS[3])
local failed_count = state_count(KEYS[4])
local success_count = state_count(KEYS[5])

return { pending_count, running_count, dead_count, failed_count, success_count }
