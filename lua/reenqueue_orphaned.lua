-- KEYS[1]: the worker set
-- KEYS[2]: the active job list
-- KEYS[3]: the signal list
-- ARGV[1]: the timestamp before which a worker is considered expired
-- ARGV[2]: (optional) restrict to this specific worker only
-- Returns: number of jobs processed
local workers
if ARGV[2] then
    -- Only consider this worker if it is in fact expired
    local score = redis.call("zscore", KEYS[1], ARGV[2])
    if score and tonumber(score) <= tonumber(ARGV[1]) then
        workers = {ARGV[2]}
    else
        workers = {}
    end
else
    workers = redis.call("zrangebyscore", KEYS[1], 0, ARGV[1])
end

redis.replicate_commands()

local processed = 0

for _, worker in ipairs(workers) do
    local jobs = redis.call("smembers", worker)
    local count = table.getn(jobs)

    if count > 0 then
        redis.call("rpush", KEYS[2], unpack(jobs))
        redis.call("del", worker)
        processed = processed + count
    end

    redis.call("zrem", KEYS[1], worker)
end

if processed > 0 then
    redis.call("del", KEYS[3])
    redis.call("lpush", KEYS[3], 1)
end

return processed
