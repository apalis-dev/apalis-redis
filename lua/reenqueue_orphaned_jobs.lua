-- KEYS[1]: the worker set
-- KEYS[2]: the active job list
-- KEYS[3]: the signal list

-- ARGV[1]: the timestamp before which a worker is considered expired

-- Returns: number of jobs processed

-- Find expired workers
local workers = redis.call("zrangebyscore", KEYS[1], 0, ARGV[1])
redis.replicate_commands()

local processed = 0

-- Pull jobs from the worker's inflight set and reschedule all jobs
for _,worker in ipairs(workers) do
  local jobs = redis.call("smembers", worker)
  local count = table.getn(jobs)

  -- Push any orphaned jobs on to the message list
  if count > 0 then
    redis.call("rpush", KEYS[2], unpack(jobs))
    redis.call("del", worker)
    processed = processed + count
  end

  -- Delete the worker since all of its jobs have been rescheduled
  redis.call("zrem", KEYS[1], worker)
end

if processed > 0 then
  -- Signal that there are jobs in the queue
  redis.call("del", KEYS[3])
  redis.call("lpush", KEYS[3], 1)
end

return processed
