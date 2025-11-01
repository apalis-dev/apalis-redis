-- KEYS[1]: the task data hash
-- KEYS[2]: the metadata prefix (e.g. "task_meta")
-- ARGV[1]: the task ID

-- Returns: { [task_data_array], [metadata_array] }

local task_id = ARGV[1]
if not task_id then
  return redis.error_reply("missing task ID")
end

-- Get the task data
local task_data = redis.call("hmget", KEYS[1], task_id)
if not task_data or not task_data[1] then
  return redis.status_reply("not_found")
end

-- Build metadata array
local meta_key = KEYS[2] .. ":" .. task_id
local fields = redis.call("hgetall", meta_key)

-- Prepend the task_id for consistent format
table.insert(fields, 1, task_id)

return { task_data, { fields } }
