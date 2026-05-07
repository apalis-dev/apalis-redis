-- KEYS[1]: the task data hash
-- KEYS[2]: the metadata prefix (e.g. "task_meta")

local terminal_statuses = { Done = true, Failed = true, Killed = true }
local result_hash = KEYS[2] .. ":result"

local deleted = 0

local fields = redis.call("hgetall", KEYS[1])

-- fields is a flat list of [field, value, field, value, ...]
for i = 1, #fields, 2 do
  local task_id = fields[i]
  local meta_key = KEYS[2] .. ':' .. task_id
  local status = redis.call("hget", meta_key, "status")

  if status and terminal_statuses[status] then
    redis.call("hdel", KEYS[1], task_id)
    redis.call("hdel", result_hash, task_id)
    redis.call("del", meta_key)
    deleted = deleted + 1
  end
end

return deleted
