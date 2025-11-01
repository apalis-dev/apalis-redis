-- KEYS[1]: the task data hash (e.g. "task:data")
-- KEYS[2]: the metadata prefix (e.g. "task_meta")
-- KEYS[3]: the queue list (e.g. "queue:email:active")
-- ARGV[1]: status filter (or empty string for no filter)
-- ARGV[2]: page number (1-based)
-- ARGV[3]: page size

local status_filter = ARGV[1]
local page = tonumber(ARGV[2])
local page_size = tonumber(ARGV[3])

if not page or not page_size then
  return redis.error_reply("missing pagination parameters")
end

local start = (page - 1) * page_size
local stop = start + page_size - 1

-- Get task IDs for the requested page
local task_ids = redis.call("lrange", KEYS[3], start, stop)
if not task_ids or #task_ids == 0 then
  return { {}, {} }
end

local job_data_list = {}
local meta_list = {}

for _, task_id in ipairs(task_ids) do
  local meta_key = KEYS[2] .. ":" .. task_id
  local meta_fields = redis.call("hgetall", meta_key)

  if not status_filter or status_filter == "" then
    local data = redis.call("hmget", KEYS[1], task_id)
    table.insert(job_data_list, data[1])
    table.insert(meta_fields, 1, task_id)
    table.insert(meta_list, meta_fields)
  else
    -- Filter by status if provided
    local status = redis.call("hget", meta_key, "status")
    if status == status_filter then
      local data = redis.call("hmget", KEYS[1], task_id)
      table.insert(job_data_list, data[1])
      table.insert(meta_fields, 1, task_id)
      table.insert(meta_list, meta_fields)
    end
  end
end

return { job_data_list, meta_list }
