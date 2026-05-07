-- KEYS[1]: the task data hash
-- KEYS[2]: the active task list
-- KEYS[3]: the signal list
-- KEYS[4]: the metadata prefix (e.g. "task_meta")
-- KEYS[5]: the scheduled set (for delayed tasks)
-- KEYS[6]: the idempotency key set (e.g. "task_idempotency")

-- ARGV: [task_id1, task_data1, attempts1, max_attempts1, metadata_json1, schedule_time1, idempotency_key1,
--        task_id2, task_data2, attempts2, max_attempts2, metadata_json2, schedule_time2, idempotency_key2, ...]

local newly_enqueued = 0
local newly_scheduled = 0

for i = 1, #ARGV, 7 do
  local task_id         = ARGV[i]
  local task_data       = ARGV[i + 1]
  local attempts        = ARGV[i + 2]
  local max_attempts    = ARGV[i + 3]
  local metadata_json   = ARGV[i + 4]
  local schedule_time   = tonumber(ARGV[i + 5])
  local idempotency_key = ARGV[i + 6]  -- empty string "" means no idempotency key

  if task_id and task_data then

    -- If an idempotency key is provided, attempt to claim it.
    -- setnx returns 0 if already exists, meaning this task was already submitted.
    local allowed = true
    if idempotency_key and #idempotency_key > 0 then
      local idem_field = KEYS[6] .. ':' .. idempotency_key
      local claimed = redis.call("setnx", idem_field, task_id)
      if claimed == 0 then
        allowed = false
      else
        -- redis.call("expire", idem_field, 86400)
      end
    end

    if allowed then
      local set = redis.call("hsetnx", KEYS[1], task_id, task_data)

      if set == 1 then
        local meta_key = KEYS[4] .. ':' .. task_id

        -- Base metadata
        local meta = {
          "attempts", tonumber(attempts),
          "max_attempts", tonumber(max_attempts),
          "status", "Pending"
        }

        -- Store the idempotency key in metadata for traceability
        table.insert(meta, "idempotency_key")
        table.insert(meta, idempotency_key)

        -- Merge in any JSON metadata
        if metadata_json and #metadata_json > 0 then
          local ok, decoded = pcall(cjson.decode, metadata_json)
          if ok and type(decoded) == "table" then
            for k, v in pairs(decoded) do
              if type(v) == "table" then
                v = cjson.encode(v)
              end
              table.insert(meta, k)
              table.insert(meta, tostring(v))
            end
          end
        end

        redis.call("hmset", meta_key, unpack(meta))

        if schedule_time and schedule_time > 0 then
          redis.call("zadd", KEYS[5], schedule_time, task_id)
          newly_scheduled = newly_scheduled + 1
        else
          redis.call("rpush", KEYS[2], task_id)
          redis.call("publish", "tasks:" .. KEYS[2] .. ':available', task_id)
          newly_enqueued = newly_enqueued + 1
        end
      end
    end
  end
end

if newly_enqueued > 0 then
  redis.call("del", KEYS[3])
  redis.call("lpush", KEYS[3], 1)
end

return { newly_enqueued, newly_scheduled }
