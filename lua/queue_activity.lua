-- KEYS[1]: done jobs key (zset or legacy set)
-- KEYS[2]: dead jobs key (zset or legacy set)
-- ARGV[1]: start of today (unix seconds, UTC)
-- ARGV[2]: seconds in a day
-- Returns: array with the number of completed jobs per day, oldest first.

local function key_type(key)
    local key_type = redis.call('TYPE', key)
    if type(key_type) == 'table' then
        return key_type['ok']
    end
    return key_type
end

local function window_count(key, min, max)
    if key_type(key) == 'zset' then
        return redis.call('ZCOUNT', key, min, max)
    end
    return 0
end

local today = tonumber(ARGV[1])
local day = tonumber(ARGV[2])
local activity = {}

for offset = 6, 0, -1 do
    local start = today - offset * day
    local finish = start + day - 1
    local done = window_count(KEYS[1], start, finish)
    local failed = window_count(KEYS[2], start, finish)
    table.insert(activity, done + failed)
end

return activity
