-- KEYS[1]: active jobs list
-- KEYS[2]: done jobs zset/set
-- KEYS[3]: dead jobs zset/set
-- KEYS[4]: inflight set
-- ARGV[1]: current timestamp (seconds)
-- Returns: JSON array of Statistic objects
--
-- Done/dead are sorted sets in new deployments and plain sets in legacy ones;
-- counts work for both, time windows are only meaningful for sorted sets.

local now = tonumber(ARGV[1])

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

local function window_count(key, min, max)
    if key_type(key) == 'zset' then
        return redis.call('ZCOUNT', key, min, max)
    end
    return 0
end

local function latest_score(key)
    if key_type(key) == 'zset' then
        local recent = redis.call('ZREVRANGE', key, 0, 0, 'WITHSCORES')
        if #recent > 0 then
            return tonumber(recent[2])
        end
    end
    return 0
end

local active_count = redis.call("LLEN", KEYS[1])
local done_count = state_count(KEYS[2])
local failed_count = state_count(KEYS[3])
local inflight_count = redis.call("SCARD", KEYS[4])

local total_jobs = active_count + done_count + failed_count + inflight_count
local success_rate = 0
local failure_rate = 0
if (done_count + failed_count) > 0 then
    success_rate = (done_count / (done_count + failed_count)) * 100
    failure_rate = (failed_count / (done_count + failed_count)) * 100
end

local one_hour_ago = now - 3600
local day_ago = now - 86400
local seven_days_ago = now - (7 * 86400)

local done_1h = window_count(KEYS[2], one_hour_ago, now)
local failed_1h = window_count(KEYS[3], one_hour_ago, now)
local done_24h = window_count(KEYS[2], day_ago, now)
local failed_24h = window_count(KEYS[3], day_ago, now)
local failed_7d = window_count(KEYS[3], seven_days_ago, now)

local total_1h = done_1h + failed_1h
local total_24h = done_24h + failed_24h
local success_rate_24h = 0
if total_24h > 0 then
    success_rate_24h = (done_24h / total_24h) * 100
end

local avg_jobs_per_hour_24h = total_24h / 24

local most_recent_ts = 0
local recent_done = latest_score(KEYS[2])
local recent_dead = latest_score(KEYS[3])
if recent_done > most_recent_ts then
    most_recent_ts = recent_done
end
if recent_dead > most_recent_ts then
    most_recent_ts = recent_dead
end

local stats = {}
local function push_stat(title, stat_type, value, priority)
    table.insert(stats, string.format(
        '{"title":"%s","stat_type":"%s","value":"%s","priority":%d}',
        title, stat_type, value, priority
    ))
end

push_stat("RUNNING_JOBS", "Number", inflight_count, 1)
push_stat("PENDING_JOBS", "Number", active_count, 2)
push_stat("FAILED_JOBS", "Number", failed_count, 3)
push_stat("DONE_JOBS", "Number", done_count, 4)
push_stat("TOTAL_JOBS", "Number", total_jobs, 5)
push_stat("SUCCESS_RATE", "Percentage", string.format("%.2f", success_rate), 6)
push_stat("FAILURE_RATE", "Percentage", string.format("%.2f", failure_rate), 7)
push_stat("MOST_RECENT_JOB", "Timestamp", tostring(most_recent_ts), 8)
push_stat("JOBS_PAST_HOUR", "Number", total_1h, 9)
push_stat("JOBS_PAST_24_HOURS", "Number", total_24h, 10)
push_stat("FAILED_JOBS_PAST_7_DAYS", "Number", failed_7d, 11)
push_stat("SUCCESS_RATE_PAST_24H", "Percentage", string.format("%.2f", success_rate_24h), 12)
push_stat("AVG_JOBS_PER_HOUR_PAST_24H", "Decimal", string.format("%.2f", avg_jobs_per_hour_24h), 13)

return '[' .. table.concat(stats, ',') .. ']'
