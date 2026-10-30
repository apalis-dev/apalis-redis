-- KEYS: none
-- ARGV[1]: current timestamp
-- ARGV[2...]: queue names
-- Returns: JSON array of Statistic objects
--
-- Terminal state keys (done/dead) are sorted sets scored by completion time in
-- new deployments and plain sets in older ones. Counts work for both shapes;
-- time-window metrics are only available for sorted sets.

local now = tonumber(ARGV[1])
local queues = {}
for i = 2, #ARGV do
    table.insert(queues, ARGV[i])
end

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

local total_active = 0
local total_done = 0
local total_failed = 0
local total_inflight = 0
local total_1h = 0
local total_24h = 0
local total_failed_7d = 0
local done_24h = 0
local most_recent_ts = 0

local one_hour_ago = now - 3600
local day_ago = now - 86400
local seven_days_ago = now - (7 * 86400)

for _, q in ipairs(queues) do
    local active = q .. ":active"
    local done = q .. ":done"
    local dead = q .. ":dead"
    local inflight = q .. ":inflight"

    local active_count = redis.call("LLEN", active)
    local done_count = state_count(done)
    local failed_count = state_count(dead)
    local inflight_count = redis.call("SCARD", inflight)

    total_active = total_active + active_count
    total_done = total_done + done_count
    total_failed = total_failed + failed_count
    total_inflight = total_inflight + inflight_count

    local done_1h = window_count(done, one_hour_ago, now)
    local failed_1h = window_count(dead, one_hour_ago, now)
    local queue_done_24h = window_count(done, day_ago, now)
    local queue_failed_24h = window_count(dead, day_ago, now)
    done_24h = done_24h + queue_done_24h

    total_1h = total_1h + done_1h + failed_1h
    total_24h = total_24h + queue_done_24h + queue_failed_24h
    total_failed_7d = total_failed_7d + window_count(dead, seven_days_ago, now)

    local recent_done = latest_score(done)
    local recent_dead = latest_score(dead)
    if recent_done > most_recent_ts then
        most_recent_ts = recent_done
    end
    if recent_dead > most_recent_ts then
        most_recent_ts = recent_dead
    end
end

local total_jobs = total_active + total_done + total_failed + total_inflight
local success_rate = 0
local failure_rate = 0
if (total_done + total_failed) > 0 then
    success_rate = (total_done / (total_done + total_failed)) * 100
    failure_rate = (total_failed / (total_done + total_failed)) * 100
end

local success_rate_24h = 0
if total_24h > 0 then
    success_rate_24h = (done_24h / total_24h) * 100
end

local avg_jobs_per_hour_24h = total_24h / 24

local stats = {}
local function push_stat(title, stat_type, value, priority)
    table.insert(stats, string.format(
        '{"title":"%s","stat_type":"%s","value":"%s","priority":%d}',
        title, stat_type, value, priority
    ))
end

push_stat("RUNNING_JOBS", "Number", total_inflight, 1)
push_stat("PENDING_JOBS", "Number", total_active, 2)
push_stat("FAILED_JOBS", "Number", total_failed, 3)
push_stat("DONE_JOBS", "Number", total_done, 4)
push_stat("TOTAL_JOBS", "Number", total_jobs, 5)
push_stat("SUCCESS_RATE", "Percentage", string.format("%.2f", success_rate), 6)
push_stat("FAILURE_RATE", "Percentage", string.format("%.2f", failure_rate), 7)
push_stat("MOST_RECENT_JOB", "Timestamp", tostring(most_recent_ts), 8)
push_stat("JOBS_PAST_HOUR", "Number", total_1h, 9)
push_stat("JOBS_PAST_24_HOURS", "Number", total_24h, 10)
push_stat("FAILED_JOBS_PAST_7_DAYS", "Number", total_failed_7d, 11)
push_stat("SUCCESS_RATE_PAST_24H", "Percentage", string.format("%.2f", success_rate_24h), 12)
push_stat("AVG_JOBS_PER_HOUR_PAST_24H", "Decimal", string.format("%.2f", avg_jobs_per_hour_24h), 13)

return '[' .. table.concat(stats, ',') .. ']'
