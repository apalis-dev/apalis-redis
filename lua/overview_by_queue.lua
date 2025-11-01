-- KEYS[1]: active jobs list
-- KEYS[2]: done jobs zset
-- KEYS[3]: failed jobs zset
-- KEYS[4]: inflight set
-- ARGV[1]: current timestamp (seconds)
-- Returns: JSON array of Statistic objects

local now = tonumber(ARGV[1])

local active_count = redis.call("llen", KEYS[1])
local done_count = redis.call("zcard", KEYS[2])
local failed_count = redis.call("zcard", KEYS[3])
local inflight_count = redis.call("scard", KEYS[4])

-- Overall totals
local total_jobs = active_count + done_count + failed_count + inflight_count
local success_rate = 0
local failure_rate = 0
if (done_count + failed_count) > 0 then
    success_rate = (done_count / (done_count + failed_count)) * 100
    failure_rate = (failed_count / (done_count + failed_count)) * 100
end

-- Rolling windows
local one_hour_ago = now - 3600
local day_ago = now - 86400
local seven_days_ago = now - (7 * 86400)

local done_1h = redis.call("zcount", KEYS[2], one_hour_ago, now)
local failed_1h = redis.call("zcount", KEYS[3], one_hour_ago, now)
local done_24h = redis.call("zcount", KEYS[2], day_ago, now)
local failed_24h = redis.call("zcount", KEYS[3], day_ago, now)
local failed_7d = redis.call("zcount", KEYS[3], seven_days_ago, now)

local total_1h = done_1h + failed_1h
local total_24h = done_24h + failed_24h
local success_rate_24h = 0
if total_24h > 0 then
    success_rate_24h = (done_24h / total_24h) * 100
end

local avg_jobs_per_hour_24h = total_24h / 24

-- most recent job timestamp
local recent_done = redis.call("zrevrange", KEYS[2], 0, 0, "WITHSCORES")
local recent_failed = redis.call("zrevrange", KEYS[3], 0, 0, "WITHSCORES")
local most_recent_ts = 0
if #recent_done > 0 then most_recent_ts = tonumber(recent_done[2]) end
if #recent_failed > 0 and tonumber(recent_failed[2]) > most_recent_ts then
    most_recent_ts = tonumber(recent_failed[2])
end

-- Helper to push Statistic JSON
local stats = {}
local function push_stat(title, stat_type, value, priority)
    table.insert(stats, string.format(
        '{"title":"%s","stat_type":"%s","value":"%s","priority":%d}',
        title, stat_type, value, priority
    ))
end

-- Base metrics
push_stat("RUNNING_JOBS", "Number", inflight_count, 1)
push_stat("PENDING_JOBS", "Number", active_count, 2)
push_stat("FAILED_JOBS", "Number", failed_count, 3)
push_stat("DONE_JOBS", "Number", done_count, 4)
push_stat("TOTAL_JOBS", "Number", total_jobs, 5)
push_stat("SUCCESS_RATE", "Percentage", string.format("%.2f", success_rate), 6)
push_stat("FAILURE_RATE", "Percentage", string.format("%.2f", failure_rate), 7)
push_stat("MOST_RECENT_JOB", "Timestamp", tostring(most_recent_ts), 8)

-- Rolling window metrics
push_stat("JOBS_PAST_HOUR", "Number", total_1h, 9)
push_stat("JOBS_PAST_24_HOURS", "Number", total_24h, 10)
push_stat("FAILED_JOBS_PAST_7_DAYS", "Number", failed_7d, 11)
push_stat("SUCCESS_RATE_PAST_24H", "Percentage", string.format("%.2f", success_rate_24h), 12)
push_stat("AVG_JOBS_PER_HOUR_PAST_24H", "Decimal", string.format("%.2f", avg_jobs_per_hour_24h), 13)

return '[' .. table.concat(stats, ',') .. ']'
