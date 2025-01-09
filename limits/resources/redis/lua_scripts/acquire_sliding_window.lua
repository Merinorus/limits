-- Time is in milliseconds in this script: TTL, expiry...

local limit = tonumber(ARGV[1])
local expiry = tonumber(ARGV[2]) * 1000
local amount = tonumber(ARGV[3])

if amount > limit then
    return false
end

local current_ttl = tonumber(redis.call('pttl', KEYS[2]))

if current_ttl > 0 and current_ttl < expiry then

    redis.call('rename', KEYS[2], KEYS[1])
    redis.call('set', KEYS[2], 0, 'PX', current_ttl + expiry)
end

local previous_count = tonumber(redis.call('get', KEYS[1])) or 0
local previous_ttl = tonumber(redis.call('pttl', KEYS[1])) or 0
local current_count = tonumber(redis.call('get', KEYS[2])) or 0
current_ttl = tonumber(redis.call('pttl', KEYS[2])) or 0

if previous_ttl <= 0 then
    previous_ttl = 0
end
if current_ttl <= 0 then
    current_ttl = 0
end

local weighted_count = previous_count * previous_ttl / expiry + current_count

if (weighted_count + amount) > limit then
    return false
end

-- Check if the key exists
if redis.call('exists', KEYS[2]) == 1 then
    redis.call('incrby', KEYS[2], amount)
else
    -- If the current doesn't exist, set the value with twice the expiry time
    redis.call('set', KEYS[2], amount, 'PX', expiry * 2)
end

return true
