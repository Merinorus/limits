local expiry = tonumber(ARGV[1])
local current_ttl = redis.call('pttl', KEYS[2])

if current_ttl > 0 and current_ttl < expiry * 1000 then
    -- Current window time elapsed, move the counter to the previous window.
    redis.call('rename', KEYS[2], KEYS[1])
    redis.call('set', KEYS[2], 0, 'PX', current_ttl + expiry * 1000)
end

local previous_counter = redis.call('get', KEYS[1])
local previous_ttl = redis.call('pttl', KEYS[1])
local current_counter = redis.call('get', KEYS[2])
current_ttl = redis.call('pttl', KEYS[2])

return {previous_counter, previous_ttl, current_counter, current_ttl}
