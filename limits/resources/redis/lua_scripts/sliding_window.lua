local previous_count = redis.call('get', KEYS[1])
local previous_ttl = redis.call('pttl', KEYS[1])
local current_count = redis.call('get', KEYS[2])
local current_ttl = redis.call('pttl', KEYS[2])

return {previous_count, previous_ttl, current_count, current_ttl}
