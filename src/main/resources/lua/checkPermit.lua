-- Return the queue length for a permit. Negative number indicates how many permits are available, > 0 indicates backlog queue length
-- Keys: available backlog
local available = redis.call('llen', KEYS[1]);
if (available > 0) then
	return available;
else
	return -redis.call('llen', KEYS[2]);
end