-- Remove permit from assigned
redis.call('lrem', KEYS[2], -1, ARGV[1]);
-- Add permit to available
redis.call('lpush', KEYS[1], ARGV[1]);
-- pop backlogged
local backlogged = redis.call('rpop', KEYS[3]);
-- push backlogged to front of task queue
if (backlogged) then
	redis.call('rpush', KEYS[4], backlogged);
end
