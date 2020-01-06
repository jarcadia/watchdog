-- Recur a task
-- Keys: taskQueue scheduledTaskZSet recurLockHashKey recurAuthorityHashKey recurKey taskId nextTaskId
-- Args: targetTimestamp, nextTimestamp, authorityKey
-- Returns: true or false

local masterAuthKey = redis.call('hget', KEYS[4], ARGV[1])

if (masterAuthKey == ARGV[4]) then

	-- Setup recurrence 
	redis.call('restore', KEYS[6], 0, redis.call('dump', KEYS[5]));
	redis.call('hset', KEYS[6], 'targetTimestamp', ARGV[3]);
	redis.call('hdel', KEYS[6], 'dependents');
	redis.call('zadd', KEYS[2], ARGV[3], KEYS[6])
	 
	-- Update lock
	redis.call('hset', KEYS[3], ARGV[1], "1");

	return true;

--redis.call('hset', KEYS[4], ARGV[1], ARGV[1]);
else 
	-- this task is no longer the authority for this recurKey
	redis.call('del', KEYS[5]);
	return false;
end
