-- Retry task after failure
-- Keys: scheduledZSetKey taskId nextId
-- Args: nextTimestamp
redis.call('restore', KEYS[3], 0, redis.call('dump', KEYS[2]));
redis.call('hincrby', KEYS[3], 'attempt', 1);
redis.call('hset', KEYS[3], 'targetTimestamp', ARGV[1]);
redis.call('hdel', KEYS[3], 'recurKey', 'authorityKey', 'recurInterval');
redis.call('zadd', KEYS[1], ARGV[1], KEYS[3])