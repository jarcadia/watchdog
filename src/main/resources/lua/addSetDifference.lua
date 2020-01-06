redis.call('sadd', KEYS[2], unpack(ARGV));
local inter = redis.call('sinter', KEYS[1], KEYS[2])
if (#inter == 0) then
	redis.call('sadd', KEYS[1], unpack(ARGV));
end
redis.call('del', KEYS[2]);
return inter;