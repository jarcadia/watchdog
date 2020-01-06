local due = redis.call('zrangebyscore', KEYS[1], 0, ARGV[1]);
for i=1, #due do
    redis.call('zrem', KEYS[1], due[i]);
end
return due;
