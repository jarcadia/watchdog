local permit = redis.call('rpoplpush', KEYS[1], KEYS[2]);
if (permit) then
    return tonumber(permit);
else
    redis.call('lpush', KEYS[3], KEYS[4]);
    return nil;
end