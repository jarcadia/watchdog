local prev = redis.pcall('json.get', KEYS[1], ARGV[1] .. '.' .. ARGV[2]);
if (prev ~= nil and prev ~=false and prev.err ~=nil) then
    if (prev.err == "ERR key '" .. ARGV[2] .. "' does not exist at level 1 in path") then
        redis.call('json.set', KEYS[1], ARGV[1] .. '.' .. ARGV[2], ARGV[3]);
        return nil;
    else
        return prev;
    end
end
redis.call('json.set', KEYS[1], ARGV[1] .. '.' .. ARGV[2], ARGV[3]);
return prev