-- Keys: jsonKey taskQueueKey notifyChannelKey task
-- Args: id field serializedValue timestamp [... context args]

local function doSet()
    local prev = redis.pcall('json.get', KEYS[1], ARGV[1] .. '.' .. ARGV[2]);
    if (prev ~= nil and prev ~=false and prev.err ~=nil and prev.err == "ERR key '" .. ARGV[2] .. "' does not exist at level 1 in path") then
        prev = 'null';
    end
    redis.call('json.set', KEYS[1], ARGV[1] .. '.' .. ARGV[2], ARGV[3]);
    return prev;
end
local prev = doSet();
if (prev ~= ARGV[3]) then
    local changedValue = '{"id": "' .. ARGV[1] .. '", "field":"' .. ARGV[2] .. '", "timestamp":' .. ARGV[4] .. ', "previous":' .. prev .. ',"current":' .. ARGV[3] .. '}';
    --redis.call('publish', KEYS[3], changedValue);
    redis.call('hmset', KEYS[4], 'routingKey', 'state-changedValue.' .. KEYS[1] .. '.' .. ARGV[2], 'params', changedValue);

    -- Push extra args into metadata
    -- for i=5,#ARGV,2 do
    --     redis.call('hset', KEYS[4], ARGV[i], ARGV[i+1]);
    -- end 
    redis.call('rpush', KEYS[2], KEYS[4]);
end
if (prev == 'null') then
    return nil;
else
    return prev;
end