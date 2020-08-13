-- Perform checked set on JSON object, create task on changedValue
-- Keys: jsonKey, taskQueue

local function doSet()
    local prev = redis.pcall('json.get', KEYS[1], ARGV[1] .. '.' .. ARGV[2]);
    if (prev ~= nil and prev ~=false and prev.err ~=nil) then
        if (prev.err == "ERR key '" .. ARGV[1] .. "' does not exist at level 0 in path") then 
            redis.call('json.set', KEYS[1], ARGV[1], '{"' .. ARGV[2] .. '":' .. ARGV[3] .. '}');
            return nil;
        elseif (prev.err == "ERR key '" .. ARGV[2] .. "' does not exist at level 1 in path") then
            redis.call('json.set', KEYS[1], ARGV[1] .. '.' .. ARGV[2], ARGV[3]);
            return nil;
        else
            return prev;
        end
    end
    local res = redis.pcall('json.set', KEYS[1], ARGV[1] .. '.' .. ARGV[2], ARGV[3]);
    if (res.err == nil) then
        return prev;
    elseif (res.err == 'ERR new objects must be created at the root') then
        redis.call('json.set', KEYS[1], '.', '{"'..ARGV[1] .. '":{"' .. ARGV[2] .. '":' .. ARGV[3] .. '}}');
        return nil;
    else
        -- return the unrecognized error
        return res;
    end
end
local prev = doSet();
if (prev ~= ARGV[3]) then
    local changedValue = '{"id": "' .. ARGV[1] .. '", "field":"' .. ARGV[2] .. '", "timestamp":' .. ARGV[4] .. ', previous":' .. prev .. ',"current":' .. ARGV[3] .. '}'
    redis.call('hmset', KEYS[3], 'routingKey', ARGV[5], 'params', changedValue);
    redis.call('rpush', KEYS[2], KEYS[3]);
end
return prev;