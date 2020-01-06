local val = redis.pcall('json.get', KEYS[1], ARGV[1] .. '.' .. ARGV[2]);
if (val ~= nil and val ~=false and val.err ~=nil) then
    if (val.err == "ERR key '" .. ARGV[1] .. "' does not exist at level 0 in path") then 
        return nil;
    elseif (val.err == "ERR key '" .. ARGV[2] .. "' does not exist at level 1 in path") then
        return nil;
    else
        return val;
    end
end


