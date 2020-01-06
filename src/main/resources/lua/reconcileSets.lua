-- Keys: existingSet tempSet
redis.call('sadd', KEYS[2], unpack(ARGV));
local result = redis.call('sdiff', KEYS[1], KEYS[2]);
if (#result == 0) then
	return '0';
else
	table.insert(result, 1,  tostring(#result));
	local added = redis.call('sdiff', KEYS[2], KEYS[1]);
	for i, v in pairs(added) do
		table.insert(result, v);
	end
	redis.call('rename', KEYS[2], KEYS[1]);
	return result;
end
