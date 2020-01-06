local function contains(list, val)
	for index, value in ipairs(list) do
		if (val == value) then
			return true
		end
	end
	return false
end
local available = redis.call('lrange', KEYS[1], 0, -1);
local assigned = redis.call('lrange', KEYS[2], 0, -1);
local expected = {};
for i=1,ARGV[1],1 do
	expected[i] = tostring(i);
end
for i, permit in ipairs(expected) do
	if (not contains(available, permit) and not contains(assigned, permit)) then
		redis.call('lpush', KEYS[1], permit);
	end
end
for i,permit in ipairs(available) do
	if (not contains(expected, permit)) then
		redis.call('lrem', KEYS[1], 0, permit);
	end
end for i,permit in ipairs(assigned) do
	if (not contains(expected, permit)) then
		return redis.error_reply('Cannot remove permit that is currently in use');
	end
end
return redis.status_reply('OK');