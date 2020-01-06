for i=2, #KEYS do
	local remaining = redis.call('hincrby', KEYS[i], 'cdl', -1);
	if (remaining == 0) then
		redis.call('rpush', KEYS[1], KEYS[i]);
	end
end