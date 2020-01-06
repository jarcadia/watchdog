local res = {};
for i=1,#ARGV do
	table.insert(res, ARGV[i]);
	table.insert(res, redis.call('json.get', KEYS[1], ARGV[i]));
end
return res;