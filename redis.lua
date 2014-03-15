-- Redis API clone for Aerospike 
--
-- This file implements the Aerospike UDF for the LIST and HASH redis API. In aerospike 
-- a bin is a list or a map. A record can have mulitple bins hence many lists and maps
-- in it. This also means in addition to the key bin name also needs to be specified 
-- while performing these operation. 
--
-- NB: First parameter in the UDF function definition is record which is created by 
--     system everything else is argument which needs to be passed in, including the
--     bin name
--
-- Usage
-- ====
--
-- aql > register module './redis.lua'
-- aql > execute redis.LPUSH('tweets", "my simple tweet") where PK = '1'
-- aql > execute redis.LRANGE("tweets", 1, 2) where PK = '1'
-- 
-- TODO
-- =========
-- Transform from normal data type to large data type beyond certain threshold
-- Few command may not support all options
--
--
-- ###########################################
-- LIST : See http://redis.io/commands#list for detail of API
--
-- NB: Does not support multi key (RPOPLPUSH on multiple bin is) and blocking
--     operation is not supported
-- ############################################

local function EXISTS(rec, bin)
	if aerospike:exists(rec)
		and rec[bin] ~= nil 
			and type(rec) == "userdata" then
		return true
	end
	return false
end

local function UPDATE(rec)
	if aerospike:exists(rec) then
		aerospike:update(rec)
	else
		aerospike:create(rec)
	end
end

function LINDEX (rec, bin, index)
	if (EXISTS(rec, bin)) then
		l = rec[bin]
		if (index >= 0) then
			return l[index+1]
		else
			return l[#l + 1 + index]
		end
	end
	return nil 
end

function LINSERT (rec, bin, pos, pivot, value)
	if (EXISTS(rec, bin)) then
		local l     = rec[bin]
		local new_l = list()
		local inserted = 0
		for v in list.iterator(l) do
			if (v == pivot) and inserted ~= 1 then
				if (pos == "BEFORE") then
					list.append(new_l, value)
					list.append(new_l, v)
				elseif (pos == "AFTER") then
					list.append(new_l, v)
					list.append(new_l, value)
				else 
					return -1
				end
				inserted = 1
			else 
				list.append(new_l, v)
			end
		end 
		if (inserted == 1) then
			rec[bin] = new_l
			local length = #rec[bin]
			UPDATE(rec)
			return length
		else
			return -1
		end
	end
	return -1
end

function LLEN (rec, bin)
	if (EXISTS(rec, bin)) then
		return #rec[bin]
	end
	return 0
end

function LPOP (rec, bin, count)
	if (EXISTS(rec, bin)) then
		local l = rec[bin]
		local new_l = list.drop(l, count)
		rec[bin] = new_l
		UPDATE(rec)
		return list.take(l, count)
	end
	return nil
end

function LPUSH (rec, bin, value)
	local l = rec[bin]
	if (l == nil) then
		l = list()
	end
	list.prepend(l, value)
	rec[bin] = l
	local length = #l
	UPDATE(rec)
	return length
end

function LPUSHX (rec, bin, value)
	if (EXISTS(rec)) then
		return LPUSH(rec, bin, value)
	end
	return -1
end

function LPUSHALL (rec, bin, value_list)
	local l = rec[bin]
	if (l == nil) then
		l = list()
	end
	for value in list.iterator(value_list) do
		list.prepend(l, value)
	end
	rec[bin] = l
	local length = #l
	UPDATE(rec)
	return length
end

function LRANGE (rec, bin, start, stop)
	if (EXISTS(rec, bin)) then
		local l     = rec[bin]
		if (start < 0) then
			start = #l + start + 1 
		end
		if (stop < 0) then
			stop = #l + stop + 1
		end

		if (start >= stop) then
			return list()
		end

		local new_l = list.take(rec[bin], stop)
		if (start > 0) then
			return list.drop(new_l, start)
		else 
			return new_l
		end
	end
	return list()
end

function LSET (rec, bin, index, value)
	if (EXISTS(rec, bin)) then
		local l = rec[bin]
		l[index] = value
		rec[bin] = l
		UPDATE(rec)
	end
end

function LREM (rec, bin, count, value)
	if (EXISTS(rec, bin)) then
		l = rec[bin]
		if (count == 0) then
			count = #l
		end
		for v in list.iterator(l) do
			if (count == 0) then	
				local l = rec[bin]
				list.drop(l, count)
			end
		end
		rec[bin] = l
		UPDATE(rec)
	end
	return 0
end

function LTRIM (rec, bin, start, stop)
	if (EXISTS(rec, bin)) then
		local l = rec[bin]
		
		if (start < 0) then
			start = #l + start + 1 
		end

		if (stop < 0) then
			stop = #l + stop + 1
		end

		if (start >= stop) then
			return "-Invalid Range"
		end

		local pre_list  = list.take(l, start)
		local post_list = list.drop(l, stop)
		for value in list.iterator(post_list) do
			list.append(pre_list, value)
		end
		rec[bin] = pre_list
		UPDATE(rec)
		return "+OK"
	end
	return "+Key/Bin Not Found"
end

function RPOP (rec, bin, count)
	if (EXISTS(rec, bin)) then
		local l     = rec[bin]
		local result_list = nil
		if (#l <= count) then
			rec[bin] = nil
			result_list = rec[bin]
		else
        	local start = #l - count
			local result_list = list.drop(l, index)
			rec[bin] = list.take(l, index)
		end
		UPDATE(rec)
		if (result_list ~= nil) then
			return result_list
		else
			return list()
		end
	end
	return nil
end

-- Does not support multikey operation only multi bin
function RPOPLPUSH (rec, bin1, bin2, count)
	if (EXISTS(rec, bin1)) then
		local l1  = rec[bin1]
		local l2  = rec[bin2]

		if (count == nil) then
			count = 1
		end

		if (count > #l1) then
			count = #l1
		end
		rec[bin1]     = list.take(l1, #l1 - count)
		local value_l = list.drop(l1, #l1 - count)

		if (l2 == nil) then
			l2 = list()
		end
		
		if (value_l ~= nil) then
			for value in list.iterator(value_l) do
				list.append(l2, value)
			end
			rec[bin2] = l2 
		end
		UPDATE(rec)
		return value_l
	end	
	return list()
end

function RPUSH (rec, bin, value)
	local l = rec[bin]
	if (l == nil) then
		l = list()
	end
	list.append(l, value)
	rec[bin] = l
	local length = #l
	UPDATE(rec)
	return length
end

function RPUSHX (rec, bin, value)
	if (EXISTS(rec,bin)) then
		return RPUSH(rec, bin, value)
	end
	return -1
end



-- ###########################################
-- HASH : See http://redis.io/commands#hash for detail of API
--
-- NB: HINCRBYFLOAT not supported 
-- ############################################

function HDEL(rec, bin, field) 
	if (EXISTS(rec, bin)) then
		m = rec[bin]
		m[field] = nil 
		rec[bin] = m
		UPDATE(rec)
		return 1
	end
	return 0
end

function HEXISTS(rec, bin, field)
	if (EXISTS(rec, bin)) and rec[bin][field] ~= nil then
		return 1
	else
		return 0
	end
end

function HGET(rec, bin, field)
	if (EXISTS(rec, bin)) then
		return rec[bin][field]
	end
	return nil 
end

function HGETALL(rec, bin)
	local l = list()
	if (EXISTS(rec, bin)) then
		for k,v in map.iterator(rec[bin]) do
			list.append(l, k);
			list.append(l, v);
		end
	end
	return l
end

function HINCRBY(rec, bin, field, increment)
	local value = 0
	local m     = map()
	if (EXISTS(rec, bin)) then
		if (rec[bin][field] ~= nil) then
			value = rec[bin][field]
		end
		m  = rec[bin]
		if (type(value) == "number") then
			m[field] = value + increment
		else
			m[field] = increment;
		end
	end
	rec[bin] = m
	UPDATE(rec)
	return value + increment
end

function HKEYS(rec, bin) 
	if (EXISTS(rec, bin)) then
		local keys = list()
		for k in map.keys(rec[bin]) do
			list.append(keys,k)
		end
		return keys
	end
	return list()
end

function HVALS(rec, bin)
	if (EXISTS(rec, bin)) then
		local vals = list()
		for v in map.values(rec[bin]) do
			list.append(vals, v)
		end
		return vals
	end
	return list()
end

function HLEN(rec, bin)
	if (EXISTS(rec, bin)) then
		return #rec[bin]
	end
	return 0
end


function HMGET(rec, bin, field_list)
	local exist = 0
	if (EXISTS(rec, bin)) then
		exist = 1
	end
	local res_list = list()

	for field in list.iterator(field_list) do
		if exist and (rec[bin][field] ~= nil) then
			list.append(res_list, rec[bin][field])
		else
			list.append(res_list, nil);
		end
	end
	return res_list
end

function HMSET(rec, bin, field_value_map)
	local res_list = list()
	m = rec[bin]
	if (m == nil) then
		m = map()
	end
	for k,v in map.iterator(field_value_map) do
		m[k] = v
	end	
	rec[bin] = m
	UPDATE(rec)
	return "OK"
end

function HSET(rec, bin, field, value)
	local created = 0
	if (EXISTS(rec, bin)) then
		created = 0
	else	
		created = 1
	end
	local m = rec[bin]
	if (m == nil) then
		m = map()
	end
	if (m[field] == nil) then
		created = 1
	end
	m[field] = value
	rec[bin] = m
	UPDATE(rec)
	return created
end

function HSETNX(rec, bin, field, value)
	local created = 0
	if (EXISTS(rec, bin)) then
		created = 0
	else
		created = 1
	end
	local m = rec[bin]
	if (m == nil) then
		m = map()
	end
	if (m[field] == nil) then
		created = 1
	else 
		return 0
	end
	m[field] = value
	rec[bin] = m
	UPDATE(rec)
	return created
end


-- Does not support sophistication of entire API. Only basic, scan with offset and count
function HSCAN(rec, bin, offset, count)
	if (count == nil) then
		count = 10;
	end
	if (EXISTS(rec, bin)) then
		local l = list()
		local new_offset = 0;
		for v in map.values(rec[bin]) do
			new_offset = new_offset + 1
			if (offset > 0) then
				offset = offset - 1
			else 
				list.append(l, v)
				count = count - 1;
				if (count == 0) then
					break;
				end
			end
		end
		local res_list = list()
		list.append(res_list, new_offset)
		list.append(res_list, l)
		return res_list
	end
end
