-- ###########################################
-- Redis List API
--
-- See http://redis.io/commands#list for detail of API
--
-- LINDEX      (bin, index)
-- LINSERT     (bin, index)
-- LLEN        (bin)
-- LPOP        (bin, count) 
-- LPUSH       (bin, value)
-- LPUSHX      (bin, value)
-- LRANGE      (bin, start, stop)
-- LREM        (bin, count)
-- LSET        (bin, index, value)
-- LTRIM       (bin, start, stop)
-- RPOP        (bin, count)
-- RPOPLPUSH   (bin1, bin2)
-- RPUSH       (bin, value)
-- RPUSHX      (bin, value)
--
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
		LPUSH(rec, bin, value)
	end
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

-- Todo proper bounds check and adherence to API
function LRANGE (rec, bin, start, stop)
	if (EXISTS(rec, bin)) then
		local l     = rec[bin]
		if (start <= 0) then
			start = 1
		end
		if (stop < 0) then
			stop = #l + 1 - stop
		end
	
		local new_l = list.take(rec[bin], stop)
		return list.drop(new_l, start)
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
		l = rec[bin];
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
		local pre_list  = list.take(l, start)
		local post_list = list.drop(l, stop)
		for value in list.iterator(post_list) do
			list.append(pre_list, value)
		end
		rec[bin] = pre_list
		UPDATE(rec)
	end
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
		return result_list
	end
end

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
		
		info("val %s %s", tostring(value_l), tostring(rec[bin1]));
		if (value_l ~= nil) then
			for value in list.iterator(value_l) do
				list.append(l2, value)
			end
			rec[bin2] = l2 
		end
		UPDATE(rec)
		return value_l
	end	
end

function RPUSH (rec, bin, value)
	local l = rec[bin]
	if (l == nil) then
		l = list()
	end
	list.append(l, value)
	rec[bin] = l
	UPDATE(rec)
end

function RPUSHX (rec, bin, value)
	if (EXISTS(rec,bin)) then
		RPUSH(rec, bin, value)
	end
end



-- ###########################################
-- Redis HASH API
--
-- See http://redis.io/commands#hash for detail of API
--
-- HDEL         (bin, field)
-- HEXISTS      (bin, field)
-- HGET         (bin, field)
-- HGETALL      (bin)
-- HINCRBY      (bin, field, increment)
-- HKEYS        (bin)
-- HLEN         (bin)
-- HMGET        (bin, field_list)
-- HMSET        (bin, map)
-- HSET         (bin, field, value)
-- HSETNX       (bin, field, value)
-- HVALS        (bin)
-- HSCAN        (bin, offset, count)
-- ############################################

function HDEL(rec, bin, field) 
	if (EXISTS(rec, bin)) then
		m = rec[bin]
		m[field] = nil 
		info("LOLAR %s %s", tostring(field), tostring(m[field]))
		rec[bin] = m
		UPDATE(rec)
	end
	return "OK"
end

function HEXISTS(rec, bin, field)
	if (EXISTS(rec, bin)) and rec[bin][field] ~= nil then
		return true
	else
		return false
	end
end

function HGET(rec, bin, field)
	if (EXISTS(rec, bin)) then
		return rec[bin][field]
	else
		return nil 
	end
end

function HGETALL(rec, bin)
	if (EXISTS(rec, bin)) then
		return rec[bin]
	end
end

function HINCRBY(rec, bin, field, increment)
	if (EXISTS(rec, bin)) then
		local value = rec[bin][field]
		local m  = rec[bin]
		if (type(value) == "number") then
			m[field] = value + increment
			rec[bin] = m
		end
		UPDATE(rec)
	end
end

function HKEYS(rec, bin) 
	if (EXISTS(rec, bin)) then
		local keys = list()
		for k in map.keys(rec[bin]) do
			list.append(keys,k)
		end
		return keys
	else
		return nil
	end
end

function HVALS(rec, bin)
	if (EXISTS(rec, bin)) then
		local vals = list()
		for v in map.values(rec[bin]) do
			list.append(vals, v)
		end
		return vals
	else
		return nil
	end
end

function HLEN(rec, bin)
	if (EXISTS(rec, bin)) then
		return #rec[bin]
	end
end


function HMGET(rec, bin, field_list)
	if (EXISTS(rec, bin)) then
		local res_list = list()
		for field in list.iterator(field_list) do
			info ("HMGET %s",tostring(field))
			if (rec[bin][field] ~= nil) then
				list.append(res_list, rec[bin][field])
			end
		end	
		return res_list
	else
		return nil
	end
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
	local m = rec[bin]
	if (m == nil) then
		m = map()
	end
	m[field] = value
	rec[bin] = m
	info("REC %s", tostring(rec[bin]))
	UPDATE(rec)
	return rec[bin]
end

function HSETNX(rec, bin, field, value)
	if (EXISTS(rec, bin)) then
	else
		local m = rec[bin]
		if (m == nil) then
			return
		end
		if (rec[bin][field] ~= nil) then
			rec[bin][field] = value
		end
		UPDATE(rec)
		return value
	end
end


function HSCAN(bin, offset, count)
end
