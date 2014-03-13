-- 
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

local function LEXISTS(rec, bin)
	if aerospike:exists(rec)
		and rec[bin] ~= nil 
			and type(rec) == "userdata" then
		return true
	else
		return false
	end
end

local function UPDATE(rec)
	if aerospike:exists(rec) then
		aerospike:update(rec)
	else
		aerospike:create(rec)
	end
end


function LINDEX (rec, bin, index)
	if (LEXISTS(rec, bin)) then
		local l = rec[bin]
		return list.index(l, index)
	else
		return nil; 
	end
end

function LINSERT (rec, bin, pivot, value)
	if (LEXISTS(rec, bin)) then
		local l     = rec[bin]
		local index = 0;
		for v in list.iterator(l) do
			index = index + 1
			if (v == pivot) then
				break;
			end
		end 
		local new_l = list.take(l, index)
		list.append(new_l, value)
		list.append(new_l, list.drop(l, index)) 
		rec[bin] = new_l;
		UPDATE(rec)
	end
end

function LLEN (rec, bin)
	if (LEXISTS(rec, bin)) then
		return list.size(rec[bin])
	else
		return 0;
	end
end

function LPOP (rec, bin, count)
	if (LEXISTS(rec, bin)) then
		local l = rec[bin];
		local new_l = list.drop(l, count)
		rec[bin] = new_l;
		UPDATE(rec);
		return list.take(l, count);
	else
		return nil;
	end
end

function LPUSH (rec, bin, value)
	local l = rec[bin];
	if (l == nil) then
		l = list();
	end
	list.prepend(l, value)
	rec[bin] = l;
	UPDATE(rec)
end

function LPUSHX (rec, bin, value)
	if (LEXISTS(rec)) then
		LPUSH(rec, bin, value)
	end
end

function LRANGE (rec, bin, start, stop)
	if (LEXISTS(rec, bin)) then
		local l = rec[bin];
		local new_l = list.take(l, stop + 1)
		if (start <= 0) then
			start = 1
		end
		return list.drop(new_l, start);
	end
end

function LREM (rec, bin, count)
	if (LEXISTS(rec, bin)) then
		local l = rec[bin];
		list.drop(l, count);
		rec[bin] = l;
		UPDATE(rec);
	end;
end

function LSET (rec, bin, index, value)
	if (LEXISTS(rec, bin)) then
		local l = rec[bin];
		list.newindex(l, index, value);
		rec[bin] = l;
		UPDATE(rec);
	end;
end

function LTRIM (rec, bin, start, stop)
	if (LEXISTS(rec, bin)) then
		local l = rec[bin];
		local pre_list  = list.take(l, start)
		local post_list = list.drop(l, stop)
		for value in list.iterator(post_list) do
			list.append(pre_list, value)
		end
		rec[bin] = pre_list;
		UPDATE(rec);
	end;
end

function RPOP (rec, bin, count)
	if (LEXISTS(rec, bin)) then
		local l     = rec[bin];
		local result_list = nil;
		if (list.size(l) <= count) then
			rec[bin] = nil
			result_list = rec[bin];
		else
        	local start = list.size(l) - count;
			local result_list = list.drop(l, index)
			rec[bin] = list.take(l, index)
		end
		UPDATE(rec)
		return result_list
	end
end

function RPOPLPUSH (rec, bin1, bin2)
end

function RPUSH (rec, bin, value)
	local l = rec[bin];
	if (l == nil) then
		l = list()
	end
	list.append(l, value)
	rec[bin] = l;
	UPDATE(rec)
end

function RPUSHX (rec, bin, value)
	if (LEXISTS(rec,bin)) then
		RPUSH(rec, bin, value)
	end
end
