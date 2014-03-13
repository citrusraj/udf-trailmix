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
-- LMULTIPUSH  (bin, value_list)
-- LMULTIPUSHX (bin, value_list)
-- LRANGE      (bin, start, stop)
-- LREM        (bin, count)
-- LSET        (bin, index, value)
-- LTRIM       (bin, start, stop)
-- RPOP        (bin, count)
-- RPOPLPUSH   (bin1, bin2)
-- RPUSH       (bin, value)
-- RPUSHX      (bin, value)
-- RMULTIPUSH  (bin, value_list)
-- RMULTIPUSHX (bin, value_list)
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
		return list.len(rec[bin])
	else
		return 0;
	end
end

function LPOP (rec, bin, count)
	if (LEXISTS(rec, bin)) then
		return list.take(rec[bin], count)
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

function LMULTIPUSH  (rec, bin, value_list)
	local l = rec[bin];
	for value in list.iterator(value_list) do
		list.prepend(l, value)
	end
	rec[bin] = l;
	UPDATE(rec)
end

function LMULTIPUSHX (rec, bin, value_list)
	if (LEXISTS(rec, bin)) then
		LMULTIPUSH(rec, bin, value_list)
	end
end

function LRANGE (rec, bin, start, stop)
	if (LEXISTS(rec, bin)) then
		local l = rec[bin];
		local new_l = list.take(l, stop)
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
		local l = rec[bin];
		local result_list = list.drop(l, list.len(l) - count);
		rec[bin] = list.take(l, list.len(l) - count)
		UPDATE(rec)
	end
end

function RPOPLPUSH (rec, bin1, bin2)
end

function RPUSH (rec, bin, value)
	local l = rec[bin];
	list.append(l, value)
	rec[bin] = l;
	UPDATE(rec)
end

function RPUSHX (rec, bin, value)
	if (LEXISTS(rec,bin)) then
		RPUSH(rec, bin, value)
	end
end

function RMULTIPUSH  (rec, bin, value_list)
	local l = rec[bin];
	for value in list.iterator(value_list) do
		list.append(l, value)
	end
	rec[bin] = l;
	UPDATE(rec)
end

function RMULTIPUSHX (rec, bin, value_list)
	if (LEXISTS(rec, bin)) then
		RMULTIPUSH(rec, bin, value_list)
	end
end


function push(rec, bin, value) 
	local stack;
	if (rec[bin] == nil) then
		stack = list;
	else
		stack = rec[bin]
	end
	list.append(stack, val)

	rec[bin] = stack;

	if aerospike:exists(rec) then
		aerospike:update(rec)
	else
		aerospike:create(rec)
	end
end

function push(rec, bin, value) 
	local stack;
	if (rec[bin] == nil) then
		stack = list;
	else
		stack = rec[bin]
	end
	list.append(stack, val)

	rec[bin] = stack;

	if aerospike:exists(rec) then
		aerospike:update(rec)
	else
		aerospike:create(rec)
	end
end
