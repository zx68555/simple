local ngx,table = ngx,table
local cjson = require "cjson.safe"
cjson.encode_sparse_array(true)
local scache = ngx.shared.cleaner_cache
local http = require"libs.utils.http.http"
local resty_lock = require "resty.lock"
local cleaner_locks = "cleaner_locks"

-------------------------------------------------
--      功能 : nginx功能封装 or 常见函数封装
-------------------------------------------------

local _M = {_VERSION = '1.0' }
-------------------------------------------------
--      ngx func 封装
-------------------------------------------------

function _M:get_now_time(is_cached)
    is_cached = is_cached or false
    if not is_cached then
        ngx.update_time()
    end

    return ngx.now()
end

function _M:arg_is_invalid(arg)
    if not arg then return true end
    if arg == ngx.null or arg == '' then
        return true
    end

    return false
end

function _M:arg_is_valid(arg)
    if not arg then return false end
    if arg == ngx.null or arg == '' then
        return false
    end

    return true
end

function _M:capture(url,args,method,body)
    if url == nil then
        return {}
    end
    local uri_args = ngx.req.get_uri_args()
    local body_data = nil
    if uri_args and uri_args["_plugin"] and uri_args["_plugin"] == "request_form2json" and ngx.req.get_headers()["Content-Type"] == "application/x-www-form-urlencoded" then
        local args, err = ngx.req.get_post_args()
        if not args then
            ngx.log(ngx.ALERT, "[REQ] [agent] [form2json] ngx.req.get_post_args() " .. err)
            body_data = ngx.req.get_body_data()
        else
            -- form2json
            body_data = cjson.encode(args)
        end
    else
        body_data = ngx.req.get_body_data()
    end

    method = string.upper(method)
    local req_start = self:get_now_time()
    local res = ngx.location.capture(url, {
        method = self:method_to_number(method),
        args = args,
        body = body_data
    })

    --[[if res.body then
        local stream = zlib.inflate()
        local deflated, eof, bytes_in, bytes_out = stream(res.body)
        if eof then
            res.body = deflated
        else
            ngx.log(ngx.WARN, string.format("[gzip] The eof result is false, body:%s", res.body))
        end

        ngx.log(ngx.INFO, string.format("[gzip] eof:%s, byte_in:%s, byte_out:%s",  eof, bytes_in, bytes_out))
    end]]



    local req_end = self:get_now_time()
    local res_time = (req_end - req_start)*1000
    if res.status ~= nil and res.status == 504 then
        self:qalarm('serious_problem','ac0001')
    end
    local log_str = '[inner_req] [req_start:'..req_start..'] [req_end:'..req_end..'][res_time:'..res_time..'ms] [url:'..url..'] [method:'..method..'][args:'..cjson.encode(args or {})..'] [body:'..cjson.encode(body or {})..'] [res:'..cjson.encode(res or {})..']'
    ngx.log(ngx.NOTICE, log_str)
    return res
end

function _M:method_to_number(method)
    if method == "GET" then
        return ngx.HTTP_GET
    elseif method == "POST" then
        return ngx.HTTP_POST
    elseif method == "PUT" then
        return ngx.HTTP_PUT
    elseif method == "DELETE" then
        return ngx.HTTP_DELETE
    elseif method == "PATCH" then
        return ngx.HTTP_PATCH
    else
        ngx.log(ngx.WARN, "[REQ] [method_to_number] invalid method:" .. method)
        return ngx.HTTP_GET
    end
end


function _M:get_all_args()
    local uri_args = ngx.req.get_uri_args()
    ngx.req.read_body()
    local post_args, err = ngx.req.get_post_args()
    if not post_args then
        ngx.log(ngx.WARN, string.format("[REQ] [agent] ngx.req.get_post_args():%s", err))
        post_args = {}
    else
        ngx.log(ngx.DEBUG, string.format("[REQ] [agent] post_args:%s", cjson.encode(post_args)))
    end

    return self:table_merge(uri_args, post_args)
end

------------------------------------------------------------------------
--                              table
------------------------------------------------------------------------

function _M:table_len(t)
    local len = 0
    for _,_ in pairs(t or {}) do
        len = len + 1
    end
    return len
end


function _M:table_append(t, v) t[#t+1] = v end

function _M:build_params(params)
    local params_arr = {}
    for k, v in pairs(params or {}) do
        if type(v) == 'boolean' then v = "" end
        self:table_append(params_arr, k .. "=" .. v)
    end
    return table.concat(params_arr, "&")
end


--一维数组排序
function _M:one_dim_map_sort(t, sort)
    if sort ~= 'asc' or sort ~= 'desc' then sort  = 'asc' end
    local key_t = {}
    for i, _ in pairs(t) do
        table.insert(key_t, k)
    end
    if sort == 'asc' then
        table.sort(key_t, function(a,b) return a<b end)
    else
        table.sort(key_t, function(a,b) return a>b end)
    end
    return t
end

--二维map排序(按照指定字段排序)
function _M:two_dim_map_sort(t, field, sort)
    if sort ~= 'asc' and sort ~= 'desc' then sort  = 'asc' end
    if sort == 'asc' then
        table.sort(t, function(a,b) return tonumber(a[field])<tonumber(b[field]) end)
    else
        table.sort(t, function(a,b) return tonumber(a[field])>tonumber(b[field]) end)
    end
    return t
end

function _M:table_merge(a, b)
    local c = {}
    for k,v in pairs(a or {}) do
        c[k] = v
    end
    for k,v in pairs(b or {}) do
        c[k] = v
    end
    return c
end


function _M:table_is_empty(t)
    return type(t) ~= 'table' or  t == nil or next(t) == nil
end

------------------------------------------------------------------------
--                              ip
------------------------------------------------------------------------



function _M:check_is_local_ip(ip)
    if type(ip) ~= "string" then return false end
    -- 内网默认为10.0.0.0/8
    local i,j = string.find(ip, "^10%.")
    if i == nil and j == nil then
        return false
    end
    return true
end


function _M:check_is_valid_ip(ip)
    if type(ip) ~= "string" then return false end
    local _, _, ipa, ipb, ipc, ipd = string.find(ip, "(%d+)%.(%d+)%.(%d+)%.(%d+)")

    if ipa and ipb and ipc and ipd then
        local ipnum = ipa*16777216 + ipb*65536 + ipc*256 + ipd
        if ipnum > 2^32 or ipnum < 0 then
            return false
        else
            return true
        end
    else
        return false
    end
end

function _M:get_real_ip()
    local ip = ngx.req.get_uri_args()["_realip"]
    if ip ~= nil then
        return ip
    end
    local ip = ngx.req.get_headers()["X-Real-IP"]
    if ip ==nil then
        ip =ngx.req.get_headers()["x-real-ip"]
    end
    if ip == nil then
        ip = ngx.var.remote_addr
    end
    if ip == nil then
        ip = "unknown"
    end
    return ip
end


------------------------------------------------------------------------
--                              string
------------------------------------------------------------------------


function _M:strtotime(timestring)
    local data_match = "(%d+)-(%d+)-(%d+) (%d+):(%d+)"
    local year,month,day,hour,min,sec = timestring:match(data_match)
    local time = os.time({day=day,month=month,year=year,hour=hour,min=min,sec=sec})
    return tonumber(time)
end



function _M:split(str, delimiter)
    if str==nil or str=='' or delimiter==nil then
        return nil
    end
    local result = {}
    for match in (str..delimiter):gmatch("(.-)"..delimiter) do
        table.insert(result, match)
    end
    return result
end

function _M:http_build_query(map)
    local string,str = '',''
    for k,v in pairs(map or {}) do
        string = string .. str .. k..'='..v
        str = '&'
    end
    return string
end

------------------------------------------------------------------------
--                              cache
------------------------------------------------------------------------


function _M:get_cache( key )
    local val, err = scache:get(key)
    if err or not val then
        return nil
    end
    return cjson.decode(val)
end


function _M:set_cache( key, data, expire_time)
    if not expire_time then expire_time = 60 end
    local ok, _ = scache:set(key, cjson.encode(data), expire_time)
    if not ok then
        ngx.log(ngx.WARN,'failed to manage shm cache:'..key)
        return false
    end
    return true
end

--利用ngx_lock解决缓存失效风暴  func 是一个function
function _M:get_data_by_lock(cache_key,cache_time,params,func)
    if cache_key == nil then return false end
    if cache_time == nil then cache_time = 1 end
    local val = self:get_cache(cache_key)
    if val then return val end

    local logtable = {}
    table.insert(logtable,'get_data_by_lock key:'..cache_key..' cache_time:'..cache_time)
    local lock = resty_lock:new(cleaner_locks)
    local elapsed, err = lock:lock(cache_key)
    if not elapsed then
        table.insert(logtable,'failed to acquire the lock: '..err)
        ngx.log(ngx.ERR, table.concat(logtable, "\t"))
        return false
    end

    val = self:get_cache(cache_key)
    if val then
        local ok, err = lock:unlock()
        if not ok then
            table.insert(logtable,'failed to unlock: '..err)
            ngx.log(ngx.ERR, table.concat(logtable, "\t"))
            return false
        end
        return val
    end
    table.insert(logtable,'params:'..cjson.encode(params or {}))
    local val = func(params)
    if not val then
        local ok, err = lock:unlock()
        if not ok then
            table.insert(logtable,'failed to unlock get data: '..err)
            ngx.log(ngx.ERR, table.concat(logtable,"\t"))
            return false
        end
        table.insert(logtable,'failed to get data')
        ngx.log(ngx.ERR, table.concat(logtable,"\t"))
        return false
    end

    local ok = self:set_cache(cache_key, val, cache_time)
    if not ok then
        local ok, err = lock:unlock()
        if not ok then
            table.insert(logtable,'failed to unlock set_cache: '..err)
            ngx.log(ngx.ERR, table.concat(logtable,"\t"))
            return false
        end
        table.insert(logtable,'failed to set_cache')
        ngx.log(ngx.ERR, table.concat(logtable,"\t"))
        return false
    end

    local ok, err = lock:unlock()
    if not ok then
        table.insert(logtable,'failed to unlock: '..err)
        ngx.log(ngx.ERR, table.concat(logtable,"\t"))
        return false
    end
    table.insert(logtable,'succ use share dict lock'..cjson.encode(val))
    ngx.log(ngx.ERR, table.concat(logtable,"\t"))
    return val

end

------------------------------------------------------------------------
--                              else
------------------------------------------------------------------------


function _M:get_distance(begin_lat, begin_lon, end_lat, end_lon)
    begin_lat = tonumber(begin_lat)
    begin_lon = tonumber(begin_lon)
    end_lat = tonumber(end_lat)
    end_lon = tonumber(end_lon)
    if not begin_lat or not begin_lon or not end_lat or not end_lon then
        return nil
    end
    local pi = math.pi
    local distance = math.floor(6378.138 * 2 * math.asin(math.sqrt(math.pow(math.sin((begin_lat * pi / 180 - end_lat * pi / 180) / 2),2) + math.cos(begin_lat * pi / 180) * math.cos(end_lat* pi / 180) * math.pow(math.sin((begin_lon * pi / 180 - end_lon * pi / 180) / 2),2))) * 1000 + 0.5)
    if distance < 0 then
        distance = 0
    end
    return distance
end

function _M:check_sign(data,sign)
    if not data.app_time or not data.app_code then
        return false
    end
    local app_code = data.app_code
    if string.len(app_code) ~= 50 then
        return false
    end
    data.app_code = nil
    local key_t = {}
    for k,_ in pairs(data) do
        table.insert(key_t, k)
    end
    table.sort(key_t)
    local s, s1, s2, s3 = ''
    for k,v in pairs(key_t) do
        s = s..v..data[v]
    end
    s1 = ngx.md5(s .. sign);
    s2 = ngx.md5(s1);
    s3 = string.sub(s1..s2, 1, 50)
    if s3 ~= app_code then
        ngx.log(ngx.WARN, 'param app_code not valid,app_code:'..app_code..',valid_app_code:'..s3)
        return false
    end
    return true
end

function _M:http(url,params,method,headers,timeout)
    local http = http:new()
    if url == nil then return false end
    if params == nil then params = {} end
    if method == nil then method = "GET" end
    if headers == nil then headers = {} end
    if timeout == nil then timeout = 3000 end
    http:set_timeout(timeout)
    url = url..self:get_trace_id()
    local params = self:build_params(params)
    local res,err = http:request_uri(url,{ method = method, body = params, headers = headers})
    if res == nil then
        self:qalarm('serious_problem','ac0001')
        return {}
    end
    return cjson.decode(res.body)
end


function _M:get_trace_id()
    local trace_id = ngx.req.get_uri_args()["__trace_id"]
    if trace_id == nil then
        return ""
    else
        return "__trace_id="..trace_id
    end
end


function _M:get_rand_seed()
    local seed = ngx.var.pid .. (ngx.var.connection%4096) .. ngx.now()*1000 ..ngx.var.request_uri
    return ngx.crc32_long(seed)
end

function _M:randomseed()
    math.randomseed(self:get_rand_seed())
end

return _M