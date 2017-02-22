local ngx,table = ngx,table
local log_biz_tables,log_sys_tables = {},{}

local function init_log()
    log_biz_tables = {}
    log_sys_tables = {}
end

local function add_biz_log(msg)
    if msg == nil then return false end
    table.insert(log_biz_tables, msg)
end

local function add_sys_log(msg)
    if msg == nil then return false end
    table.insert(log_sys_tables, msg)
end

local function write_log(name)
    if name == nil then name = "" end
    local rand_str = ngx.md5(ngx.var.pid .. ngx.req.start_time()*1000 ..ngx.var.request_uri)
    if #log_biz_tables > 0 then
        ngx.log(ngx.NOTICE,"biz:"..rand_str.."#"..name..table.concat(log_biz_tables, "#").."\t"..ngx.localtime())
        log_biz_tables = {}
    end
    if #log_sys_tables > 0  then
        ngx.log(ngx.NOTICE,"sys:"..rand_str.."#"..name..table.concat(log_sys_tables, "#").."\t"..ngx.localtime())
        log_sys_tables = {}
    end
end

return {
    init_log = init_log,
    write_log = write_log,
    add_biz_log = add_biz_log,
    add_sys_log = add_sys_log,
}