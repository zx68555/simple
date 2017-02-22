local ngx,type,setmetatable,require = ngx,type,setmetatable,require
local Request = require 'libs.request'
local Response = require 'libs.response'
local log = require 'libs.utils.log'
local util = require "libs.utils.util"

function print_r( ... )
    ngx.say(util.print_r( ... ))
end

local _M = {}

function _M:new()
    self.request = Request:new()
    self.response = Response:new()
    self.log = log
    local instance = {
        module_prefix = 'app.'
    }
    setmetatable(instance, {__index = self})
    return instance
end

function _M:require_module(module)
    return require(self.module_prefix..module)
end

function _M:dispatch()
    self.log.init_log()
    local module_path,action = self:_route(self.request.uri_path_table)
    local module = self:require_module(module_path)
    setmetatable(module,{__index = self})
    if module[action] == nil then
        return self:err_response(500,action.."-non-exist")
    end
    local body = module[action](module)
    if body == nil or type(body) ~= 'string' then
        return self:err_response(500,'must return a String.')
    end
    self.response.body = body
    self.response:response()
    log.write_log()
    ngx.eof()
end

function _M:_route(uri_path_table)
    local moudle,action = 'api','index'
    if #uri_path_table == 0 then
        return moudle, action
    end

    if uri_path_table[1] then
        moudle = uri_path_table[1]
    end

    if uri_path_table[2] then
        action = uri_path_table[2]
    end
    local keywords = util:get_keywords()
    if keywords[action] then
        return 'api', 'index'
    else
        return moudle,action
    end
end

function _M:err_response(code,msg)
    self.response.body = self.response:error(code, msg)
    self.response:response()
    log.write_log()
    ngx.eof()
end

return _M