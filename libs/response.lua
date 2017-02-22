local cjson = require "cjson"
local ngx,setmetatable = ngx,setmetatable
--local errors = require "config.errors"
local Response = {}
Response.__index = Response

function Response:new()
    local instance = {
        http_status = nil,
        status = 200,
        headers = {},
        body = '',
    }
    setmetatable(instance, Response)
    return instance
end

function Response:json(data, code, msg)
    if not code then code = 200 end
    if not msg then msg = "OK" end
    self:set_header("Content-Type", "application/json; charset=UTF-8")
    return cjson.encode({status = code, message = msg, data = data})
end


function Response:jsonp(data)
    local arg_callback = ngx.var.arg_callback
    if arg_callback then
        local m, err = ngx.re.match(arg_callback, "[^\\w]")
        if not m then
            data = arg_callback .. "(" .. data .. ");"
            self:set_header("Content-Type", "application/x-javascript; charset=UTF-8")
        end
    end
    return data
end

function Response:raw(payload)
    return payload
end

function Response:error(code,msg)
    if not code then code = 500 end
    if not msg or code == 500 then
        ngx.log(ngx.ERR, 'system_error:'..msg)
    end
    self:set_header("Content-Type", "application/json; charset=UTF-8")
    return cjson.encode({status = code, message = msg, data = {}})
end

function Response:warn(msg)
    self:set_header("Content-Type", "application/json; charset=UTF-8")
    return cjson.encode({status = 500, message = msg, data = {}})
end

function Response:response()
    ngx.print(self.body)
    return true
end

function Response:get_body()
    return self.body
end

function Response:get_headers()
    return self.headers
end

function Response:get_header(key)
    return self.headers[key]
end

function Response:set_header(key, value)
    ngx.header[key] = value
end

function Response:status(status)
    ngx.status = status
    self.http_status = status
    return self
end

function Response:send(text)
    self:set_header("Content-Type", "application/json; charset=UTF-8")
    self:response(text)
end

return Response