local pcall,setmetatable = pcall,setmetatable
local Dispatcher = require 'libs.dispatcher'

local function new_dispatcher(self)
    return Dispatcher:new(self)
end

local _M = {}

function _M:new()
    local instance = {
        run = self.run,
        dispatcher = self:lpcall(new_dispatcher,self)
    }
    setmetatable(instance, {__index = self})
    return instance
end

function _M:run()
    self:lpcall(self.dispatcher.dispatch, self.dispatcher)
end

function _M:lpcall( ... )
    local ok, rs_or_error = pcall( ... )
    if ok then
        return rs_or_error
    else
        self.dispatcher:err_response(500,rs_or_error)
    end
end

return _M