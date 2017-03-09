local config = require "config.config"
local cjson = require "cjson.safe"
cjson.encode_sparse_array(true)
local _M = {}

function _M:index()
    return self.response:json({"hello simple"})
end


function _M:test()
    return self.response:json(config,200,"OK")
end

function _M:data()
    if self.validate:check("get",{
        id = 'required',
    }):is_not_ok() then
        return self.response:json(self.validate.status,self.validate.error)
    end
    return self.response:json(self.validate.params,200,"OK")
end

return _M