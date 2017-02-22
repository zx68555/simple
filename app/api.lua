local config = require "config.config"
local cjson = require "cjson.safe"
cjson.encode_sparse_array(true)
local _M = {}

function _M:index()
    return self.response:json({"hello simple"})
end


function _M:test()
    print_r(config)
    print_r(self.request.params)
    self.add_biz_log("collect_data:"..cjson.encode(self.request.params))

    return self.response:json(config,200,"OK")
end

return _M