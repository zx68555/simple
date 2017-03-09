--[[ 验证类 eg: { uid = 'required|int', type = 'sometimes|string',}]]
local _M = {}
_M.__index = _M

function _M:new(request)
    return setmetatable({
        req_params = request.params,
        method = request.method,
        params = {},
        status = 200,
        error = 'OK',
    }, _M)
end

function _M:check(method,field)
    local validate_params = {}
    for k, v in pairs(field or {}) do
        local _tmep,tmp  = self:split(v, '|'),{}
        tmp['value'] = _tmep
        tmp['is_need'] = 0
        if _tmep[1] ~= nil and  _tmep[1] == 'required' then
            tmp['is_need'] = 1
        end
        if _tmep[2] ~= nil then
            tmp['validate_type'] = _tmep[2]
        end
        validate_params[k] = tmp
    end
    if self.method ~= string.upper(method) then

        return self:_result('error_method_not_valid')
    end
    for field, value in pairs(validate_params or {}) do
        local sub_param_value = self.req_params[field]
        if value.is_need == 1 then
            if sub_param_value == nil or sub_param_value == '' then
                return self:_result('error_param_not_empty')
            end
        end

        if value.validate_type ~= nil and value.validate_type == "int" and tonumber(sub_param_value) == nil then
            return self:_result('error_param_not_valid')
        end
        if value.validate_type ~= nil and value.validate_type == "string" then
            sub_param_value = tostring(sub_param_value)
        end
        self.params[field] = sub_param_value
    end

    return self

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

function _M:_result(error)
    local errors = {
        error_param_not_empty=5001,
        error_param_not_valid=5001,
    }
    self.status = errors[error]
    self.error = error
    return self
end

function _M:is_not_ok()
    return self.status ~= 200
end

return _M
