local ngx,pairs,setmetatable,ngxmatch = ngx,pairs,setmetatable,ngx.re.gmatch

local Request = {}

function Request:new()
    ngx.req.read_body()
    local params = ngx.req.get_uri_args()
    local posts = {}
    for k,v in pairs(ngx.req.get_post_args()) do
        params[k] = v
        posts[k] = v
    end

    local instance = {
        uri = ngx.var.uri,
        req_uri = ngx.var.request_uri,
        req_args = ngx.var.args,
        params = params,
        posts = posts,
        uri_args = ngx.req.get_uri_args(),
        uri_path_table = self:_uri_path(),
        method = ngx.req.get_method(),
        headers = ngx.req.get_headers(),
        body_raw = ngx.req.get_body_data()
    }
    setmetatable(instance, {__index = self})
    return instance
end

function Request:_uri_path()
    local match = {}
    local tmp = 1
    for v in ngxmatch(ngx.var.uri , '/([A-Za-z0-9_.]+)') do
        match[tmp] = v[1]
        tmp = tmp +1
    end
    return match
end

return Request