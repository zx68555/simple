local mysql = require "resty.mysql"

local Connection = {}
Connection.__index = Connection

function Connection:new(host, port, user, password, database, conn_timeout, pool_size, keepalive_time, charset)
    if not host then host = "127.0.0.1" end
    if not port then port = 3306 end
    if not conn_timeout then conn_timeout = 1000 end
    if not pool_size then pool_size = 100 end
    if not keepalive_time then keepalive_time = 10000 end
    if not charset then charset = "utf8" end

    return setmetatable({
        host = host,
        port = port,
        user = user,
        password = password,
        database = database,
        pool_size = pool_size,
        keepalive_time = keepalive_time,
        charset = charset,
    }, Connection)
end

local function connect(host, port, user, password, database, conn_timeout, pool_size, keepalive_time, charset)
    local conn = mysql:new()
    conn:set_timeout(conn_timeout)
    local ok, err, errno, sqlstate = conn:connect({
        host = host,
        port = port,
        user = user,
        password = password,
        database = database,
        pool_size = pool_size,
        keepalive_time = keepalive_time,
    })

    if not ok then
        return nil, err, errno, sqlstate
    end

    conn:query("SET NAMES " .. charset)

    return conn
end

local function keepalive(conn, keepalive_time, pool_size)
    return conn:set_keepalive(keepalive_time, pool_size)
end

local function query(self, sql)
    local conn = connect(self.host, self.port, self.user, self.password, self.database,
        self.conn_timeout, self.pool_size, self.keepalive_time, self.charset)
    if not conn then
        return
    end
    local res, err, errno, sqlstate = conn:query(sql)

    if not res then
        return nil, err, errno, sqlstate
    end

    keepalive(conn, self.keepalive_time, self.pool_size)

    return res
end

function Connection:query_one(sql)
    local rows = query(self, sql)
    if rows and #rows > 0 then
        return rows[1]
    end
    return nil
end

function Connection:query_all(sql)
    return query(self, sql)
end

--Deprecated
function Connection:query(sql)
    return query(self, sql)
end

function Connection:execute(sql)
    return query(self, sql)   
end

return Connection
