local pairs,type,append,concat,tostring = pairs,type,table.insert,table.concat,tostring
local function tappend(t, v) t[#t+1] = v end

local function has_lquote(s)
    local lstring_pat = '([%[%]])(=*)%1'
    local start, finish, bracket, equals, next_equals = nil, 0, nil, nil, nil
    repeat
        start, finish, bracket, next_equals =  s:find(lstring_pat, finish + 1)
        if start then
            next_equals = #next_equals
            equals = next_equals >= (equals or 0) and next_equals or equals
        end
    until not start
    return   equals
end

-- @return The quoted string.
local function quote_string(s)
    local equal_signs = has_lquote(s)
    if  s:find("\n") or equal_signs then
        equal_signs =  ("="):rep((equal_signs or -1) + 1)
        if s:find("^\n") then s = "\n" .. s end
        local lbracket, rbracket =
        "[" .. equal_signs .. "[",
        "]" .. equal_signs .. "]"
        s = lbracket .. s .. rbracket
    else
        s = ("%q"):format(s)
    end
    return s
end

local function quote (s)
    if type(s) == 'table' then
        return _M.write(s,'')
    else
        return quote_string(s)-- ('%q'):format(tostring(s))
    end
end

local function quote_if_necessary (v)
    if not v then return ''
    else
        if v:find ' ' then v = quote_string(v) end
    end
    return v
end

local function index (numkey,key)
    if not numkey then
        key = quote(key)
        key = key:find("^%[") and (" " .. key .. " ") or key
    end
    return '['..key..']'
end

local function is_identifier (s)
    return type(s) == 'string' and s:find('^[%a_][%w_]*$')
end

local _M = {}

--- Create a string representation of a Lua table.
--  This function never fails, but may complain by returning an
--  extra value. Normally puts out one item per line, using
--  the provided indent; set the second parameter to '' if
--  you want output on one line.
--  @tab tbl Table to serialize to a string.
--  @string space (optional) The indent to use.
--  Defaults to two spaces; make it the empty string for no indentation
--  @bool not_clever (optional) Use for plain output, e.g {['key']=1}.
--  Defaults to false.
--  @return a string
--  @return a possible error message
function _M.write (tbl,space,not_clever)
    if type(tbl) ~= 'table' then
        local res = tostring(tbl)
        if type(tbl) == 'string' then return quote(tbl) end
        return res, ' type is '..type(tbl)
    end
    local set = ' = '
    if space == '' then set = '=' end
    space = space or '  '
    local lines = {}
    local line = ''
    local tables = {}


    local function put(s)
        if #s > 0 then
            line = line..s
        end
    end

    local function putln (s)
        if #line > 0 then
            line = line..s
            append(lines,line)
            line = ''
        else
            append(lines,s)
        end
    end

    local function eat_last_comma ()
        local n,lastch = #lines
        local lastch = lines[n]:sub(-1,-1)
        if lastch == ',' then
            lines[n] = lines[n]:sub(1,-2)
        end
    end

    local writeit
    writeit = function (t,oldindent,indent)
        local tp = type(t)
        if tp ~= 'string' and  tp ~= 'table' then
            putln(quote_if_necessary(tostring(t))..',')
        elseif tp == 'string' then
            putln(quote_string(t) ..",")
        elseif tp == 'table' then
            if tables[t] then
                putln('<cycle>,')
                return
            end
            tables[t] = true
            local newindent = indent..space
            putln('{')
            local used = {}
            if not not_clever then
                for i,val in ipairs(t) do
                    put(indent)
                    writeit(val,indent,newindent)
                    used[i] = true
                end
            end
            for key,val in pairs(t) do
                local numkey = type(key) == 'number'
                if not_clever then
                    key = tostring(key)
                    put(indent..index(numkey,key)..set)
                    writeit(val,indent,newindent)
                else
                    if not numkey or not used[key] then -- non-array indices
                    if numkey or not is_identifier(key) then
                        key = index(numkey,key)
                    end
                    put(indent..key..set)
                    writeit(val,indent,newindent)
                    end
                end
            end
            tables[t] = nil
            eat_last_comma()
            putln(oldindent..'},')
        else
            putln(tostring(t)..',')
        end
    end
    writeit(tbl,'',space)
    eat_last_comma()
    return concat(lines,#space > 0 and '\n' or '')
end

function _M.print_r(o)
    return _M.write(o)
end

--- get the Lua keywords as a set-like table.
-- So `res["and"]` etc would be `true`.
-- @return a table
function _M:get_keywords ()
    local keywords = {
        ["and"] = true, ["break"] = true,  ["do"] = true,
        ["else"] = true, ["elseif"] = true, ["end"] = true,
        ["false"] = true, ["for"] = true, ["function"] = true,
        ["if"] = true, ["in"] = true,  ["local"] = true, ["nil"] = true,
        ["not"] = true, ["or"] = true, ["repeat"] = true,
        ["return"] = true, ["then"] = true, ["true"] = true,
        ["until"] = true,  ["while"] = true,
        --sys keywords
        ["manage"] = true,
        ["admin"] = true,
        ["php"] = true,
        ["java"] = true,
        ["python"] = true,
        ["manage"] = true,
        ["gateway"] = true,
        ["redis"] = true,
        ["api"] = true,
    }
    return keywords
end

return _M