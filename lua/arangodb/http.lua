--- Minimal HTTP transport used by the ArangoDB client.
local M = {}

local uv = vim.uv or vim.loop
local response_parser = require("arangodb.http.response")

local BASE64_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
local HEADER_NAME_PATTERN = "^[!#$%%&'*+.^_`|~%w-]+$"

local function close_handle(handle)
  if handle and not handle:is_closing() then
    handle:close()
  end
end

--- Encode basic-auth credentials without depending on an external library.
local function encode_base64(data)
  local parts = {}

  for index = 1, #data, 3 do
    local first = data:byte(index) or 0
    local second = data:byte(index + 1) or 0
    local third = data:byte(index + 2) or 0
    local chunk = first * 65536 + second * 256 + third

    local a = math.floor(chunk / 262144) % 64 + 1
    local b = math.floor(chunk / 4096) % 64 + 1
    local c = math.floor(chunk / 64) % 64 + 1
    local d = chunk % 64 + 1

    parts[#parts + 1] = BASE64_CHARS:sub(a, a)
    parts[#parts + 1] = BASE64_CHARS:sub(b, b)

    if index + 1 <= #data then
      parts[#parts + 1] = BASE64_CHARS:sub(c, c)
    else
      parts[#parts + 1] = "="
    end

    if index + 2 <= #data then
      parts[#parts + 1] = BASE64_CHARS:sub(d, d)
    else
      parts[#parts + 1] = "="
    end
  end

  return table.concat(parts)
end

local function normalize_scheme(value)
  local scheme = tostring(value or "http"):lower()
  if scheme ~= "http" and scheme ~= "https" then
    error("Unsupported ArangoDB scheme: " .. tostring(value))
  end
  return scheme
end

--- Build a predictable header set for the outgoing HTTP request.
local function build_headers(opts, host, port, body)
  local headers = {}
  for name, value in pairs(opts.headers or {}) do
    if value ~= nil then
      if type(name) ~= "string" or not name:match(HEADER_NAME_PATTERN) then
        error("Invalid HTTP header name: " .. tostring(name))
      end
      if tostring(value):find("[\r\n]") then
        error("Invalid HTTP header value for " .. name)
      end
      headers[name] = tostring(value)
    end
  end

  if headers.Host == nil and headers.host == nil then
    local header_host = host:find(":", 1, true) and ("[" .. host .. "]") or host
    headers.Host = string.format("%s:%d", header_host, port)
  end
  if headers.Accept == nil and headers.accept == nil then
    headers.Accept = "application/json"
  end
  if headers.Connection == nil and headers.connection == nil then
    headers.Connection = "close"
  end
  if headers.Authorization == nil and headers.authorization == nil and (opts.user ~= nil or opts.password ~= nil) then
    headers.Authorization = "Basic " .. encode_base64(string.format("%s:%s", opts.user or "", opts.password or ""))
  end
  if body ~= nil then
    if headers["Content-Length"] == nil and headers["content-length"] == nil then
      headers["Content-Length"] = tostring(#body)
    end
    if headers["Content-Type"] == nil and headers["content-type"] == nil then
      headers["Content-Type"] = "application/json"
    end
  end

  return headers
end

local function sorted_header_names(headers)
  local names = {}
  for name, _ in pairs(headers) do
    names[#names + 1] = name
  end
  table.sort(names, function(left, right)
    return left:lower() < right:lower()
  end)
  return names
end

--- Render an HTTP/1.1 request for the libuv TCP transport.
local function build_request(method, path, headers, body)
  local request_parts = {
    string.format("%s %s HTTP/1.1", method, path),
  }

  for _, name in ipairs(sorted_header_names(headers)) do
    request_parts[#request_parts + 1] = string.format("%s: %s", name, headers[name])
  end

  request_parts[#request_parts + 1] = ""
  request_parts[#request_parts + 1] = body or ""

  return table.concat(request_parts, "\r\n")
end

local function format_timeout_seconds(timeout)
  return string.format("%.3f", math.max(timeout, 1) / 1000)
end

local function run_command(args, input)
  if vim.system then
    local result = vim
      .system(args, {
        stdin = input,
        text = true,
      })
      :wait()
    return result.stdout or "", result.stderr or "", result.code or 0
  end

  local output = vim.fn.system(args, input or "")
  return output, "", vim.v.shell_error
end

local function write_curl_headers(headers)
  local path = vim.fn.tempname()
  local fd, open_err = uv.fs_open(path, "w", 384)
  if not fd then
    error("Unable to create secure curl header file: " .. tostring(open_err))
  end

  local lines = {}
  for _, name in ipairs(sorted_header_names(headers)) do
    lines[#lines + 1] = string.format("%s: %s", name, headers[name])
  end

  local ok, written, write_err = pcall(uv.fs_write, fd, table.concat(lines, "\n") .. "\n", 0)
  pcall(uv.fs_close, fd)
  if not ok or not written then
    pcall(uv.fs_unlink, path)
    error("Unable to write secure curl header file: " .. tostring(ok and write_err or written))
  end

  return path
end

--- Shell out to curl for HTTPS requests and parse its full response output.
local function curl_request(opts, scheme, host, port, method, path, headers, body, timeout)
  if vim.fn.executable("curl") ~= 1 then
    error("HTTPS ArangoDB connections require `curl` to be installed")
  end

  local curl_headers = vim.deepcopy(headers)
  if body ~= nil and curl_headers.Expect == nil and curl_headers.expect == nil then
    curl_headers.Expect = ""
  end

  local args = {
    "curl",
    "--silent",
    "--show-error",
    "--globoff",
    "--http1.1",
    "--include",
    "--suppress-connect-headers",
    "--request",
    method,
    "--connect-timeout",
    format_timeout_seconds(timeout),
    "--max-time",
    format_timeout_seconds(timeout),
  }

  if opts.tls_verify == false then
    args[#args + 1] = "--insecure"
  end

  if type(opts.tls_ca_file) == "string" and opts.tls_ca_file ~= "" then
    args[#args + 1] = "--cacert"
    args[#args + 1] = opts.tls_ca_file
  end

  local header_file = write_curl_headers(curl_headers)
  args[#args + 1] = "--header"
  args[#args + 1] = "@" .. header_file

  if body ~= nil then
    args[#args + 1] = "--data-binary"
    args[#args + 1] = "@-"
  end

  args[#args + 1] = "--url"
  local url_host = host:find(":", 1, true) and ("[" .. host .. "]") or host
  args[#args + 1] = string.format("%s://%s:%d%s", scheme, url_host, port, path)

  local called, output, stderr, code = pcall(run_command, args, body)
  pcall(uv.fs_unlink, header_file)
  if not called then
    error(output, 0)
  end
  if code ~= 0 then
    local message = vim.trim(stderr ~= "" and stderr or output or "")
    if message == "" then
      message = string.format("curl exited with code %d", code)
    end
    error(message)
  end

  return response_parser.parse(output)
end

--- Execute a request through curl for HTTPS or through libuv TCP for HTTP.
function M.request(opts)
  opts = opts or {}

  if not uv then
    error("Lua HTTP transport unavailable")
  end

  local host = opts.host
  if type(host) ~= "string" or host == "" or host:find("[\r\n]") then
    error("Missing ArangoDB host")
  end

  local port = tonumber(opts.port)
  if not port or port < 1 or port > 65535 or port % 1 ~= 0 then
    error("Invalid ArangoDB port: " .. tostring(opts.port))
  end

  local method = tostring(opts.method or "GET"):upper()
  if not method:match("^[A-Z]+$") then
    error("Invalid HTTP method: " .. method)
  end
  local scheme = normalize_scheme(opts.scheme)
  local path = tostring(opts.path or "/")
  local timeout = math.floor(tonumber(opts.timeout) or 30000)
  local body = opts.body

  if path == "" then
    path = "/"
  elseif path:sub(1, 1) ~= "/" then
    path = "/" .. path
  end
  if path:find("[\r\n]") then
    error("Invalid HTTP request path")
  end
  if timeout < 1 then
    error("HTTP timeout must be a positive number")
  end

  if body ~= nil and type(body) ~= "string" then
    error("HTTP request body must be a string")
  end

  local headers = build_headers(opts, host, port, body)
  if scheme == "https" then
    return curl_request(opts, scheme, host, port, method, path, headers, body, timeout)
  end

  local request = build_request(method, path, headers, body)
  local tcp = assert(uv.new_tcp())
  local timer = assert(uv.new_timer())
  local state = {
    done = false,
    err = nil,
    chunks = {},
  }

  local function finish(err)
    if state.done then
      return
    end

    state.done = true
    state.err = err

    if timer then
      timer:stop()
      close_handle(timer)
      timer = nil
    end

    if tcp then
      pcall(tcp.read_stop, tcp)
      close_handle(tcp)
      tcp = nil
    end
  end

  timer:start(timeout, 0, function()
    finish(string.format("ArangoDB request timed out after %d ms", timeout))
  end)

  tcp:connect(host, port, function(connect_err)
    if connect_err then
      finish(connect_err)
      return
    end

    tcp:read_start(function(read_err, chunk)
      if read_err then
        finish(read_err)
        return
      end

      if chunk then
        state.chunks[#state.chunks + 1] = chunk
        return
      end

      finish(nil)
    end)

    tcp:write(request, function(write_err)
      if write_err then
        finish(write_err)
      end
    end)
  end)

  if not vim.wait(timeout + 100, function()
    return state.done
  end, 10) then
    finish(string.format("ArangoDB request timed out after %d ms", timeout))
  end

  if state.err then
    error(state.err)
  end

  return response_parser.parse(table.concat(state.chunks))
end

return M
