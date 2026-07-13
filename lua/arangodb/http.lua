--- Minimal HTTP transport used by the ArangoDB client.
local M = {}

local uv = vim.uv or vim.loop
local diagnostics = require("arangodb.diagnostics")
local errors = require("arangodb.errors")
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

local function write_curl_headers(headers)
  local temp_dir = uv.os_tmpdir()
  if type(temp_dir) ~= "string" or temp_dir == "" then
    error("Unable to locate a temporary directory for curl headers")
  end

  -- HTTPS requests can start from a libuv callback. Keep temporary-file
  -- creation entirely in libuv because Vimscript functions such as tempname()
  -- are forbidden in Neovim's fast-event context.
  local fd, path = uv.fs_mkstemp(vim.fs.joinpath(temp_dir, "arangodb-nvim-curl-XXXXXX"))
  if not fd then
    error("Unable to create secure curl header file: " .. tostring(path))
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

local function curl_args(opts, scheme, host, port, method, path, headers, body, timeout)
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

  return args, header_file
end

local function prepare_request(opts)
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
  return {
    opts = opts,
    host = host,
    port = port,
    method = method,
    scheme = scheme,
    path = path,
    timeout = timeout,
    body = body,
    headers = headers,
  }
end

local function transport_error(request, message, kind)
  if errors.is(message) then
    return message
  end
  return errors.new({
    kind = kind or "transport",
    message = tostring(message),
    method = request.method,
    path = request.path,
  })
end

local function tcp_request_async(request, callback)
  local raw_request = build_request(request.method, request.path, request.headers, request.body)
  local tcp = assert(uv.new_tcp())
  local timer = assert(uv.new_timer())
  local state = {
    done = false,
    chunks = {},
  }

  local function finish(err, response)
    if state.done then
      return
    end
    state.done = true

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
    callback(err, response)
  end

  timer:start(request.timeout, 0, function()
    finish(string.format("ArangoDB request timed out after %d ms", request.timeout))
  end)

  tcp:connect(request.host, request.port, function(connect_err)
    if state.done then
      return
    end
    if connect_err then
      finish(connect_err)
      return
    end

    tcp:read_start(function(read_err, chunk)
      if state.done then
        return
      end
      if read_err then
        finish(read_err)
      elseif chunk then
        state.chunks[#state.chunks + 1] = chunk
      else
        local ok, response = pcall(response_parser.parse, table.concat(state.chunks))
        if ok then
          finish(nil, response)
        else
          finish(response)
        end
      end
    end)

    tcp:write(raw_request, function(write_err)
      if write_err then
        finish(write_err)
      end
    end)
  end)

  return {
    cancel = function()
      finish("ArangoDB request cancelled")
    end,
  }
end

local function curl_request_async(request, callback)
  local args, header_file = curl_args(
    request.opts,
    request.scheme,
    request.host,
    request.port,
    request.method,
    request.path,
    request.headers,
    request.body,
    request.timeout
  )
  local state = { done = false }
  local process

  local function finish(err, response)
    if state.done then
      return
    end
    state.done = true
    pcall(uv.fs_unlink, header_file)
    callback(err, response)
  end

  local started, process_or_error = pcall(vim.system, args, {
    stdin = request.body,
    text = true,
  }, function(result)
    if state.done then
      return
    end
    local output = result.stdout or ""
    local stderr = result.stderr or ""
    if result.code ~= 0 then
      local message = vim.trim(stderr ~= "" and stderr or output)
      finish(message ~= "" and message or string.format("curl exited with code %d", result.code))
      return
    end
    local ok, response = pcall(response_parser.parse, output)
    if ok then
      finish(nil, response)
    else
      finish(response)
    end
  end)
  if not started then
    pcall(uv.fs_unlink, header_file)
    error(process_or_error, 0)
  end
  process = process_or_error

  return {
    cancel = function()
      if state.done then
        return
      end
      if process then
        pcall(process.kill, process, 15)
      end
      finish("ArangoDB request cancelled")
    end,
  }
end

--- Execute a non-blocking request and return a cancellable handle.
function M.request_async(opts, callback)
  assert(type(callback) == "function", "HTTP callback is required")
  local prepared, request = pcall(prepare_request, opts)
  if not prepared then
    vim.schedule(function()
      callback(errors.new({ kind = "configuration", message = tostring(request) }))
    end)
    return { cancel = function() end }
  end
  local started = uv.hrtime()
  local completed = false

  local function complete(err, response)
    if completed then
      return
    end
    completed = true
    if err then
      err = transport_error(request, err, tostring(err):find("cancelled", 1, true) and "cancelled" or "transport")
    end
    vim.schedule(function()
      local response_failed = response and response.status and response.status >= 400
      -- Diagnostics are best-effort and must never change request completion.
      pcall(diagnostics.record, {
        method = request.method,
        scheme = request.scheme,
        host = request.host,
        port = request.port,
        path = request.path,
        status = response and response.status or nil,
        duration_ms = math.floor((uv.hrtime() - started) / 1000000),
        outcome = (err or response_failed) and "error" or "success",
        error_kind = err and err.kind or (response_failed and "server" or nil),
      })
      callback(err, response)
    end)
  end

  local ok, handle = pcall(function()
    if request.scheme == "https" then
      return curl_request_async(request, complete)
    end
    return tcp_request_async(request, complete)
  end)
  if not ok then
    complete(handle)
    return { cancel = function() end }
  end
  return handle
end

--- Execute a request synchronously for command-style and mutation operations.
function M.request(opts)
  opts = opts or {}
  local done = false
  local response
  local request_error
  local handle = M.request_async(opts, function(err, value)
    request_error = err
    response = value
    done = true
  end)

  local timeout = math.floor(tonumber(opts.timeout) or 30000)
  if not vim.wait(timeout + 250, function()
    return done
  end, 10) then
    handle.cancel()
    vim.wait(100, function()
      return done
    end, 10)
  end

  if request_error then
    error(request_error, 0)
  end
  if not done then
    error(errors.new({ kind = "transport", message = "ArangoDB request did not complete" }), 0)
  end
  return response
end

return M
