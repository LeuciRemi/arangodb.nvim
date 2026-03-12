local M = {}

local uv = vim.uv or vim.loop

local BASE64_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

local function close_handle(handle)
  if handle and not handle:is_closing() then
    handle:close()
  end
end

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

local function read_line(text, offset)
  local crlf = text:find("\r\n", offset, true)
  local lf = text:find("\n", offset, true)

  if crlf and (not lf or crlf < lf) then
    return text:sub(offset, crlf - 1), crlf + 2
  end
  if lf then
    return text:sub(offset, lf - 1), lf + 1
  end
end

local function decode_chunked(body)
  local chunks = {}
  local offset = 1

  while true do
    local line, next_offset = read_line(body, offset)
    if not line then
      error("Invalid chunked response from ArangoDB")
    end

    local size_text = line:match("^%s*([0-9A-Fa-f]+)")
    local size = size_text and tonumber(size_text, 16) or nil
    if not size then
      error("Invalid chunk size in ArangoDB response")
    end

    offset = next_offset
    if size == 0 then
      break
    end

    local chunk = body:sub(offset, offset + size - 1)
    if #chunk < size then
      error("Truncated chunked response from ArangoDB")
    end

    chunks[#chunks + 1] = chunk
    offset = offset + size

    if body:sub(offset, offset + 1) == "\r\n" then
      offset = offset + 2
    elseif body:sub(offset, offset) == "\n" then
      offset = offset + 1
    else
      error("Invalid chunk separator in ArangoDB response")
    end
  end

  return table.concat(chunks)
end

local function parse_response(raw)
  local header_end = raw:find("\r\n\r\n", 1, true)
  local separator_len = 4

  if not header_end then
    header_end = raw:find("\n\n", 1, true)
    separator_len = 2
  end

  if not header_end then
    error("Invalid HTTP response from ArangoDB")
  end

  local head = raw:sub(1, header_end - 1)
  local body = raw:sub(header_end + separator_len)
  local delimiter = head:find("\r\n", 1, true) and "\r\n" or "\n"
  local lines = vim.split(head, delimiter, { plain = true, trimempty = false })
  local status = tonumber((lines[1] or ""):match("^HTTP/%d+%.%d+%s+(%d+)$") or (lines[1] or ""):match("^HTTP/%d+%.%d+%s+(%d+)%s+"))

  if not status then
    error("Invalid HTTP status line from ArangoDB: " .. (lines[1] or ""))
  end

  local headers = {}
  for index = 2, #lines do
    local name, value = lines[index]:match("^([^:]+):%s*(.*)$")
    if name then
      headers[name:lower()] = value
    end
  end

  local transfer_encoding = headers["transfer-encoding"]
  if transfer_encoding and transfer_encoding:lower():find("chunked", 1, true) then
    body = decode_chunked(body)
  else
    local content_length = tonumber(headers["content-length"])
    if content_length then
      body = body:sub(1, content_length)
    end
  end

  return {
    status = status,
    headers = headers,
    body = body,
  }
end

local function normalize_scheme(value)
  local scheme = tostring(value or "http"):lower()
  if scheme ~= "http" and scheme ~= "https" then
    error("Unsupported ArangoDB scheme: " .. tostring(value))
  end
  return scheme
end

local function build_headers(opts, host, port, body)
  local headers = {}
  for name, value in pairs(opts.headers or {}) do
    if value ~= nil then
      headers[name] = tostring(value)
    end
  end

  if headers.Host == nil and headers.host == nil then
    headers.Host = string.format("%s:%d", host, port)
  end
  if headers.Accept == nil and headers.accept == nil then
    headers.Accept = "application/json"
  end
  if headers.Connection == nil and headers.connection == nil then
    headers.Connection = "close"
  end
  if headers.Authorization == nil and headers.authorization == nil then
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
    local result = vim.system(args, {
      stdin = input,
      text = true,
    }):wait()
    local output = result.stdout or ""
    if result.stderr and result.stderr ~= "" then
      output = output .. result.stderr
    end
    return output, result.code or 0
  end

  local output = vim.fn.system(args, input or "")
  return output, vim.v.shell_error
end

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
    "--stderr",
    "-",
    "--globoff",
    "--http1.1",
    "--include",
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

  for _, name in ipairs(sorted_header_names(curl_headers)) do
    args[#args + 1] = "--header"
    args[#args + 1] = string.format("%s: %s", name, curl_headers[name])
  end

  if body ~= nil then
    args[#args + 1] = "--data-binary"
    args[#args + 1] = "@-"
  end

  args[#args + 1] = "--url"
  args[#args + 1] = string.format("%s://%s:%d%s", scheme, host, port, path)

  local output, code = run_command(args, body)
  if code ~= 0 then
    local message = vim.trim(output or "")
    if message == "" then
      message = string.format("curl exited with code %d", code)
    end
    error(message)
  end

  return parse_response(output)
end

function M.request(opts)
  opts = opts or {}

  if not uv then
    error("Lua HTTP transport unavailable")
  end

  local host = opts.host
  if type(host) ~= "string" or host == "" then
    error("Missing ArangoDB host")
  end

  local port = tonumber(opts.port)
  if not port then
    error("Invalid ArangoDB port: " .. tostring(opts.port))
  end

  local method = tostring(opts.method or "GET"):upper()
  local scheme = normalize_scheme(opts.scheme)
  local path = tostring(opts.path or "/")
  local timeout = tonumber(opts.timeout) or 30000
  local body = opts.body

  if path == "" then
    path = "/"
  elseif path:sub(1, 1) ~= "/" then
    path = "/" .. path
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

  return parse_response(table.concat(state.chunks))
end

return M
