--- JSON request boundary shared by the domain-oriented ArangoDB clients.
local M = {}

local core = require("arangodb.core")
local errors = require("arangodb.errors")

local function plugin_options()
  return require("arangodb.config").get()
end

local function trim_message(value)
  if type(value) ~= "string" then
    return value
  end
  return vim.trim(value)
end

local function database_root(config)
  return "/_db/" .. core.url_encode(config.database)
end

--- Decode a JSON response and turn ArangoDB API failures into structured errors.
function M.decode(response, context)
  context = context or {}
  if type(response) ~= "table" or type(response.status) ~= "number" then
    error(
      errors.new({
        kind = "protocol",
        message = "Invalid response from the ArangoDB HTTP transport",
        method = context.method,
        path = context.path,
      }),
      0
    )
  end

  local body = response.body or ""
  local decoded
  if body ~= "" then
    local ok, value = pcall(vim.json.decode, body)
    if ok then
      decoded = value
    end
  end

  if response.status >= 400 then
    local message
    local error_num
    if type(decoded) == "table" then
      message = decoded.errorMessage or decoded.message
      error_num = decoded.errorNum
    end
    if type(message) ~= "string" or message == "" then
      message = body ~= "" and trim_message(body) or ("ArangoDB request failed with HTTP " .. response.status)
    end
    local conflict = response.status == 412 or (response.status == 409 and error_num == 1200)
    error(
      errors.new({
        kind = conflict and "conflict" or "server",
        message = trim_message(message),
        status = response.status,
        error_num = error_num,
        method = context.method,
        path = context.path,
      }),
      0
    )
  end

  if decoded == nil then
    if body == "" then
      return {}
    end
    error(
      errors.new({
        kind = "protocol",
        message = "Invalid JSON response from ArangoDB",
        status = response.status,
        method = context.method,
        path = context.path,
      }),
      0
    )
  end

  return decoded
end

local function request_options(config, method, path, payload, opts)
  local options = plugin_options()
  opts = opts or {}
  return {
    method = method,
    scheme = config.scheme,
    host = config.host,
    port = config.port,
    path = path,
    body = payload ~= nil and vim.json.encode(payload) or nil,
    headers = opts.headers,
    user = config.user,
    password = config.password,
    timeout = options.http_timeout or 30000,
    tls_verify = options.tls_verify,
    tls_ca_file = options.tls_ca_file,
  }
end

--- Send a synchronous JSON request through the configured transport.
function M.request(config, method, path, payload, opts)
  local response = require("arangodb.http").request(request_options(config, method, path, payload, opts))
  return M.decode(response, { method = method, path = path })
end

--- Send a cancellable JSON request through the configured transport.
function M.request_async(config, method, path, payload, callback, opts)
  return require("arangodb.http").request_async(
    request_options(config, method, path, payload, opts),
    function(err, response)
      if err then
        callback(err)
        return
      end
      local ok, decoded = pcall(M.decode, response, { method = method, path = path })
      if ok then
        callback(nil, decoded)
      else
        callback(decoded)
      end
    end
  )
end

function M.server_request(config, method, path, payload, opts)
  return M.request(config, method, path, payload, opts)
end

function M.server_request_async(config, method, path, payload, callback, opts)
  return M.request_async(config, method, path, payload, callback, opts)
end

function M.database_request(config, method, path, payload, opts)
  return M.request(config, method, database_root(config) .. path, payload, opts)
end

function M.database_request_async(config, method, path, payload, callback, opts)
  return M.request_async(config, method, database_root(config) .. path, payload, callback, opts)
end

return M
