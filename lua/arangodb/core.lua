--- Shared helpers for connection discovery, URL handling, and notifications.
local M = {}

local utils = require("arangodb.utils")

local URL_SCHEMES = {
  http = "http",
  https = "https",
}

local ENV_SCHEME_ALIASES = {
  ssl = "https",
  tls = "https",
}

local function plugin_config()
  return require("arangodb.config").get()
end

--- Read an environment variable and fall back to the provided default.
function M.env(name, default)
  local value = vim.env[name]
  if value == nil or value == "" then
    return default
  end
  return value
end

--- Return the environment variable name used for a specific database URL.
function M.database_env_key(database)
  return "NVIM_ARANGO_" .. database:upper():gsub("[^%w]", "_") .. "_URL"
end

--- Percent-encode a value for use inside connection URLs.
function M.url_encode(value)
  return (
    tostring(value):gsub("[^%w%-_%.~]", function(char)
      return string.format("%%%02X", string.byte(char))
    end)
  )
end

--- Decode a percent-encoded value taken from a connection URL.
function M.url_decode(value)
  return (tostring(value):gsub("%%(%x%x)", function(hex)
    return string.char(tonumber(hex, 16))
  end))
end

local function normalize_scheme(value, allow_env_aliases)
  if type(value) ~= "string" or value == "" then
    return nil
  end

  local lowered = value:lower()
  local normalized = URL_SCHEMES[lowered]
  if normalized ~= nil then
    return normalized
  end

  if allow_env_aliases then
    return ENV_SCHEME_ALIASES[lowered]
  end
end

--- Resolve the default transport scheme used for generated connection URLs.
function M.default_transport_scheme()
  return normalize_scheme(M.env("NVIM_ARANGO_SCHEME", "http"), true) or "http"
end

--- Report whether HTTPS support is available through curl.
function M.https_transport_available()
  return vim.fn.executable("curl") == 1
end

--- Summarize the available HTTP transports for health reporting.
function M.transport_display()
  if M.https_transport_available() then
    return "built-in Lua HTTP for http://, curl for https://"
  end
  return "built-in Lua HTTP for http:// (install curl for https://)"
end

--- Build a connection URL for the requested database from environment defaults.
function M.arango_url(database)
  local specific_env = database == "_system" and "NVIM_ARANGO_SYSTEM_URL" or M.database_env_key(database)
  local explicit = M.env(specific_env, nil)
  if explicit ~= nil then
    return explicit
  end

  local scheme = M.default_transport_scheme()
  local host = M.env("NVIM_ARANGO_HOST", "127.0.0.1")
  if host:find(":", 1, true) and not host:match("^%[.*%]$") then
    host = "[" .. host .. "]"
  end

  return string.format(
    "%s://%s:%s@%s:%s/%s",
    scheme,
    M.url_encode(M.env("NVIM_ARANGO_USER", "root")),
    M.url_encode(M.env("NVIM_ARANGO_PASSWORD", "root")),
    host,
    M.env("NVIM_ARANGO_PORT", "8529"),
    M.url_encode(database)
  )
end

local function split_authority(authority)
  if authority:sub(1, 1) == "[" then
    local bracketed_host, bracketed_port = authority:match("^%[([^%]]+)%]:(%d+)$")
    if bracketed_host then
      return bracketed_host, bracketed_port
    end

    bracketed_host = authority:match("^%[([^%]]+)%]$")
    if bracketed_host then
      return bracketed_host, ""
    end

    return nil, nil
  end

  local host, port = authority:match("^([^:]+):(%d+)$")
  if host then
    return host, port
  end

  host = authority:match("^([^:]+)$")
  return host, host and "" or nil
end

--- Parse a connection URL into the fields used by the HTTP client.
function M.parse_connection(url)
  if type(url) ~= "string" or url == "" then
    return nil
  end

  local raw_scheme, remainder = url:match("^([%w%+%-%.]+)://(.+)$")
  local scheme = normalize_scheme(raw_scheme, false)
  if not scheme then
    return nil
  end

  local userinfo, host_path = remainder:match("^(.*)@([^@]+)$")
  if not host_path then
    host_path = remainder
  end

  local user, password
  if userinfo then
    user, password = userinfo:match("^([^:]*):?(.*)$")
    if not user then
      return nil
    end
  end

  local authority, database = host_path:match("^([^/]+)/([^/?#]+)/?$")
  if not authority or database == "" then
    return nil
  end

  local host, port = split_authority(authority)
  if not host or host == "" then
    return nil
  end

  local port_number = port ~= "" and tonumber(port) or 8529
  if not port_number or port_number < 1 or port_number > 65535 then
    return nil
  end

  return {
    scheme = scheme,
    user = user and M.url_decode(user) or nil,
    password = password and M.url_decode(password) or nil,
    host = host,
    port = port_number,
    database = M.url_decode(database),
  }
end

--- Display a normalized user-facing error message.
function M.notify_error(err, title)
  if type(err) == "string" then
    err = vim.trim(err)
  end
  if err == nil or err == "" then
    err = title or "ArangoDB operation failed"
  end
  vim.notify(err, vim.log.levels.ERROR, title and { title = title } or nil)
end

local function add_connection(items, seen, name, url)
  if type(name) ~= "string" or name == "" then
    return
  end
  if type(url) ~= "string" or url == "" or M.parse_connection(url) == nil then
    return
  end
  if seen[name] then
    return
  end

  seen[name] = true
  items[#items + 1] = {
    name = name,
    url = url,
  }
end

--- Merge configured connection tables into a de-duplicated list.
local function collect_connections(source, items, seen)
  if type(source) ~= "table" then
    return
  end

  if utils.is_list(source) then
    for _, entry in ipairs(source) do
      if type(entry) == "table" then
        add_connection(items, seen, entry.name, entry.url)
      end
    end
    return
  end

  for name, url in pairs(source) do
    add_connection(items, seen, name, url)
  end
end

local function configured_connections()
  local items = {}
  local seen = {}
  local options = plugin_config()

  collect_connections(options.connections, items, seen)
  collect_connections(vim.g.arangodb_connections, items, seen)

  for name, url in pairs(vim.fn.environ()) do
    if type(url) == "string" and url ~= "" and name:match("^NVIM_ARANGO_.+_URL$") then
      local connection = M.parse_connection(url)
      if connection then
        add_connection(items, seen, connection.database, url)
      end
    end
  end

  return items, seen
end

--- Discover databases from the default server connection, falling back to _system.
function M.discover_databases()
  local fallback = { "_system" }
  local system_connection = M.parse_connection(M.arango_url("_system"))
  if not system_connection then
    return fallback
  end

  local ok, output = pcall(function()
    return require("arangodb.client").list_databases(system_connection)
  end)
  if not ok then
    return fallback
  end

  output = vim.tbl_map(vim.trim, output)
  output = vim.tbl_filter(function(database)
    return database ~= ""
  end, output)

  if vim.tbl_isempty(output) then
    return fallback
  end

  return output
end

--- Return the full list of configured and discovered database connections.
function M.available_databases()
  local items, seen = configured_connections()

  if plugin_config().auto_discover then
    for _, database in ipairs(M.discover_databases()) do
      add_connection(items, seen, database, M.arango_url(database))
    end
  end

  table.sort(items, function(a, b)
    return a.name < b.name
  end)

  return items
end

--- Find a named database from the available connection list.
function M.find_database(name)
  for _, item in ipairs(M.available_databases()) do
    if item.name == name then
      return item
    end
  end
end

--- Resolve the database that should be opened first by the browser.
function M.default_database()
  local preferred = plugin_config().default_database
  if type(preferred) == "table" and preferred.name and preferred.url then
    return preferred
  end

  if type(preferred) == "string" and preferred ~= "" then
    return M.find_database(preferred) or {
      name = preferred,
      url = M.arango_url(preferred),
    }
  end

  local items = M.available_databases()
  if #items == 0 then
    return nil
  end

  return items[1]
end

return M
