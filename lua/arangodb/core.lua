local M = {}

local URL_SCHEMES = {
  arangodb = "http",
  http = "http",
  https = "https",
  ["arangodb+http"] = "http",
  ["arangodb+https"] = "https",
}

local ENV_SCHEME_ALIASES = {
  ssl = "https",
  tls = "https",
}

local function is_list(value)
  if vim.islist then
    return vim.islist(value)
  end
  if type(value) ~= "table" then
    return false
  end

  local count = 0
  for key, _ in pairs(value) do
    if type(key) ~= "number" or key < 1 or key % 1 ~= 0 then
      return false
    end
    count = count + 1
  end

  for index = 1, count do
    if value[index] == nil then
      return false
    end
  end

  return true
end

local function plugin_config()
  return require("arangodb.config").get()
end

function M.env(name, default)
  local value = vim.env[name]
  if value == nil or value == "" then
    return default
  end
  return value
end

function M.database_env_key(database)
  return "NVIM_ARANGO_" .. database:upper():gsub("[^%w]", "_") .. "_URL"
end

function M.url_encode(value)
  return (
    tostring(value):gsub("[^%w%-_%.~]", function(char)
      return string.format("%%%02X", string.byte(char))
    end)
  )
end

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

function M.default_transport_scheme()
  return normalize_scheme(M.env("NVIM_ARANGO_SCHEME", "http"), true) or "http"
end

function M.https_transport_available()
  return vim.fn.executable("curl") == 1
end


function M.transport_display()
  if M.https_transport_available() then
    return "built-in Lua HTTP for http:// and arangodb://, curl for https://"
  end
  return "built-in Lua HTTP for http:// and arangodb:// (install curl for https://)"
end

function M.arango_url(database)
  local specific_env = database == "_system" and "NVIM_ARANGO_SYSTEM_URL" or M.database_env_key(database)
  local explicit = M.env(specific_env, nil)
  if explicit ~= nil then
    return explicit
  end

  local scheme = M.default_transport_scheme()
  local url_scheme = scheme == "https" and "https" or "arangodb"

  return string.format(
    "%s://%s:%s@%s:%s/%s",
    url_scheme,
    M.url_encode(M.env("NVIM_ARANGO_USER", "root")),
    M.url_encode(M.env("NVIM_ARANGO_PASSWORD", "root")),
    M.env("NVIM_ARANGO_HOST", "127.0.0.1"),
    M.env("NVIM_ARANGO_PORT", "8529"),
    M.url_encode(database)
  )
end

function M.parse_connection(url)
  if type(url) ~= "string" or url == "" then
    return nil
  end

  local raw_scheme, remainder = url:match("^([%w%+%-%.]+)://(.+)$")
  local scheme = normalize_scheme(raw_scheme, false)
  if not scheme then
    return nil
  end

  local userinfo, host_path = remainder:match("^([^@]+)@(.+)$")
  if not userinfo then
    return nil
  end

  local user, password = userinfo:match("^([^:]+):?(.*)$")
  if not user or user == "" then
    return nil
  end

  local authority, database = host_path:match("^([^/]+)/(.+)$")
  if not authority or database == "" then
    return nil
  end

  local host, port = authority:match("^([^:]+):?(%d*)$")
  if not host or host == "" then
    return nil
  end

  return {
    scheme = scheme,
    user = M.url_decode(user),
    password = M.url_decode(password),
    host = host,
    port = port ~= "" and port or "8529",
    database = M.url_decode(database),
  }
end

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

local function collect_connections(source, items, seen)
  if type(source) ~= "table" then
    return
  end

  if is_list(source) then
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
  if options.legacy_globals then
    collect_connections(vim.g.arango_connections, items, seen)
    collect_connections(vim.g.dbs, items, seen)
  end

  return items, seen
end

function M.discover_databases()
  local fallback = { "_system" }

  local ok, output = pcall(function()
    return require("arangodb.client").list_databases({
      scheme = M.default_transport_scheme(),
      host = M.env("NVIM_ARANGO_HOST", "127.0.0.1"),
      port = M.env("NVIM_ARANGO_PORT", "8529"),
      user = M.env("NVIM_ARANGO_USER", "root"),
      password = M.env("NVIM_ARANGO_PASSWORD", "root"),
      database = "_system",
    })
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

function M.available_databases()
  local items, seen = configured_connections()

  for _, database in ipairs(M.discover_databases()) do
    add_connection(items, seen, database, M.arango_url(database))
  end

  table.sort(items, function(a, b)
    return a.name < b.name
  end)

  return items
end

function M.find_database(name)
  for _, item in ipairs(M.available_databases()) do
    if item.name == name then
      return item
    end
  end
end

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
