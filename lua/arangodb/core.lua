local M = {}

local uv = vim.uv or vim.loop

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

local function python_base_command()
  local command = plugin_config().python_command
  if type(command) == "table" then
    return vim.deepcopy(command)
  end
  if type(command) == "string" and command ~= "" then
    return { command }
  end
  return { "python3" }
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

function M.runner_script()
  local configured = plugin_config().runner
  if type(configured) == "function" then
    configured = configured()
  end
  if type(configured) == "string" and configured ~= "" then
    return configured
  end

  local matches = vim.api.nvim_get_runtime_file("python/arango_browser.py", true)
  if #matches > 0 then
    return matches[1]
  end

  matches = vim.api.nvim_get_runtime_file("scripts/arango_browser.py", true)
  if #matches > 0 then
    return matches[1]
  end

  return nil
end

function M.script_exists()
  local runner = M.runner_script()
  return runner ~= nil and uv.fs_stat(runner) ~= nil
end

function M.python_binary()
  return python_base_command()[1]
end

function M.python_command_display()
  return table.concat(python_base_command(), " ")
end

function M.arango_url(database)
  local specific_env = database == "_system" and "NVIM_ARANGO_SYSTEM_URL" or M.database_env_key(database)
  local explicit = M.env(specific_env, nil)
  if explicit ~= nil then
    return explicit
  end

  return string.format(
    "arangodb://%s:%s@%s:%s/%s",
    M.url_encode(M.env("NVIM_ARANGO_USER", "root")),
    M.url_encode(M.env("NVIM_ARANGO_PASSWORD", "root")),
    M.env("NVIM_ARANGO_HOST", "127.0.0.1"),
    M.env("NVIM_ARANGO_PORT", "8529"),
    M.url_encode(database)
  )
end

function M.parse_connection(url)
  local user, password, host, port, database = url:match("^arangodb://([^:]+):([^@]*)@([^:/]+):?(%d*)/(.+)$")
  if not user then
    return nil
  end

  return {
    user = M.url_decode(user),
    password = M.url_decode(password),
    host = host,
    port = port ~= "" and port or "8529",
    database = M.url_decode(database),
  }
end

function M.python_command(config, command, extra)
  local runner = M.runner_script()
  if not runner then
    error("ArangoDB runner script not found")
  end

  local cmd = python_base_command()
  cmd[#cmd + 1] = runner
  vim.list_extend(cmd, {
    "--host",
    config.host,
    "--port",
    config.port,
    "--user",
    config.user,
    "--password",
    config.password,
    "--database",
    config.database,
    command,
  })

  for _, arg in ipairs(extra or {}) do
    cmd[#cmd + 1] = tostring(arg)
  end

  return cmd
end

function M.run_lines(cmd)
  local output = vim.fn.systemlist(cmd)
  if vim.v.shell_error ~= 0 then
    local msg = #output > 0 and table.concat(output, "\n") or "ArangoDB command failed"
    error(msg)
  end
  return output
end

function M.run_json(cmd)
  local output = vim.fn.system(cmd)
  if vim.v.shell_error ~= 0 then
    error(output ~= "" and output or "ArangoDB command failed")
  end

  local ok, decoded = pcall(vim.json.decode, output)
  if not ok then
    error("Invalid JSON from ArangoDB runner")
  end

  return decoded
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
  if type(url) ~= "string" or url == "" or not url:match("^arangodb://") then
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
  if vim.fn.executable(M.python_binary()) ~= 1 or not M.script_exists() then
    return fallback
  end

  local cmd = python_base_command()
  cmd[#cmd + 1] = M.runner_script()
  vim.list_extend(cmd, {
    "--host",
    M.env("NVIM_ARANGO_HOST", "127.0.0.1"),
    "--port",
    M.env("NVIM_ARANGO_PORT", "8529"),
    "--user",
    M.env("NVIM_ARANGO_USER", "root"),
    "--password",
    M.env("NVIM_ARANGO_PASSWORD", "root"),
    "--database",
    "_system",
    "databases",
  })

  local ok, output = pcall(M.run_lines, cmd)
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
