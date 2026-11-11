--- Persistent, credential-free history for AQL editor sessions.
local M = {}

local store = require("arangodb.json_store")

local LABEL = "AQL history"
local TITLE = "ArangoDB AQL History"

local function history_options()
  return ((require("arangodb.config").get().aql or {}).history or {})
end

--- Resolve the configured AQL history path.
function M.path()
  local opts = history_options()
  if type(opts.path) == "string" and opts.path ~= "" then
    return vim.fn.expand(opts.path)
  end
  return vim.fs.joinpath(vim.fn.stdpath("data"), "arangodb.nvim", "aql_history.json")
end

local function valid_entry(entry)
  return type(entry) == "table" and type(entry.query) == "string"
end

local function valid_entries(entries)
  local result = {}
  for _, entry in ipairs(entries) do
    if valid_entry(entry) then
      result[#result + 1] = entry
    end
  end
  return result
end

local function same_entry(left, right)
  return left.connection == right.connection
    and left.database == right.database
    and left.query == right.query
    and vim.deep_equal(left.bind_vars or {}, right.bind_vars or {})
end

--- Load valid history entries, returning an optional warning for corrupt files.
function M.load()
  local opts = history_options()
  if opts.enabled == false then
    return {}
  end
  local entries, corrupt = store.read(M.path())
  if corrupt then
    return {}, "Invalid AQL history file: " .. M.path()
  end
  return valid_entries(entries)
end

--- Add or move an entry to the front of the bounded local history.
--- Skipped with a warning, never raising into the AQL flow, when another
--- Neovim instance holds the store lock.
function M.add(entry)
  local opts = history_options()
  if opts.enabled == false then
    return {}
  end
  local sanitized = {
    timestamp = entry.timestamp or os.date("!%Y-%m-%dT%H:%M:%SZ"),
    connection = entry.connection,
    database = entry.database,
    query = entry.query,
  }
  if opts.store_bind_vars ~= false then
    sanitized.bind_vars = vim.deepcopy(entry.bind_vars or {})
  end

  local ok, entries = pcall(store.update, M.path(), function(stored)
    local updated = { sanitized }
    for _, item in ipairs(valid_entries(stored)) do
      local retained = vim.deepcopy(item)
      if opts.store_bind_vars == false then
        retained.bind_vars = nil
      end
      if not same_entry(retained, sanitized) then
        updated[#updated + 1] = retained
      end
    end
    while #updated > (opts.max_entries or 100) do
      updated[#updated] = nil
    end
    return updated
  end, { label = LABEL, title = TITLE })
  if not ok then
    if not store.locked(entries) then
      error(entries, 0)
    end
    vim.notify(tostring(entries), vim.log.levels.WARN, { title = TITLE })
    return M.load()
  end
  return entries
end

--- Remove the history entry with the identity used by `add` and report whether it existed.
function M.remove(entry)
  local opts = history_options()
  if opts.enabled == false then
    return false
  end
  -- Entries come from `load()`: match them as stored, whatever `store_bind_vars` says now.
  local target = {
    connection = entry.connection,
    database = entry.database,
    query = entry.query,
    bind_vars = vim.deepcopy(entry.bind_vars or {}),
  }

  local removed = false
  store.update(M.path(), function(stored)
    local retained = {}
    for _, item in ipairs(valid_entries(stored)) do
      if not removed and same_entry(item, target) then
        removed = true
      else
        retained[#retained + 1] = item
      end
    end
    return removed and retained or nil
  end, { label = LABEL, title = TITLE })
  return removed
end

--- Remove every history entry and return how many were removed.
function M.clear()
  local opts = history_options()
  if opts.enabled == false then
    return 0
  end
  local count = 0
  store.update(M.path(), function(stored)
    count = #valid_entries(stored)
    if count == 0 then
      return nil
    end
    return {}
  end, { label = LABEL, title = TITLE })
  return count
end

return M
