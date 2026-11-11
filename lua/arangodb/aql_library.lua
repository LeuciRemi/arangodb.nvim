--- Persistent named AQL queries without connection credentials.
local M = {}

local store = require("arangodb.json_store")

local LABEL = "AQL library"
local TITLE = "ArangoDB AQL Library"

local function options()
  return ((require("arangodb.config").get().aql or {}).library or {})
end

--- Resolve the configured named-query library path.
function M.path()
  local opts = options()
  if type(opts.path) == "string" and opts.path ~= "" then
    return vim.fn.expand(opts.path)
  end
  return vim.fs.joinpath(vim.fn.stdpath("data"), "arangodb.nvim", "aql_library.json")
end

local function valid_entry(entry)
  return type(entry) == "table"
    and type(entry.name) == "string"
    and entry.name ~= ""
    and type(entry.query) == "string"
    and entry.query ~= ""
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

local function same_identity(left, right)
  return left.name == right.name and left.connection == right.connection and left.database == right.database
end

--- Load named queries and return an optional warning for a corrupt file.
function M.load()
  local entries, corrupt = store.read(M.path())
  if corrupt then
    return {}, "Invalid AQL library file: " .. M.path()
  end
  return valid_entries(entries)
end

--- Return the stored query sharing the name, connection, and database of `entry`.
function M.find(entry)
  local lookup = {
    name = vim.trim(type(entry.name) == "string" and entry.name or ""),
    connection = entry.connection,
    database = entry.database,
  }
  for _, existing in ipairs(M.load()) do
    if same_identity(existing, lookup) then
      return existing
    end
  end
end

--- Create or replace a named query in its connection/database scope.
function M.put(entry)
  assert(valid_entry(entry), "A named AQL query requires a non-empty name and query")
  local name = vim.trim(entry.name)
  assert(name ~= "", "A named AQL query requires a non-empty name and query")
  local saved = {
    name = name,
    connection = entry.connection,
    database = entry.database,
    query = entry.query,
    bind_vars = vim.deepcopy(entry.bind_vars or {}),
    updated_at = os.date("!%Y-%m-%dT%H:%M:%SZ"),
  }
  store.update(M.path(), function(stored)
    local entries = { saved }
    for _, existing in ipairs(valid_entries(stored)) do
      if not same_identity(existing, saved) then
        entries[#entries + 1] = existing
      end
    end
    return entries
  end, { label = LABEL, title = TITLE })
  return saved
end

--- Remove a named query and return whether it existed.
function M.remove(entry)
  local removed = false
  store.update(M.path(), function(stored)
    local retained = {}
    for _, existing in ipairs(valid_entries(stored)) do
      if same_identity(existing, entry) then
        removed = true
      else
        retained[#retained + 1] = existing
      end
    end
    return removed and retained or nil
  end, { label = LABEL, title = TITLE })
  return removed
end

return M
