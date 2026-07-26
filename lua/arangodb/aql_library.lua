--- Persistent named AQL queries without connection credentials.
local M = {}

local uv = vim.uv or vim.loop

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

local function read_file(path)
  local fd = uv and uv.fs_open(path, "r", 384) or nil
  if not fd then
    return nil
  end
  local stat = uv.fs_fstat(fd)
  local content = stat and uv.fs_read(fd, stat.size, 0) or nil
  uv.fs_close(fd)
  return content
end

local function valid_entry(entry)
  return type(entry) == "table"
    and type(entry.name) == "string"
    and entry.name ~= ""
    and type(entry.query) == "string"
    and entry.query ~= ""
end

--- Load named queries and return an optional warning for a corrupt file.
function M.load()
  local content = read_file(M.path())
  if not content or content == "" then
    return {}
  end
  local ok, decoded = pcall(vim.json.decode, content)
  if not ok or type(decoded) ~= "table" or not require("arangodb.utils").is_list(decoded) then
    return {}, "Invalid AQL library file: " .. M.path()
  end
  local entries = {}
  for _, entry in ipairs(decoded) do
    if valid_entry(entry) then
      entries[#entries + 1] = entry
    end
  end
  return entries
end

local function write_entries(entries)
  assert(uv, "libuv is required to persist the AQL library")
  local path = M.path()
  vim.fn.mkdir(vim.fs.dirname(path), "p")
  local temporary = string.format("%s.tmp.%s", path, tostring(vim.fn.getpid()))
  local fd = assert(uv.fs_open(temporary, "w", 384), "Unable to open temporary AQL library file")
  local encoded = vim.json.encode(entries)
  local ok, written, write_err = pcall(uv.fs_write, fd, encoded, 0)
  uv.fs_close(fd)
  if not ok or not written then
    pcall(uv.fs_unlink, temporary)
    error(write_err or written or "Unable to write AQL library file", 0)
  end
  local renamed, rename_err = uv.fs_rename(temporary, path)
  if not renamed then
    pcall(uv.fs_unlink, temporary)
    error(rename_err or "Unable to replace AQL library file", 0)
  end
  pcall(uv.fs_chmod, path, 384)
end

local function same_identity(left, right)
  return left.name == right.name and left.connection == right.connection and left.database == right.database
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
  local entries = { saved }
  for _, existing in ipairs(M.load()) do
    if not same_identity(existing, saved) then
      entries[#entries + 1] = existing
    end
  end
  write_entries(entries)
  return saved
end

--- Remove a named query and return whether it existed.
function M.remove(entry)
  local retained = {}
  local removed = false
  for _, existing in ipairs(M.load()) do
    if same_identity(existing, entry) then
      removed = true
    else
      retained[#retained + 1] = existing
    end
  end
  if removed then
    write_entries(retained)
  end
  return removed
end

return M
