--- Persistent, credential-free history for AQL editor sessions.
local M = {}

local uv = vim.uv or vim.loop

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

--- Load valid history entries, returning an optional warning for corrupt files.
function M.load()
  local opts = history_options()
  if opts.enabled == false then
    return {}
  end
  local content = read_file(M.path())
  if not content or content == "" then
    return {}
  end
  local ok, decoded = pcall(vim.json.decode, content)
  if not ok or type(decoded) ~= "table" or not require("arangodb.utils").is_list(decoded) then
    return {}, "Invalid AQL history file: " .. M.path()
  end
  local entries = {}
  for _, entry in ipairs(decoded) do
    if type(entry) == "table" and type(entry.query) == "string" then
      entries[#entries + 1] = entry
    end
  end
  return entries
end

local function same_entry(left, right)
  return left.connection == right.connection
    and left.database == right.database
    and left.query == right.query
    and vim.deep_equal(left.bind_vars or {}, right.bind_vars or {})
end

local function write_entries(entries)
  assert(uv, "libuv is required to persist AQL history")
  local path = M.path()
  vim.fn.mkdir(vim.fs.dirname(path), "p")
  local temporary = string.format("%s.tmp.%s", path, tostring(vim.fn.getpid()))
  local fd = assert(uv.fs_open(temporary, "w", 384), "Unable to open temporary AQL history file")
  local encoded = vim.json.encode(entries)
  local ok, written, write_err = pcall(uv.fs_write, fd, encoded, 0)
  uv.fs_close(fd)
  if not ok or not written then
    pcall(uv.fs_unlink, temporary)
    error(write_err or written or "Unable to write AQL history file", 0)
  end
  local renamed, rename_err = uv.fs_rename(temporary, path)
  if not renamed then
    pcall(uv.fs_unlink, temporary)
    error(rename_err or "Unable to replace AQL history file", 0)
  end
  pcall(uv.fs_chmod, path, 384)
end

--- Add or move an entry to the front of the bounded local history.
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

  local existing = M.load()
  local entries = { sanitized }
  for _, item in ipairs(existing) do
    local retained = vim.deepcopy(item)
    if opts.store_bind_vars == false then
      retained.bind_vars = nil
    end
    if not same_entry(retained, sanitized) then
      entries[#entries + 1] = retained
    end
  end
  while #entries > (opts.max_entries or 100) do
    entries[#entries] = nil
  end
  write_entries(entries)
  return entries
end

return M
