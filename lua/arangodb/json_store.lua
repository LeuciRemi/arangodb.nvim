--- Shared JSON list storage with atomic writes and cross-instance locking.
--- Every read-modify-write goes through `update` so that two Neovim instances
--- writing the same file cannot silently lose each other's entries.
local M = {}

local uv = vim.uv or vim.loop

local DEFAULT_LOCK_TIMEOUT = 1000
local STALE_LOCK_SECONDS = 10
local LOCK_RETRY_MS = 10
local LOCKED_SUFFIX = " is locked by another Neovim instance"

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

--- Read the stored list, reporting whether the file held undecodable content.
--- A missing or empty file is an empty, healthy store.
function M.read(path)
  local content = read_file(path)
  if not content or content == "" then
    return {}
  end
  local ok, decoded = pcall(vim.json.decode, content)
  if not ok or type(decoded) ~= "table" or not require("arangodb.utils").is_list(decoded) then
    return {}, true
  end
  return decoded
end

--- Whether `err` reports a lock timeout raised by `update`.
function M.locked(err)
  local message = type(err) == "string" and err or ""
  return message:sub(-#LOCKED_SUFFIX) == LOCKED_SUFFIX
end

local function back_up_corrupt(path, label, title)
  local backup = string.format("%s.corrupt-%d", path, os.time())
  if uv and uv.fs_rename(path, backup) then
    vim.notify("Invalid " .. label .. " file backed up to " .. backup, vim.log.levels.WARN, { title = title })
  else
    vim.notify("Invalid " .. label .. " file could not be backed up", vim.log.levels.WARN, { title = title })
  end
end

local function write_entries(path, entries, label)
  assert(uv, "libuv is required to persist the " .. label)
  vim.fn.mkdir(vim.fs.dirname(path), "p")
  local temporary = string.format("%s.tmp.%s", path, tostring(vim.fn.getpid()))
  local fd = assert(uv.fs_open(temporary, "w", 384), "Unable to open temporary " .. label .. " file")
  local encoded = vim.json.encode(entries)
  local ok, written, write_err = pcall(uv.fs_write, fd, encoded, 0)
  uv.fs_close(fd)
  if not ok or not written then
    pcall(uv.fs_unlink, temporary)
    error(write_err or written or ("Unable to write " .. label .. " file"), 0)
  end
  local renamed, rename_err = uv.fs_rename(temporary, path)
  if not renamed then
    pcall(uv.fs_unlink, temporary)
    error(rename_err or ("Unable to replace " .. label .. " file"), 0)
  end
  pcall(uv.fs_chmod, path, 384)
end

local function is_stale(stat)
  return os.time() - stat.mtime.sec > STALE_LOCK_SECONDS
end

--- Take the exclusive lock, waiting and taking over stale locks until `timeout` ms.
local function acquire(path, timeout, label)
  local lock = path .. ".lock"
  vim.fn.mkdir(vim.fs.dirname(path), "p")
  local deadline = uv.now() + timeout
  while true do
    local fd, _, code = uv.fs_open(lock, "wx", 384)
    if fd then
      uv.fs_close(fd)
      return lock
    end
    if code ~= "EEXIST" then
      error(string.format("Unable to lock %s: %s", lock, tostring(code or fd)), 0)
    end
    local stat = uv.fs_stat(lock)
    if stat and is_stale(stat) then
      -- Crashed instance left its lock behind: reclaim it.
      pcall(uv.fs_unlink, lock)
    else
      if uv.now() >= deadline then
        error(label .. LOCKED_SUFFIX, 0)
      end
      vim.wait(LOCK_RETRY_MS)
    end
  end
end

--- Read, transform, and atomically rewrite the stored list under an exclusive lock.
--- `fn` receives the stored list and returns the new list, or `nil` to leave the
--- file untouched. A corrupt file is backed up before `fn` runs.
function M.update(path, fn, opts)
  opts = opts or {}
  local label = opts.label or "AQL store"
  local title = opts.title or ("ArangoDB " .. label)
  local lock = acquire(path, opts.lock_timeout or DEFAULT_LOCK_TIMEOUT, label)
  local ok, result = pcall(function()
    local entries, corrupt = M.read(path)
    if corrupt then
      back_up_corrupt(path, label, title)
    end
    local updated = fn(entries)
    if updated ~= nil then
      write_entries(path, updated, label)
    end
    return updated
  end)
  pcall(uv.fs_unlink, lock)
  if not ok then
    error(result, 0)
  end
  return result
end

return M
