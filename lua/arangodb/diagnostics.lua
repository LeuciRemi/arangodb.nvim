--- Optional JSON-lines diagnostic journal with sensitive fields excluded by design.
local M = {}

local uv = vim.uv or vim.loop

local function options()
  return require("arangodb.config").get().diagnostics or {}
end

--- Resolve the configured journal path.
function M.path()
  local opts = options()
  if type(opts.path) == "string" and opts.path ~= "" then
    return vim.fn.expand(opts.path)
  end
  return vim.fs.joinpath(vim.fn.stdpath("log"), "arangodb.nvim.log")
end

local function rotate(path, max_size)
  local stat = uv.fs_stat(path)
  if not stat or stat.size < max_size then
    return
  end
  pcall(uv.fs_unlink, path .. ".1")
  pcall(uv.fs_rename, path, path .. ".1")
end

--- Append one sanitized request event to the journal when diagnostics are enabled.
function M.record(event)
  local opts = options()
  if opts.enabled ~= true or not uv then
    return
  end

  local path = M.path()
  vim.fn.mkdir(vim.fs.dirname(path), "p")
  rotate(path, opts.max_size or 1048576)

  local safe = {
    timestamp = os.date("!%Y-%m-%dT%H:%M:%SZ"),
    method = event.method,
    scheme = event.scheme,
    host = event.host,
    port = event.port,
    path = event.path,
    status = event.status,
    duration_ms = event.duration_ms,
    outcome = event.outcome,
    error_kind = event.error_kind,
  }
  local line = vim.json.encode(safe) .. "\n"
  local fd = uv.fs_open(path, "a", 384)
  if fd then
    pcall(uv.fs_write, fd, line, -1)
    pcall(uv.fs_close, fd)
  end
end

return M
