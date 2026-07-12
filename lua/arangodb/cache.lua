--- Small in-memory TTL cache for read-only ArangoDB metadata.
local M = {}

local uv = vim.uv or vim.loop
local entries = {}

local function now()
  return uv and uv.hrtime and math.floor(uv.hrtime() / 1000000)
    or math.floor(vim.fn.reltimefloat(vim.fn.reltime()) * 1000)
end

--- Return a cached value or nil when it is absent or expired.
function M.get(key)
  local entry = entries[key]
  if not entry then
    return nil
  end
  if entry.expires_at <= now() then
    entries[key] = nil
    return nil
  end
  return vim.deepcopy(entry.value)
end

--- Store a value for ttl milliseconds. A non-positive ttl disables storage.
function M.set(key, value, ttl)
  ttl = tonumber(ttl) or 0
  if ttl <= 0 then
    entries[key] = nil
    return value
  end
  entries[key] = {
    expires_at = now() + ttl,
    value = vim.deepcopy(value),
  }
  return value
end

--- Remove one cache key or every key sharing a prefix.
function M.invalidate(key, prefix)
  if prefix then
    for candidate, _ in pairs(entries) do
      if vim.startswith(candidate, key) then
        entries[candidate] = nil
      end
    end
    return
  end
  entries[key] = nil
end

--- Clear the complete cache.
function M.clear()
  entries = {}
end

return M
