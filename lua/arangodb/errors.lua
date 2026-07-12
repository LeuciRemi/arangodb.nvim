--- Structured errors shared by the transport, client, and user interface.
local M = {}

local Error = {}
Error.__index = Error

function Error:__tostring()
  return M.format(self)
end

--- Create a structured ArangoDB error.
function M.new(fields)
  fields = vim.deepcopy(fields or {})
  fields.kind = fields.kind or "unknown"
  fields.message = fields.message or "ArangoDB operation failed"
  return setmetatable(fields, Error)
end

--- Report whether a value is a structured error, optionally of a given kind.
function M.is(value, kind)
  local matches = getmetatable(value) == Error
  return matches and (kind == nil or value.kind == kind)
end

--- Format an error for notifications without exposing request credentials or bodies.
function M.format(value)
  if not M.is(value) then
    return tostring(value)
  end

  local context = {}
  if value.status then
    context[#context + 1] = "HTTP " .. tostring(value.status)
  end
  if value.error_num then
    context[#context + 1] = "ArangoDB " .. tostring(value.error_num)
  end
  if value.method and value.path then
    context[#context + 1] = value.method .. " " .. value.path
  elseif value.path then
    context[#context + 1] = value.path
  end

  if #context == 0 then
    return value.message
  end
  return value.message .. "\n" .. table.concat(context, " · ")
end

return M
