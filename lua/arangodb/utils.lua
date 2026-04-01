--- Shared utility functions used across ArangoDB modules.
local M = {}

--- Check whether a Lua table is a list (sequential integer keys starting at 1).
function M.is_list(value)
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

--- Pretty-print a Lua value as indented JSON text.
--- When indent is omitted the configured json_indent option is used.
function M.json_pretty(value, indent, depth)
  indent = indent or require("arangodb.config").get().json_indent or 2
  depth = depth or 0

  if value == vim.NIL then
    return "null"
  end

  if type(value) ~= "table" then
    return vim.json.encode(value)
  end

  local current_indent = string.rep(" ", depth * indent)
  local next_indent = string.rep(" ", (depth + 1) * indent)

  if M.is_list(value) then
    if vim.tbl_isempty(value) then
      return "[]"
    end

    local items = {}
    for _, item in ipairs(value) do
      items[#items + 1] = next_indent .. M.json_pretty(item, indent, depth + 1)
    end

    return string.format("[\n%s\n%s]", table.concat(items, ",\n"), current_indent)
  end

  local keys = {}
  for key, _ in pairs(value) do
    keys[#keys + 1] = key
  end
  table.sort(keys, function(left, right)
    return tostring(left) < tostring(right)
  end)

  if #keys == 0 then
    return "{}"
  end

  local items = {}
  for _, key in ipairs(keys) do
    items[#items + 1] = string.format(
      "%s%s: %s",
      next_indent,
      vim.json.encode(tostring(key)),
      M.json_pretty(value[key], indent, depth + 1)
    )
  end

  return string.format("{\n%s\n%s}", table.concat(items, ",\n"), current_indent)
end

return M
