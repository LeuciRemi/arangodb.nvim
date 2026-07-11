local M = {}

function M.eq(expected, actual, message)
  if not vim.deep_equal(expected, actual) then
    error(
      (message and (message .. ": ") or "") .. "expected " .. vim.inspect(expected) .. ", got " .. vim.inspect(actual),
      2
    )
  end
end

function M.matches(pattern, value)
  if type(value) ~= "string" or not value:match(pattern) then
    error(string.format("expected %s to match %s", vim.inspect(value), vim.inspect(pattern)), 2)
  end
end

function M.fails(pattern, callback)
  local ok, err = pcall(callback)
  if ok then
    error("expected callback to fail", 2)
  end
  M.matches(pattern, tostring(err))
end

function M.test(name, callback)
  return { name = name, callback = callback }
end

return M
