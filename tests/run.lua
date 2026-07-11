local files = vim.fn.globpath(vim.fn.getcwd(), "tests/*_spec.lua", false, true)
table.sort(files)

local total = 0
local failures = {}

for _, file in ipairs(files) do
  local tests = dofile(file)
  for _, test in ipairs(tests) do
    total = total + 1
    local ok, err = xpcall(test.callback, debug.traceback)
    if ok then
      vim.api.nvim_out_write("ok - " .. test.name .. "\n")
    else
      failures[#failures + 1] = { name = test.name, error = err }
      vim.api.nvim_err_write("not ok - " .. test.name .. "\n" .. err .. "\n")
    end
  end
end

vim.api.nvim_out_write(string.format("%d tests, %d failures\n", total, #failures))
if #failures > 0 then
  vim.cmd("cquit 1")
end
