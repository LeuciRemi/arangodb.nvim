local M = {}

M.defaults = {
  connections = nil,
  default_database = nil,
  python_command = "python3",
  runner = nil,
  keymaps = {
    browse = nil,
    resume = nil,
  },
  document_keymaps = {
    save = nil,
    delete = nil,
    related = nil,
  },
  field_sample_size = 200,
  page_size = 50,
  legacy_globals = true,
}

local options = vim.deepcopy(M.defaults)

function M.setup(opts)
  options = vim.tbl_deep_extend("force", vim.deepcopy(M.defaults), opts or {})
  return options
end

function M.get()
  return options
end

return M
