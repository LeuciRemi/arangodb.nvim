--- Configuration defaults and accessors shared by all modules.
local M = {}

--- Default options applied when the user does not override them in setup().
M.defaults = {
  connections = nil,
  default_database = nil,
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
  http_timeout = 30000,
  tls_verify = true,
  tls_ca_file = nil,
}

local options = vim.deepcopy(M.defaults)

--- Store the merged plugin options.
function M.setup(opts)
  options = vim.tbl_deep_extend("force", vim.deepcopy(M.defaults), opts or {})
  return options
end

--- Return the current plugin options table.
function M.get()
  return options
end

return M
