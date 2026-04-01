--- Configuration defaults and accessors shared by all modules.
local M = {}

--- Default options applied when the user does not override them in setup().
M.defaults = {
  connections = nil,
  default_database = nil,
  keymaps = {
    browse = nil,
    resume = nil,
    back = nil,
  },
  picker_keymaps = {
    execute = "<C-x>",
    create = "<C-a>",
    next_page = "<C-n>",
    prev_page = "<C-p>",
    back = "<C-b>",
    change_field = "<C-f>",
    reset = "<C-u>",
    related = "<C-o>",
    delete = "<C-d>",
    duplicate = "<C-y>",
    truncate = "<C-t>",
    rename = "<C-r>",
  },
  document_keymaps = {
    save = nil,
    delete = nil,
    duplicate = nil,
    related = nil,
  },
  layout = {
    preset = "auto",
    preview = true,
  },
  field_sample_size = 200,
  page_size = 50,
  json_indent = 2,
  truncate_length = 120,
  max_field_depth = 4,
  aql_batch_size = 1000,
  default_sort = "doc._key ASC",
  show_system_collections = false,
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
