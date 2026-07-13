--- Configuration defaults and accessors shared by all modules.
local M = {}

--- Default options applied when the user does not override them in setup().
M.defaults = {
  connections = nil,
  default_database = nil,
  auto_discover = false,
  keymaps = {
    browse = nil,
    resume = nil,
    back = nil,
  },
  picker_keymaps = {
    execute = "<C-x>",
    create = "<C-a>",
    create_collection = "<C-n>",
    duplicate_collection = "<C-d>",
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
  aql_keymaps = {
    execute = "<leader>ar",
    validate = "<leader>av",
    explain = "<leader>ae",
    profile = "<leader>ap",
    bind_vars = "<leader>ab",
    history = "<leader>ah",
    cancel = "<leader>ac",
    next_page = "<C-n>",
    prev_page = "<C-p>",
  },
  aql = {
    batch_size = 100,
    cursor_ttl = 300,
    max_runtime = nil,
    result_split = "auto",
    history = {
      enabled = true,
      max_entries = 100,
      path = nil,
      store_bind_vars = true,
    },
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
  cache_ttl = 5000,
  default_sort = "doc._key ASC",
  show_system_collections = false,
  http_timeout = 30000,
  tls_verify = true,
  tls_ca_file = nil,
  diagnostics = {
    enabled = false,
    path = nil,
    max_size = 1048576,
  },
}

local options = vim.deepcopy(M.defaults)

local function assert_type(name, value, expected, optional)
  if optional and value == nil then
    return
  end
  if type(value) ~= expected then
    error(string.format("arangodb.nvim: `%s` must be %s, got %s", name, expected, type(value)), 3)
  end
end

local function assert_positive_integer(name, value)
  if type(value) ~= "number" or value < 1 or value % 1 ~= 0 then
    error(string.format("arangodb.nvim: `%s` must be a positive integer", name), 3)
  end
end

local function assert_nonnegative_integer(name, value)
  if type(value) ~= "number" or value < 0 or value % 1 ~= 0 then
    error(string.format("arangodb.nvim: `%s` must be a non-negative integer", name), 3)
  end
end

local function assert_nonnegative_number(name, value)
  if type(value) ~= "number" or value < 0 then
    error(string.format("arangodb.nvim: `%s` must be a non-negative number", name), 3)
  end
end

local function validate_keymaps(name, keymaps)
  if keymaps == nil then
    return
  end
  assert_type(name, keymaps, "table")
  for key, value in pairs(keymaps) do
    if value ~= false and type(value) ~= "string" then
      error(string.format("arangodb.nvim: `%s.%s` must be a string or false", name, tostring(key)), 3)
    end
  end
end

local function validate_connections(connections)
  if connections == nil then
    return
  end
  assert_type("connections", connections, "table")

  if vim.islist(connections) then
    for index, entry in ipairs(connections) do
      if type(entry) ~= "table" or type(entry.name) ~= "string" or type(entry.url) ~= "string" then
        error(string.format("arangodb.nvim: `connections[%d]` must contain string `name` and `url` fields", index), 3)
      end
    end
    return
  end

  for name, url in pairs(connections) do
    if type(name) ~= "string" or type(url) ~= "string" then
      error("arangodb.nvim: `connections` keys and values must be strings", 3)
    end
  end
end

local function validate(opts)
  assert_type("opts", opts, "table")
  validate_connections(opts.connections)
  if
    opts.default_database ~= nil
    and type(opts.default_database) ~= "string"
    and type(opts.default_database) ~= "table"
  then
    error("arangodb.nvim: `default_database` must be a string or table", 3)
  end
  if
    type(opts.default_database) == "table"
    and (type(opts.default_database.name) ~= "string" or type(opts.default_database.url) ~= "string")
  then
    error("arangodb.nvim: table `default_database` must contain string `name` and `url` fields", 3)
  end

  validate_keymaps("keymaps", opts.keymaps)
  validate_keymaps("picker_keymaps", opts.picker_keymaps)
  validate_keymaps("document_keymaps", opts.document_keymaps)
  validate_keymaps("aql_keymaps", opts.aql_keymaps)
  assert_type("layout", opts.layout, "table", true)
  assert_type("aql", opts.aql, "table", true)

  for _, name in ipairs({
    "field_sample_size",
    "page_size",
    "json_indent",
    "truncate_length",
    "max_field_depth",
    "aql_batch_size",
    "http_timeout",
  }) do
    if opts[name] ~= nil then
      assert_positive_integer(name, opts[name])
    end
  end

  if opts.cache_ttl ~= nil then
    assert_nonnegative_integer("cache_ttl", opts.cache_ttl)
  end

  assert_type("default_sort", opts.default_sort, "string", true)
  assert_type("auto_discover", opts.auto_discover, "boolean", true)
  assert_type("show_system_collections", opts.show_system_collections, "boolean", true)
  assert_type("tls_verify", opts.tls_verify, "boolean", true)
  assert_type("tls_ca_file", opts.tls_ca_file, "string", true)
  assert_type("diagnostics", opts.diagnostics, "table", true)

  if opts.layout then
    assert_type("layout.preset", opts.layout.preset, "string", true)
    assert_type("layout.preview", opts.layout.preview, "boolean", true)
  end
  if opts.diagnostics then
    assert_type("diagnostics.enabled", opts.diagnostics.enabled, "boolean", true)
    assert_type("diagnostics.path", opts.diagnostics.path, "string", true)
    if opts.diagnostics.max_size ~= nil then
      assert_positive_integer("diagnostics.max_size", opts.diagnostics.max_size)
    end
  end
  if opts.aql then
    if opts.aql.batch_size ~= nil then
      assert_positive_integer("aql.batch_size", opts.aql.batch_size)
    end
    if opts.aql.cursor_ttl ~= nil then
      assert_positive_integer("aql.cursor_ttl", opts.aql.cursor_ttl)
    end
    if opts.aql.max_runtime ~= nil then
      assert_nonnegative_number("aql.max_runtime", opts.aql.max_runtime)
    end
    if
      opts.aql.result_split ~= nil
      and opts.aql.result_split ~= "auto"
      and opts.aql.result_split ~= "right"
      and opts.aql.result_split ~= "bottom"
    then
      error("arangodb.nvim: `aql.result_split` must be `auto`, `right`, or `bottom`", 3)
    end
    assert_type("aql.history", opts.aql.history, "table", true)
    if opts.aql.history then
      assert_type("aql.history.enabled", opts.aql.history.enabled, "boolean", true)
      assert_type("aql.history.path", opts.aql.history.path, "string", true)
      assert_type("aql.history.store_bind_vars", opts.aql.history.store_bind_vars, "boolean", true)
      if opts.aql.history.max_entries ~= nil then
        assert_positive_integer("aql.history.max_entries", opts.aql.history.max_entries)
      end
    end
  end
end

--- Store the merged plugin options.
function M.setup(opts)
  opts = opts or {}
  validate(opts)
  options = vim.tbl_deep_extend("force", vim.deepcopy(M.defaults), vim.deepcopy(opts))
  local loaded_cache = package.loaded["arangodb.cache"]
  if loaded_cache then
    loaded_cache.clear()
  end
  return options
end

--- Return the current plugin options table.
function M.get()
  return options
end

return M
