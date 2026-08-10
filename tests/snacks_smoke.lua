local database = {
  scheme = "http",
  host = "localhost",
  port = 8529,
  database = "test",
}

local function picker_action(picker, action)
  for _, mapping in pairs(picker.opts.win.input.keys or {}) do
    if type(mapping) == "table" and mapping[1] == action then
      return mapping
    end
  end
end

local function supports_mode(mapping, mode)
  local modes = type(mapping.mode) == "table" and mapping.mode or { mapping.mode }
  return vim.tbl_contains(modes, mode)
end

local function assert_navigation_modes(picker, actions)
  for _, action in ipairs(actions) do
    local mapping = assert(picker_action(picker, action), "Missing picker action " .. action)
    assert(supports_mode(mapping, "n"), action .. " is not available in normal mode")
    assert(supports_mode(mapping, "i"), action .. " is not available in insert mode")
  end
end

local function assert_actions_unmapped(picker, actions)
  for _, action in ipairs(actions) do
    assert(picker_action(picker, action) == nil, action .. " should not have a default mapping")
  end
end

local function assert_normal_only(picker, action)
  local mapping = assert(picker_action(picker, action), "Missing configured picker action " .. action)
  assert(supports_mode(mapping, "n"), action .. " is not available in normal mode")
  assert(not supports_mode(mapping, "i"), action .. " should not be available in insert mode")
end

package.loaded["arangodb.client"] = {
  list_databases = function()
    return { "test" }
  end,
  list_collections = function()
    return { "items" }
  end,
  list_collection_details_async = function(_, done)
    vim.schedule(function()
      done(nil, {
        { name = "items", type = "document", status = "loaded" },
      })
    end)
    return { cancel = function() end }
  end,
  collection_metrics_async = function(_, _, done)
    vim.schedule(function()
      done(nil, { count = 1, size = 42 })
    end)
    return { cancel = function() end }
  end,
  database_overview_async = function(_, opts, done)
    vim.schedule(function()
      local collections = vim.deepcopy(opts.collections)
      collections[1].count = 1
      collections[1].size = 42
      done(nil, {
        name = "test",
        endpoint = "localhost:8529",
        collection_count = 1,
        total_documents = 1,
        total_size = 42,
        collections = collections,
      })
    end)
    return { cancel = function() end }
  end,
  database_overview = function()
    return {
      name = "test",
      collection_count = 1,
      collections = {
        { name = "items", type = "document", status = "loaded", count = 0 },
      },
    }
  end,
  browse_collection = function()
    return {
      database = "test",
      collection = "items",
      field = "_key",
      search = "",
      offset = 0,
      limit = 50,
      total_count = 0,
      has_more = false,
      items = {},
    }
  end,
  browse_collection_async = function(_, _, _, search, limit, cursor_id, done)
    vim.schedule(function()
      done(nil, {
        database = "test",
        collection = "items",
        field = "_key",
        search = search,
        limit = limit,
        total_count = cursor_id and nil or 1,
        has_more = false,
        items = {
          {
            key = "one",
            id = "items/one",
            field = "_key",
            field_value = "one",
            field_value_text = "one",
            preview = '{"_id":"items/one","_key":"one"}',
          },
        },
      })
    end)
    return { cancel = function() end }
  end,
  get_document_async = function(_, id, done)
    local collection, key = id:match("^([^/]+)/(.+)$")
    vim.schedule(function()
      done(nil, {
        database = "test",
        id = id,
        key = key,
        collection = collection,
        document = { _id = id, _key = key },
        preview = vim.json.encode({ _id = id, _key = key }),
        show = true,
      })
    end)
    return { cancel = function() end }
  end,
  list_indexes_async = function(_, collection, done)
    vim.schedule(function()
      done(nil, {
        { id = collection .. "/0", name = "primary", type = "primary", fields = { "_key" } },
      })
    end)
    return { cancel = function() end }
  end,
  close_cursor_async = function() end,
}

require("snacks").setup({ picker = { enabled = true } })
require("arangodb").setup({
  connections = { test = "http://localhost:8529/test" },
  default_database = "test",
})

require("arangodb.browser").open({
  database = database.database,
  pick_database = false,
})

assert(
  vim.wait(1000, function()
    local pickers = require("snacks.picker.core.picker").get()
    return #pickers == 1 and pickers[1].finder:count() == 1
  end),
  "Snacks collection picker did not load asynchronously"
)
local collection_picker = require("snacks.picker.core.picker").get()[1]
assert_navigation_modes(collection_picker, { "arango_action_menu" })
assert_actions_unmapped(collection_picker, {
  "arango_create_document",
  "arango_create_collection",
  "arango_duplicate_collection",
  "arango_rename_collection",
  "arango_truncate_collection",
})
assert(
  vim.wait(1000, function()
    for _, buf in ipairs(vim.api.nvim_list_bufs()) do
      if vim.api.nvim_buf_is_valid(buf) then
        local ok, lines = pcall(vim.api.nvim_buf_get_lines, buf, 0, -1, false)
        if ok then
          local text = table.concat(lines, "\n")
          if
            text:find("Database", 1, true)
            and text:match("Documents:%s+1")
            and text:match("Approx%. size:%s+42 B")
            and not text:match("Documents:%s+unavailable")
            and not text:match("Approx%. size:%s+unavailable")
          then
            return true
          end
        end
      end
    end
    return false
  end),
  "Snacks collection preview did not render database totals"
)

collection_picker.opts.actions.arango_action_menu(collection_picker, { item = { name = "items" } })
assert(
  vim.wait(1000, function()
    local pickers = require("snacks.picker.core.picker").get()
    return #pickers == 2 and pickers[2].title == "Collection actions (test)"
  end),
  "Collection action menu did not open"
)
local collection_action_picker = require("snacks.picker.core.picker").get()[2]
local manage_indexes_item
for _, item in ipairs(collection_action_picker.finder.items) do
  if item.item and item.item.action == "arango_manage_indexes" then
    manage_indexes_item = item
    break
  end
end
assert(manage_indexes_item, "Collection action menu did not contain index management")
collection_action_picker.opts.actions.confirm(collection_action_picker, manage_indexes_item)
assert(
  vim.wait(1000, function()
    local pickers = require("snacks.picker.core.picker").get()
    return #pickers == 1 and pickers[1].title == "Indexes (test/items)"
  end),
  "Index picker did not replace the collection action menu"
)
local collection_index_picker = require("snacks.picker.core.picker").get()[1]
vim.api.nvim_feedkeys(vim.keycode("<C-x>"), "mx", false)
assert(
  vim.wait(1000, function()
    local pickers = require("snacks.picker.core.picker").get()
    return #pickers == 1 and pickers[1] ~= collection_index_picker and pickers[1].title == "Index actions (test/items)"
  end),
  "Index action menu did not open from the collection picker"
)
local collection_index_action_picker = require("snacks.picker.core.picker").get()[1]
collection_index_action_picker:close()
assert(
  vim.wait(1000, function()
    local pickers = require("snacks.picker.core.picker").get()
    return #pickers == 1 and pickers[1] ~= collection_index_action_picker and pickers[1].title == "Indexes (test/items)"
  end),
  "Closing the collection index action menu did not restore the index picker"
)
collection_index_picker = require("snacks.picker.core.picker").get()[1]
collection_index_picker:close()
assert(
  vim.wait(1000, function()
    local pickers = require("snacks.picker.core.picker").get()
    return #pickers == 1 and pickers[1] ~= collection_index_picker and pickers[1].title:match("Arango test") ~= nil
  end),
  "Closing the collection index picker did not restore the collection picker"
)

require("snacks.picker.core.picker").get()[1]:close()
require("arangodb.browser").open({
  kind = "collection",
  config = database,
  collection = "items",
  field = "_key",
})

assert(
  vim.wait(1000, function()
    local pickers = require("snacks.picker.core.picker").get()
    return #pickers == 1 and pickers[1].finder:count() == 1
  end),
  "Snacks document picker did not load a cursor page asynchronously"
)
local document_picker = require("snacks.picker.core.picker").get()[1]
assert_navigation_modes(document_picker, {
  "arango_action_menu",
  "arango_prev_page",
  "arango_next_page",
  "arango_change_field",
  "arango_reset_search",
  "arango_open_related",
  "arango_go_back",
})
assert_actions_unmapped(document_picker, {
  "arango_create_document",
  "arango_duplicate_document",
  "arango_delete_document",
  "arango_truncate_collection",
  "arango_manage_indexes",
})

document_picker.opts.actions.arango_manage_indexes(document_picker)
assert(
  vim.wait(1000, function()
    local pickers = require("snacks.picker.core.picker").get()
    return #pickers == 1 and pickers[1] ~= document_picker and pickers[1].title == "Indexes (test/items)"
  end),
  "Index picker did not replace the document picker"
)
local index_picker = require("snacks.picker.core.picker").get()[1]
index_picker.opts.actions.arango_action_menu(index_picker)
assert(
  vim.wait(1000, function()
    local pickers = require("snacks.picker.core.picker").get()
    return #pickers == 1 and pickers[1] ~= index_picker and pickers[1].title == "Index actions (test/items)"
  end),
  "Index action menu did not replace the index picker"
)
local index_action_picker = require("snacks.picker.core.picker").get()[1]
index_action_picker:close()
assert(
  vim.wait(1000, function()
    local pickers = require("snacks.picker.core.picker").get()
    return #pickers == 1 and pickers[1] ~= index_action_picker and pickers[1].title == "Indexes (test/items)"
  end),
  "Closing the index action menu did not restore the index picker"
)
index_picker = require("snacks.picker.core.picker").get()[1]
index_picker:close()
assert(
  vim.wait(1000, function()
    local pickers = require("snacks.picker.core.picker").get()
    return #pickers == 1 and pickers[1] ~= index_picker and pickers[1].title:match("Arango test/items") ~= nil
  end),
  "Closing the index picker did not restore the document picker"
)
document_picker = require("snacks.picker.core.picker").get()[1]

document_picker.opts.actions.arango_open_document(document_picker, { item = { id = "items/one" } })
assert(
  vim.wait(1000, function()
    return #require("snacks.picker.core.picker").get() == 0
      and vim.api.nvim_buf_get_name(0) == "arangodb-buffer://test/items/one"
  end),
  "Real Snacks document handoff did not remove the picker before activating the buffer"
)
local document_buf = vim.api.nvim_get_current_buf()
assert(not document_picker.layout or not document_picker.layout.root or not document_picker.layout.root.win)
document_picker = nil
vim.api.nvim_buf_delete(document_buf, { force = true })

local active_document_picker = require("snacks.picker.core.picker").get()[1]
if active_document_picker then
  active_document_picker:close()
end
require("arangodb").setup({
  connections = { test = "http://localhost:8529/test" },
  default_database = "test",
  picker_keymaps = {
    create = "<C-a>",
    delete = "<C-d>",
  },
})
require("arangodb.browser").open({
  kind = "collection",
  config = database,
  collection = "items",
  field = "_key",
})
assert(
  vim.wait(1000, function()
    local pickers = require("snacks.picker.core.picker").get()
    return #pickers == 1 and pickers[1].finder:count() == 1
  end),
  "Snacks document picker with configured write mappings did not load"
)
local configured_picker = require("snacks.picker.core.picker").get()[1]
assert_normal_only(configured_picker, "arango_create_document")
assert_normal_only(configured_picker, "arango_delete_document")
configured_picker:close()

package.loaded["arangodb.aql_history"] = {
  load = function()
    return {
      {
        timestamp = "2026-01-01T00:00:00Z",
        connection = "test",
        database = "test",
        query = "RETURN 1",
        bind_vars = {},
      },
    }
  end,
  add = function() end,
}
package.loaded["arangodb.aql"] = nil
local session = require("arangodb.aql").open({
  config = database,
  connection = "test",
  query = "RETURN 1",
})
vim.api.nvim_buf_call(session.query_buf, function()
  vim.cmd("ArangoAqlHistory")
end)

assert(
  vim.wait(1000, function()
    local pickers = require("snacks.picker.core.picker").get()
    return #pickers == 1 and pickers[1].finder:count() == 1
  end),
  "Snacks AQL history picker did not load"
)
require("snacks.picker.core.picker").get()[1]:close()
vim.api.nvim_buf_delete(session.query_buf, { force = true })
