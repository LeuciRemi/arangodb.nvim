-- Run from snacks_smoke.lua with real Snacks selectors and JSON editors.
local admin = require("arangodb.browser.collection_admin")
local client = require("arangodb.client")
local config = { scheme = "http", host = "localhost", port = 8529, database = "test" }
local indexes = {
  { id = "items/0", name = "primary", type = "primary", fields = { "_key" } },
  { id = "items/1", name = "edge", type = "edge", fields = { "_from", "_to" } },
  { id = "items/by_email", name = "by_email", type = "persistent", fields = { "email" } },
}
local back_count, change_count, delete_count, confirm_count = 0, 0, 0, 0
local original_list = client.list_indexes_async
local original_delete = client.delete_index_async
local original_confirm = vim.fn.confirm
client.list_indexes_async = function(_, _, done)
  vim.schedule(function()
    done(nil, indexes)
  end)
end
client.delete_index_async = function()
  delete_count = delete_count + 1
end
vim.fn.confirm = function()
  confirm_count = confirm_count + 1
  return 2
end

local function pickers()
  return require("snacks.picker.core.picker").get()
end

local function wait_for(check, message)
  assert(vim.wait(1000, check), message)
end

local function index_picker()
  wait_for(function()
    local active = pickers()
    return #active == 1 and active[1].title == "Indexes (test/items)" and #active[1]:items() == 4
  end, "Index picker was not restored")
  return pickers()[1]
end

local function open_indexes()
  admin.manage_indexes(config, "items", function()
    change_count = change_count + 1
  end, function()
    back_count = back_count + 1
  end)
  return index_picker()
end

local function confirm(picker, predicate)
  for _, item in ipairs(picker:items()) do
    if predicate(item.item) then
      picker.opts.actions.confirm(picker, item)
      return
    end
  end
  error("Missing selector item")
end

local function inspect_buffer()
  wait_for(function()
    return #pickers() == 0 and vim.b.arangodb_editor_title == "ArangoDB Index"
  end, "Inspect JSON did not open the editor directly")
  assert(vim.bo.readonly and not vim.bo.modifiable, "Index inspector must be read-only")
  local value = vim.json.decode(table.concat(vim.api.nvim_buf_get_lines(0, 0, -1, false), "\n"))
  assert(value.id == "items/by_email", "Inspector opened the wrong index")
  vim.api.nvim_buf_delete(0, { force = true })
end

local function action_menu(picker, index_type)
  for row, item in ipairs(picker:items()) do
    if item.item.type == index_type then
      picker.list:move(row, true)
      break
    end
  end
  assert(picker:current().item.type == index_type, "Wrong index selected")
  picker.opts.actions.arango_action_menu(picker)
  wait_for(function()
    local active = pickers()
    return picker.closed
      and #active == 1
      and active[1].title == "Index actions (test/items)"
      and #active[1]:items() == 3
  end, "Index action menu did not replace the picker")
  return pickers()[1]
end

-- Normal confirmation closes the picker even with auto_close = false.
local picker = open_indexes()
confirm(picker, function(item)
  return item.create
end)
wait_for(function()
  return picker.closed and #pickers() == 0 and vim.b.arangodb_editor_title == "ArangoDB Create Index"
end, "Normal create confirmation retained the index picker")
vim.api.nvim_buf_delete(0, { force = true })
picker = open_indexes()
confirm(picker, function(item)
  return item.type == "persistent"
end)
wait_for(function()
  return picker.closed and #pickers() == 1 and pickers()[1] ~= picker and #pickers()[1]:items() == 2
end, "Normal index confirmation retained the index picker")
confirm(pickers()[1], function(item)
  return item == "Inspect JSON"
end)
inspect_buffer()
assert(back_count == 0, "Normal confirmation invoked the back callback")

local menu = action_menu(open_indexes(), "persistent")
confirm(menu, function(item)
  return item.action == "inspect"
end)
inspect_buffer()

-- Cancellation and both protected index types restore navigation without writes.
for _, index_type in ipairs({ "persistent", "primary", "edge" }) do
  local backs_before = back_count
  menu = action_menu(open_indexes(), index_type)
  confirm(menu, function(item)
    return item.action == "delete"
  end)
  picker = index_picker()
  assert(back_count == backs_before, "Delete cancellation navigated out of index management")
  assert(delete_count == 0 and change_count == 0, "Rejected deletion dispatched a mutation")
  picker:close()
  wait_for(function()
    return #pickers() == 0 and back_count == backs_before + 1
  end, "Restored index picker lost its back callback")
end
assert(confirm_count == 1, "System indexes must be rejected before confirmation")

client.list_indexes_async = original_list
client.delete_index_async = original_delete
vim.fn.confirm = original_confirm
print("Snacks index navigation regressions passed")
