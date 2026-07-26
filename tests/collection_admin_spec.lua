local h = require("tests.helpers")

local config = { database = "test" }

local function with_admin(client, editor, callback)
  local original_client = package.loaded["arangodb.client"]
  local original_editor = package.loaded["arangodb.browser.json_editor"]
  local original_admin = package.loaded["arangodb.browser.collection_admin"]
  package.loaded["arangodb.client"] = client
  package.loaded["arangodb.browser.json_editor"] = editor
  package.loaded["arangodb.browser.collection_admin"] = nil
  local ok, err = xpcall(function()
    callback(require("arangodb.browser.collection_admin"))
  end, debug.traceback)
  package.loaded["arangodb.browser.collection_admin"] = original_admin
  package.loaded["arangodb.browser.json_editor"] = original_editor
  package.loaded["arangodb.client"] = original_client
  if not ok then
    error(err, 0)
  end
end

return {
  h.test("index deletion requires a database-qualified confirmation", function()
    local original_select = vim.ui.select
    local original_confirm = vim.fn.confirm
    local select_call = 0
    local confirms = { 2, 1 }
    local confirm_messages = {}
    local deletes = 0
    vim.ui.select = function(items, opts, done)
      select_call = select_call + 1
      h.eq(true, type(opts.snacks) == "table")
      if select_call == 1 or select_call == 3 then
        done(items[2])
      elseif select_call == 2 or select_call == 4 then
        done("Delete index")
      else
        done(nil)
      end
    end
    vim.fn.confirm = function(message)
      confirm_messages[#confirm_messages + 1] = message
      return table.remove(confirms, 1)
    end

    local ok, err = xpcall(function()
      with_admin({
        list_indexes_async = function(_, _, done)
          done(nil, { { id = "items/by_email", name = "by_email", type = "persistent", fields = { "email" } } })
        end,
        delete_index_async = function(_, id, done)
          deletes = deletes + 1
          done(nil, { id = id })
        end,
      }, { open = function() end }, function(admin)
        admin.manage_indexes(config, "items")
        h.eq(0, deletes)
        admin.manage_indexes(config, "items")
        h.eq(1, deletes)
      end)
    end, debug.traceback)
    vim.ui.select = original_select
    vim.fn.confirm = original_confirm
    if not ok then
      error(err, 0)
    end
    h.eq(2, #confirm_messages)
    h.matches("test/items", confirm_messages[1])
    h.matches("by_email", confirm_messages[1])
  end),

  h.test("collection property and index editors dispatch validated writes", function()
    local original_select = vim.ui.select
    local opened = {}
    local created
    local updated
    vim.ui.select = function(items, opts, done)
      h.eq(true, type(opts.snacks) == "table")
      done(items[1])
    end
    local ok, err = xpcall(function()
      with_admin({
        list_indexes_async = function(_, _, done)
          done(nil, {})
        end,
        create_index_async = function(_, collection, value, done)
          created = { collection = collection, value = value }
          done(nil, value)
        end,
        collection_properties_async = function(_, _, done)
          done(nil, { waitForSync = true, ignored = true })
        end,
        update_collection_properties_async = function(_, collection, value, done)
          updated = { collection = collection, value = value }
          done(nil, value)
        end,
      }, {
        open = function(opts)
          opened[#opened + 1] = opts
        end,
      }, function(admin)
        admin.manage_indexes(config, "items")
        opened[1].on_save({ type = "persistent", fields = { "email" } }, function() end)
        admin.edit_properties(config, "items")
        opened[2].on_save({ waitForSync = false, ignored = true }, function() end)
      end)
    end, debug.traceback)
    vim.ui.select = original_select
    if not ok then
      error(err, 0)
    end
    h.eq("items", created.collection)
    h.eq("email", created.value.fields[1])
    h.eq("items", updated.collection)
    h.eq(false, updated.value.waitForSync)
    h.eq(nil, updated.value.ignored)
  end),
}
