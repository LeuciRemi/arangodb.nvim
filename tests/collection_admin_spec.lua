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
    local select_index
    local index_picker_count = 0
    local confirms = { 2, 1 }
    local confirm_messages = {}
    local deletes = 0
    vim.ui.select = function(items, opts, done)
      h.eq(true, type(opts.snacks) == "table")
      if opts.prompt == "Indexes (test/items)" then
        index_picker_count = index_picker_count + 1
        select_index = function()
          done(items[2])
        end
      else
        done("Delete index")
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
        select_index()
        h.eq(0, deletes)
        h.eq(2, index_picker_count)
        select_index()
        h.eq(1, deletes)
        h.eq(3, index_picker_count)
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

  h.test("cancelling the index picker invokes its back callback", function()
    local original_select = vim.ui.select
    local backed = false
    vim.ui.select = function(_, opts, done)
      h.eq(nil, opts.snacks.win.input.keys["<C-b>"])
      h.eq(nil, opts.snacks.win.list.keys["<C-b>"])
      h.eq("arango_action_menu", opts.snacks.win.input.keys["<C-x>"][1])
      h.eq("arango_action_menu", opts.snacks.win.list.keys["<C-x>"][1])
      h.eq("function", type(opts.snacks.actions.arango_action_menu))
      done(nil)
    end

    local ok, err = xpcall(function()
      with_admin({
        list_indexes_async = function(_, _, done)
          done(nil, {})
        end,
      }, { open = function() end }, function(admin)
        admin.manage_indexes(config, "items", nil, function()
          backed = true
        end)
      end)
    end, debug.traceback)
    vim.ui.select = original_select
    if not ok then
      error(err, 0)
    end
    h.eq(true, backed)
  end),

  h.test("the index action mapping opens its action menu after closing the index picker", function()
    local original_select = vim.ui.select
    local action_items
    local action_menu
    local index_picker_count = 0
    local index_done
    local backed = 0
    local picker = {
      closed = false,
      opts = {
        on_close = function()
          index_done(nil)
        end,
      },
      current = function()
        return { item = { id = "items/by_email", name = "by_email", type = "persistent" } }
      end,
      close = function(self)
        self.closed = true
        if self.opts.on_close then
          self.opts.on_close()
        end
      end,
    }
    vim.ui.select = function(items, opts, done)
      if opts.prompt == "Indexes (test/items)" then
        index_picker_count = index_picker_count + 1
        action_menu = opts.snacks.actions.arango_action_menu
        index_done = done
      elseif opts.prompt == "Index actions (test/items)" then
        action_items = items
        done(nil)
      end
    end

    local ok, err = xpcall(function()
      with_admin({
        list_indexes_async = function(_, _, done)
          done(nil, { { id = "items/by_email", name = "by_email", type = "persistent" } })
        end,
      }, { open = function() end }, function(admin)
        admin.manage_indexes(config, "items", nil, function()
          backed = backed + 1
        end)
      end)
      action_menu(picker)
      assert(
        vim.wait(1000, function()
          return action_items ~= nil and index_picker_count == 2
        end),
        "Index action menu did not open"
      )
    end, debug.traceback)
    vim.ui.select = original_select
    if not ok then
      error(err, 0)
    end
    h.eq(true, picker.closed)
    h.eq(nil, picker.opts.on_close)
    h.eq(0, backed)
    h.eq("Inspect JSON", action_items[1].label)
    h.eq("Delete index", action_items[2].label)
    h.eq("Create index", action_items[3].label)
  end),
}
