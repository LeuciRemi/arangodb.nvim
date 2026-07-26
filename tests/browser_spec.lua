local h = require("tests.helpers")

local config = {
  scheme = "http",
  host = "localhost",
  port = 8529,
  database = "test",
}

local function with_browser(client, callback)
  local original_client = package.loaded["arangodb.client"]
  local original_browser = package.loaded["arangodb.browser"]
  package.loaded["arangodb.client"] = client
  package.loaded["arangodb.browser"] = nil

  local ok, err = xpcall(function()
    callback(require("arangodb.browser"))
  end, debug.traceback)

  package.loaded["arangodb.browser"] = original_browser
  package.loaded["arangodb.client"] = original_client
  if not ok then
    error(err, 0)
  end
end

local function document()
  return {
    database = "test",
    id = "items/a",
    key = "a",
    collection = "items",
    document = { _id = "items/a", _key = "a", _rev = "1", name = "local" },
    preview = '{"_id":"items/a","_key":"a","_rev":"1","name":"local"}',
    show = false,
  }
end

return {
  h.test("document buffers save against their canonical identity", function()
    local saved_id
    with_browser({
      save_document_async = function(_, id, payload, opts, done)
        if type(opts) == "function" then
          done = opts
        end
        saved_id = id
        done(nil, {
          database = "test",
          id = id,
          key = "a",
          collection = "items",
          document = payload,
          preview = require("arangodb.utils").json_pretty(payload),
        })
        return { cancel = function() end }
      end,
    }, function(browser)
      local doc = document()
      browser.open_document(config, doc)
      local buf = vim.fn.bufnr("arangodb-buffer://test/items/a")
      vim.api.nvim_buf_call(buf, function()
        vim.cmd("ArangoDocumentSave")
      end)
      h.eq("items/a", saved_id)
      vim.api.nvim_buf_delete(buf, { force = true })
    end)
  end),

  h.test("document deletion is confirmed with its database and can be cancelled", function()
    local original_confirm = vim.fn.confirm
    local confirmations = 0
    local deleted = 0
    vim.fn.confirm = function(message)
      confirmations = confirmations + 1
      h.matches("test/items/a", message)
      return confirmations == 1 and 2 or 1
    end
    local ok, err = xpcall(function()
      with_browser({
        delete_document_async = function(_, id, done)
          deleted = deleted + 1
          vim.schedule(function()
            done(nil, { _id = id })
          end)
          return { cancel = function() end }
        end,
      }, function(browser)
        browser.open_document(config, document())
        local buf = vim.fn.bufnr("arangodb-buffer://test/items/a")
        vim.api.nvim_buf_call(buf, function()
          vim.cmd("ArangoDocumentDelete")
        end)
        h.eq(0, deleted)
        vim.api.nvim_buf_call(buf, function()
          vim.cmd("ArangoDocumentDelete")
        end)
        h.eq(1, deleted)
        assert(vim.wait(1000, function()
          return not vim.api.nvim_buf_is_valid(buf)
        end))
      end)
    end, debug.traceback)
    vim.fn.confirm = original_confirm
    if not ok then
      error(err, 0)
    end
  end),

  h.test("modified document buffers block destructive actions before confirmation", function()
    local original_confirm = vim.fn.confirm
    local confirmations = 0
    local deleted = 0
    vim.fn.confirm = function()
      confirmations = confirmations + 1
      return 1
    end
    local ok, err = xpcall(function()
      with_browser({
        delete_document_async = function()
          deleted = deleted + 1
        end,
      }, function(browser)
        browser.open_document(config, document())
        local buf = vim.fn.bufnr("arangodb-buffer://test/items/a")
        vim.bo[buf].modified = true
        vim.api.nvim_buf_call(buf, function()
          vim.cmd("ArangoDocumentDelete")
        end)
        h.eq(0, confirmations)
        h.eq(0, deleted)
        h.eq(true, vim.api.nvim_buf_is_valid(buf))
        vim.api.nvim_buf_delete(buf, { force = true })
      end)
    end, debug.traceback)
    vim.fn.confirm = original_confirm
    if not ok then
      error(err, 0)
    end
  end),

  h.test("collection truncation uses a reinforced confirmation and restores picker focus", function()
    local original_confirm = vim.fn.confirm
    local original_input = vim.ui.input
    local original_snacks = package.loaded.snacks
    local confirmations = 0
    local inputs = 0
    local truncated = 0
    local focused = false
    local picker_opts
    local picker = {
      closed = false,
      opts = {},
      input = { filter = { search = "" }, win = { win = vim.api.nvim_get_current_win() } },
      find = function() end,
      update_titles = function() end,
      focus = function()
        focused = true
      end,
    }
    package.loaded.snacks = {
      picker = function(opts)
        picker_opts = opts
        picker.opts = opts
        return picker
      end,
    }
    vim.fn.confirm = function(message)
      confirmations = confirmations + 1
      h.matches("test/items", message)
      h.matches("cannot be undone", message)
      return confirmations == 1 and 2 or 1
    end
    vim.ui.input = function()
      inputs = inputs + 1
    end
    local ok, err = xpcall(function()
      with_browser({
        truncate_collection_async = function(_, collection, done)
          truncated = truncated + 1
          done(nil, { name = collection })
        end,
      }, function(browser)
        browser.open({ kind = "collections", config = config })
        browser.open_document(config, document())
        local document_buf = vim.fn.bufnr("arangodb-buffer://test/items/a")
        vim.bo[document_buf].modified = true
        picker_opts.actions.arango_rename_collection(picker, { item = { name = "items" } })
        picker_opts.actions.arango_truncate_collection(picker, { item = { name = "items" } })
        h.eq(0, inputs)
        h.eq(0, confirmations)
        h.eq(0, truncated)
        vim.api.nvim_buf_delete(document_buf, { force = true })

        picker_opts.actions.arango_truncate_collection(picker, { item = { name = "items" } })
        h.eq(0, truncated)
        assert(vim.wait(1000, function()
          return focused
        end))
        focused = false
        picker_opts.actions.arango_truncate_collection(picker, { item = { name = "items" } })
        h.eq(1, truncated)
        assert(vim.wait(1000, function()
          return focused
        end))
      end)
    end, debug.traceback)
    vim.fn.confirm = original_confirm
    vim.ui.input = original_input
    package.loaded.snacks = original_snacks
    if not ok then
      error(err, 0)
    end
  end),

  h.test("revision conflicts can be explicitly force-overwritten", function()
    local calls = {}
    local original_select = vim.ui.select
    vim.ui.select = function(_, _, done)
      done("Force overwrite")
    end

    local ok, err = xpcall(function()
      with_browser({
        save_document_async = function(_, id, payload, opts, done)
          if type(opts) == "function" then
            done = opts
            opts = nil
          end
          calls[#calls + 1] = { id = id, opts = opts }
          if #calls == 1 then
            done(require("arangodb.errors").new({
              kind = "conflict",
              message = "revision conflict",
              status = 412,
            }))
            return { cancel = function() end }
          end
          done(nil, {
            database = "test",
            id = id,
            key = "a",
            collection = "items",
            document = payload,
            preview = require("arangodb.utils").json_pretty(payload),
          })
          return { cancel = function() end }
        end,
      }, function(browser)
        browser.open_document(config, document())
        local buf = vim.fn.bufnr("arangodb-buffer://test/items/a")
        vim.api.nvim_buf_call(buf, function()
          vim.cmd("ArangoDocumentSave")
        end)
        h.eq(2, #calls)
        h.eq(nil, calls[1].opts)
        h.eq(true, calls[2].opts.force)
        vim.api.nvim_buf_delete(buf, { force = true })
      end)
    end, debug.traceback)
    vim.ui.select = original_select
    if not ok then
      error(err, 0)
    end
  end),
}
