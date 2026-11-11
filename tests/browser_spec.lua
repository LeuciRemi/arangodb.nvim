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

local function new_picker_harness()
  local active = {}
  local created = {}

  local function remove_active(picker)
    for index, candidate in ipairs(active) do
      if candidate == picker then
        table.remove(active, index)
        return
      end
    end
  end

  local function pick(opts)
    opts = opts or {}
    local root_win = vim.api.nvim_get_current_win()
    local picker = {
      closed = false,
      layout_open = true,
      layout_closed = false,
      created_with_stale_layout = false,
      find_calls = 0,
      focus_calls = 0,
      opts = opts,
      input = {
        filter = { search = "" },
        win = { win = root_win },
        set = function(self, _, value)
          self.filter.search = value or ""
        end,
      },
      list = {
        win = {
          win = root_win,
          execute = function(self, action)
            self.last_action = action
          end,
        },
      },
      preview = { win = { win = root_win } },
      layout = {
        root = {
          win = root_win,
          opts = {},
          drop = function() end,
        },
      },
    }

    for _, previous in ipairs(created) do
      if previous.layout_open then
        picker.created_with_stale_layout = true
        break
      end
    end

    function picker:find()
      self.find_calls = self.find_calls + 1
    end

    function picker:update_titles() end

    function picker:focus()
      self.focus_calls = self.focus_calls + 1
    end

    function picker:current()
      return self.current_item
    end

    function picker:close()
      if self.closed then
        return
      end
      if self.opts.on_close then
        self.opts.on_close(self)
      end
      self.closed = true
      remove_active(self)
      vim.schedule(function()
        self.layout_open = false
        self.layout_closed = true
        self.input.win.win = nil
        self.list.win.win = nil
        self.preview.win.win = nil
        self.layout.root.win = nil
      end)
    end

    active[#active + 1] = picker
    created[#created + 1] = picker
    if opts.on_show then
      opts.on_show(picker)
    end
    return picker
  end

  return {
    snacks = { picker = pick },
    active_count = function()
      return #active
    end,
    last = function()
      return created[#created]
    end,
  }
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

local function with_picker_harness(callback)
  local original_snacks = package.loaded.snacks
  local harness = new_picker_harness()
  package.loaded.snacks = harness.snacks

  local ok, err = xpcall(function()
    callback(harness)
  end, debug.traceback)

  package.loaded.snacks = original_snacks
  if not ok then
    error(err, 0)
  end
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

  h.test("document handoff closes the picker before activating the buffer", function()
    local document_buf
    local ok, err = xpcall(function()
      with_picker_harness(function(harness)
        with_browser({
          get_document_async = function(_, id, done)
            vim.schedule(function()
              local payload = document()
              payload.id = id
              payload.show = true
              done(nil, payload)
            end)
            return { cancel = function() end }
          end,
        }, function(browser)
          browser.open({ kind = "collection", config = config, collection = "items", field = "_key" })
          local picker = harness.last()
          picker.opts.actions.arango_open_document(picker, { item = { id = "items/a" } })

          assert(
            vim.wait(1000, function()
              return vim.api.nvim_buf_get_name(0) == "arangodb-buffer://test/items/a"
            end),
            "Document buffer did not become active"
          )
          document_buf = vim.api.nvim_get_current_buf()
          h.eq(true, picker.closed)
          h.eq(true, picker.layout_closed)
          h.eq(false, picker.layout_open)
          h.eq(0, harness.active_count())
        end)
      end)
    end, debug.traceback)

    if document_buf and vim.api.nvim_buf_is_valid(document_buf) then
      vim.api.nvim_buf_delete(document_buf, { force = true })
    end
    if not ok then
      error(err, 0)
    end
  end),

  h.test("create document handoff closes the collection picker before opening a draft", function()
    local document_buf
    local ok, err = xpcall(function()
      with_picker_harness(function(harness)
        with_browser({}, function(browser)
          browser.open({ kind = "collections", config = config })
          local picker = harness.last()
          picker.opts.actions.arango_create_document(picker, { item = { name = "items" } })

          assert(
            vim.wait(1000, function()
              local buf = vim.api.nvim_get_current_buf()
              return vim.b[buf].arangodb_document_is_new == true and vim.b[buf].arangodb_document_collection == "items"
            end),
            "Draft document buffer did not become active"
          )
          document_buf = vim.api.nvim_get_current_buf()
          h.eq(true, picker.closed)
          h.eq(true, picker.layout_closed)
          h.eq(0, harness.active_count())
        end)
      end)
    end, debug.traceback)

    if document_buf and vim.api.nvim_buf_is_valid(document_buf) then
      vim.api.nvim_buf_delete(document_buf, { force = true })
    end
    if not ok then
      error(err, 0)
    end
  end),

  h.test("duplicate document handoff closes the picker before opening a draft", function()
    local document_buf
    local ok, err = xpcall(function()
      with_picker_harness(function(harness)
        with_browser({
          get_document_async = function(_, _, done)
            vim.schedule(function()
              done(nil, document())
            end)
            return { cancel = function() end }
          end,
        }, function(browser)
          browser.open({ kind = "collection", config = config, collection = "items", field = "_key" })
          local picker = harness.last()
          picker.opts.actions.arango_duplicate_document(picker, { item = { id = "items/a" } })

          assert(
            vim.wait(1000, function()
              local buf = vim.api.nvim_get_current_buf()
              return vim.b[buf].arangodb_document_is_new == true and vim.b[buf].arangodb_document_collection == "items"
            end),
            "Duplicated document buffer did not become active"
          )
          document_buf = vim.api.nvim_get_current_buf()
          h.eq(true, picker.closed)
          h.eq(true, picker.layout_closed)
          h.eq(0, harness.active_count())
        end)
      end)
    end, debug.traceback)

    if document_buf and vim.api.nvim_buf_is_valid(document_buf) then
      vim.api.nvim_buf_delete(document_buf, { force = true })
    end
    if not ok then
      error(err, 0)
    end
  end),

  h.test("graph handoff closes the document picker before opening the graph", function()
    local original_graph = package.loaded["arangodb.graph"]
    local graph_opts
    local graph_after_layout_close = false
    local ok, err = xpcall(function()
      with_picker_harness(function(harness)
        package.loaded["arangodb.graph"] = {
          open = function(opts)
            graph_opts = opts
            graph_after_layout_close = harness.last().layout_closed
          end,
        }
        with_browser({}, function(browser)
          browser.open({ kind = "collection", config = config, collection = "items", field = "_key" })
          local picker = harness.last()
          picker.opts.actions.arango_explore_graph(picker, { item = { id = "items/a" } })

          assert(
            vim.wait(1000, function()
              return graph_opts ~= nil
            end),
            "Graph workflow did not start"
          )
          h.eq("items/a", graph_opts.start)
          h.eq(true, graph_after_layout_close)
          h.eq(true, picker.closed)
          h.eq(0, harness.active_count())
        end)
      end)
    end, debug.traceback)
    package.loaded["arangodb.graph"] = original_graph
    if not ok then
      error(err, 0)
    end
  end),

  h.test("database handoff closes the old picker before creating the new one", function()
    local original_core = package.loaded["arangodb.core"]
    local original_select = vim.ui.select
    local database_item = { name = "test", url = "http://localhost:8529/test" }
    local select_calls = 0
    local ok, err = xpcall(function()
      package.loaded["arangodb.core"] = {
        available_databases = function()
          return { database_item }
        end,
        default_database = function()
          return database_item
        end,
        find_database = function()
          return database_item
        end,
        arango_url = function(name)
          return "http://localhost:8529/" .. name
        end,
        resolve_connection = function()
          return config
        end,
        notify_error = function() end,
      }
      vim.ui.select = function(items, _, done)
        select_calls = select_calls + 1
        done(items[1])
      end

      with_picker_harness(function(harness)
        with_browser({}, function(browser)
          browser.open({ kind = "collections", config = config, allow_database_back = true })
          local previous = harness.last()
          previous.opts.actions.arango_pick_database(previous)

          assert(
            vim.wait(1000, function()
              return harness.last() ~= previous
            end),
            "Database picker handoff did not create a new collection picker"
          )
          local current = harness.last()
          h.eq(true, previous.closed)
          h.eq(true, previous.layout_closed)
          h.eq(false, previous.layout_open)
          h.eq(false, current.created_with_stale_layout)
          h.eq(1, harness.active_count())
          h.eq(1, select_calls)
        end)
      end)
    end, debug.traceback)
    vim.ui.select = original_select
    package.loaded["arangodb.core"] = original_core
    if not ok then
      error(err, 0)
    end
  end),

  h.test("closing a picker cancels its pending document request", function()
    local original_core = package.loaded["arangodb.core"]
    local canceled = false
    local ok, err = xpcall(function()
      package.loaded["arangodb.core"] = {
        notify_error = function() end,
      }
      with_picker_harness(function(harness)
        with_browser({
          get_document_async = function()
            return {
              cancel = function()
                canceled = true
              end,
            }
          end,
        }, function(browser)
          browser.open({ kind = "collection", config = config, collection = "items", field = "_key" })
          local picker = harness.last()
          picker.opts.actions.arango_open_document(picker, { item = { id = "items/a" } })
          picker:close()
          h.eq(true, canceled)
          h.eq(0, harness.active_count())
          assert(
            vim.wait(1000, function()
              return picker.layout_closed
            end),
            "Canceled picker layout did not close"
          )
        end)
      end)
    end, debug.traceback)
    package.loaded["arangodb.core"] = original_core
    if not ok then
      error(err, 0)
    end
  end),

  h.test("collection editor transitions close the active picker", function()
    local original_admin = package.loaded["arangodb.browser.collection_admin"]
    local original_input = vim.ui.input
    local picker
    local inputs = 0
    local edit_started = false
    local edit_after_layout_close = false
    local rename_after_layout_close = false

    package.loaded["arangodb.browser.collection_admin"] = {
      edit_properties = function()
        edit_started = true
        edit_after_layout_close = picker.layout_closed
      end,
    }
    vim.ui.input = function()
      inputs = inputs + 1
      rename_after_layout_close = picker.layout_closed
    end

    local ok, err = xpcall(function()
      with_picker_harness(function(harness)
        with_browser({}, function(browser)
          browser.open({ kind = "collections", config = config })
          picker = harness.last()
          picker.opts.actions.arango_edit_collection_properties(picker, { item = { name = "items" } })
          assert(
            vim.wait(1000, function()
              return edit_started
            end),
            "Collection property editor did not start"
          )
          h.eq(true, edit_after_layout_close)
          h.eq(true, picker.closed)
          h.eq(true, picker.layout_closed)
          h.eq(0, harness.active_count())

          browser.open({ kind = "collections", config = config })
          picker = harness.last()
          picker.opts.actions.arango_rename_collection(picker, { item = { name = "items" } })
          assert(
            vim.wait(1000, function()
              return inputs == 1
            end),
            "Collection rename prompt did not start"
          )
          h.eq(true, rename_after_layout_close)
          h.eq(true, picker.closed)
          h.eq(true, picker.layout_closed)
          h.eq(0, harness.active_count())
        end)
      end)
    end, debug.traceback)

    vim.ui.input = original_input
    package.loaded["arangodb.browser.collection_admin"] = original_admin
    if not ok then
      error(err, 0)
    end
  end),

  h.test("index administration returns to the collection picker when cancelled", function()
    local original_admin = package.loaded["arangodb.browser.collection_admin"]
    local picker
    local managed = false
    local managed_after_layout_close = false

    package.loaded["arangodb.browser.collection_admin"] = {
      manage_indexes = function(_, _, _, on_back)
        managed = true
        managed_after_layout_close = picker.layout_closed
        on_back()
      end,
    }

    local ok, err = xpcall(function()
      with_picker_harness(function(harness)
        with_browser({}, function(browser)
          browser.open({ kind = "collections", config = config })
          picker = harness.last()
          picker.opts.actions.arango_manage_indexes(picker, { item = { name = "items" } })
          assert(
            vim.wait(1000, function()
              return managed and harness.active_count() == 1 and harness.last() ~= picker
            end),
            "Index management workflow did not start"
          )
          h.eq(true, managed_after_layout_close)
          h.eq(true, picker.closed)
          h.eq(true, picker.layout_closed)
          h.eq(1, harness.active_count())
        end)
      end)
    end, debug.traceback)

    package.loaded["arangodb.browser.collection_admin"] = original_admin
    if not ok then
      error(err, 0)
    end
  end),

  h.test("index administration is available from the document picker", function()
    local original_admin = package.loaded["arangodb.browser.collection_admin"]
    local picker
    local managed = false
    local managed_after_layout_close = false

    package.loaded["arangodb.browser.collection_admin"] = {
      manage_indexes = function(_, _, _, on_back)
        managed = true
        managed_after_layout_close = picker.layout_closed
        on_back()
      end,
    }

    local ok, err = xpcall(function()
      with_picker_harness(function(harness)
        with_browser({}, function(browser)
          browser.open({ kind = "collection", config = config, collection = "items", field = "_key" })
          picker = harness.last()
          picker.opts.actions.arango_manage_indexes(picker)
          assert(
            vim.wait(1000, function()
              return managed and harness.active_count() == 1 and harness.last() ~= picker
            end),
            "Index management workflow did not start from the document picker"
          )
          h.eq(true, managed_after_layout_close)
          h.eq(true, picker.closed)
          h.eq(true, picker.layout_closed)
          h.eq(1, harness.active_count())
        end)
      end)
    end, debug.traceback)

    package.loaded["arangodb.browser.collection_admin"] = original_admin
    if not ok then
      error(err, 0)
    end
  end),

  h.test("going back to a document closes the related picker before activating the buffer", function()
    local original_select = vim.ui.select
    local document_buf
    local ok, err = xpcall(function()
      vim.ui.select = function(items, _, done)
        done(items[1])
      end
      with_picker_harness(function(harness)
        with_browser({
          list_collections_async = function(_, done)
            done(nil, { "items" })
          end,
          browse_collection_async = function(_, _, _, _, _, _, _, done)
            done(nil, {
              total_count = 1,
              has_more = false,
              items = {
                {
                  key = "a",
                  id = "items/a",
                  field = "_key",
                  field_value = "a",
                  field_value_text = "a",
                  preview = '{"_id":"items/a","_key":"a"}',
                },
              },
            })
          end,
          get_document_async = function(_, _, done)
            vim.schedule(function()
              local payload = document()
              payload.show = true
              done(nil, payload)
            end)
            return { cancel = function() end }
          end,
        }, function(browser)
          local source = document()
          source.document.related = { _id = "items/b" }
          source.preview = nil
          browser.open_document(config, source)
          document_buf = vim.fn.bufnr("arangodb-buffer://test/items/a")
          vim.api.nvim_buf_call(document_buf, function()
            vim.cmd("ArangoDocumentRelated")
          end)

          local picker = harness.last()
          h.eq(1, harness.active_count())
          browser.back()

          assert(
            vim.wait(1000, function()
              return vim.api.nvim_buf_get_name(0) == "arangodb-buffer://test/items/a"
            end),
            "Back navigation did not activate the document buffer"
          )
          h.eq(true, picker.closed)
          h.eq(true, picker.layout_closed)
          h.eq(false, picker.layout_open)
          h.eq(0, harness.active_count())
        end)
      end)
    end, debug.traceback)

    for _, buf in ipairs(vim.api.nvim_list_bufs()) do
      if vim.api.nvim_buf_is_valid(buf) and vim.api.nvim_buf_get_name(buf) == "arangodb-buffer://test/items/a" then
        vim.api.nvim_buf_delete(buf, { force = true })
      end
    end
    vim.ui.select = original_select
    if not ok then
      error(err, 0)
    end
  end),

  h.test("resume reopens the last browser route without clearing history", function()
    local document_buf
    local ok, err = xpcall(function()
      with_picker_harness(function(harness)
        with_browser({}, function(browser)
          browser.open({ kind = "collections", config = config })
          local picker = harness.last()
          picker.opts.actions.arango_create_document(picker, { item = { name = "items" } })
          assert(
            vim.wait(1000, function()
              local buf = vim.api.nvim_get_current_buf()
              return vim.b[buf].arangodb_document_is_new == true
            end),
            "Draft document buffer did not become active"
          )
          document_buf = vim.api.nvim_get_current_buf()
          h.eq(true, picker.closed)
          h.eq(0, harness.active_count())

          browser.resume()
          local resumed = harness.last()
          assert(resumed ~= picker, "resume did not open a new picker")
          h.eq(false, resumed.closed)
          h.matches("^Arango test collections", resumed.opts.title)
          h.eq(1, harness.active_count())

          browser.back()
          assert(
            vim.wait(1000, function()
              return harness.last() ~= resumed
            end),
            "resume cleared the back history"
          )
        end)
      end)
    end, debug.traceback)

    if document_buf and vim.api.nvim_buf_is_valid(document_buf) then
      vim.api.nvim_buf_delete(document_buf, { force = true })
    end
    if not ok then
      error(err, 0)
    end
  end),

  h.test("renaming a collection reopens the collections picker", function()
    local original_input = vim.ui.input
    local pending_input
    vim.ui.input = function(_, done)
      pending_input = done
    end

    local ok, err = xpcall(function()
      with_picker_harness(function(harness)
        with_browser({
          rename_collection_async = function(_, _, new_name, done)
            vim.schedule(function()
              done(nil, { name = new_name })
            end)
            return { cancel = function() end }
          end,
        }, function(browser)
          browser.open({ kind = "collections", config = config })
          local picker = harness.last()
          picker.opts.actions.arango_rename_collection(picker, { item = { name = "items" } })
          assert(
            vim.wait(1000, function()
              return pending_input ~= nil and picker.closed
            end),
            "Collection rename prompt did not start"
          )

          pending_input("renamed")
          assert(
            vim.wait(1000, function()
              return harness.active_count() == 1 and harness.last() ~= picker
            end),
            "Renaming did not reopen the collections picker"
          )
          local reopened = harness.last()
          h.eq(false, reopened.closed)

          pending_input = nil
          reopened.opts.actions.arango_rename_collection(reopened, { item = { name = "items" } })
          assert(
            vim.wait(1000, function()
              return pending_input ~= nil and reopened.closed
            end),
            "Collection rename prompt did not start again"
          )

          pending_input(nil)
          assert(
            vim.wait(1000, function()
              return harness.active_count() == 1 and harness.last() ~= reopened
            end),
            "Cancelling the rename did not reopen the collections picker"
          )
        end)
      end)
    end, debug.traceback)

    vim.ui.input = original_input
    if not ok then
      error(err, 0)
    end
  end),

  h.test("the collections action menu offers document creation", function()
    local original_select = vim.ui.select
    local choices
    vim.ui.select = function(items, _, done)
      choices = items
      for _, choice in ipairs(items) do
        if choice.action == "arango_create_document" then
          done(choice)
          return
        end
      end
      done(nil)
    end

    local ok, err = xpcall(function()
      with_picker_harness(function(harness)
        with_browser({}, function(browser)
          browser.open({ kind = "collections", config = config })
          local picker = harness.last()

          picker.opts.actions.arango_action_menu(picker, { item = { name = "items" } })
          assert(
            vim.wait(1000, function()
              return choices ~= nil
            end),
            "Collection action menu did not open"
          )
          local create
          for _, choice in ipairs(choices) do
            if choice.action == "arango_create_document" then
              create = choice
            end
          end
          assert(create, "Create document choice is missing")
          h.matches("^Create document", create.label)
          h.eq("arango_create_document", picker.list.win.last_action)

          choices = nil
          picker.opts.actions.arango_action_menu(picker)
          assert(
            vim.wait(1000, function()
              return choices ~= nil
            end),
            "Collection action menu did not reopen"
          )
          for _, choice in ipairs(choices) do
            assert(choice.action ~= "arango_create_document", "Create document offered without a collection")
          end
        end)
      end)
    end, debug.traceback)

    vim.ui.select = original_select
    if not ok then
      error(err, 0)
    end
  end),

  h.test("a failed next page retries with a fresh cursor query", function()
    local cursors = {}
    local calls = 0
    local ok, err = xpcall(function()
      with_picker_harness(function(harness)
        with_browser({
          browse_collection_async = function(_, _, _, _, _, cursor_id, _, done)
            calls = calls + 1
            cursors[calls] = cursor_id
            if calls == 1 then
              done(nil, {
                total_count = 2,
                has_more = true,
                cursor_id = "cursor-1",
                items = {
                  {
                    key = "a",
                    id = "items/a",
                    field = "_key",
                    field_value = "a",
                    field_value_text = "a",
                    preview = '{"_id":"items/a","_key":"a"}',
                  },
                },
              })
            else
              done(require("arangodb.errors").new({ kind = "transport", message = "cursor is gone" }))
            end
            return { cancel = function() end }
          end,
          close_cursor_async = function() end,
        }, function(browser)
          browser.open({ kind = "collection", config = config, collection = "items", field = "_key" })
          local picker = harness.last()
          local ctx = {
            filter = { search = "" },
            async = {
              aborted = function()
                return false
              end,
              on = function() end,
              suspend = function() end,
              resume = function() end,
            },
          }
          local finder = picker.opts.finder(picker, ctx)

          finder(function() end)
          h.eq(nil, cursors[1])

          picker.opts.actions.arango_next_page(picker)
          finder(function() end)
          h.eq("cursor-1", cursors[2])

          picker.opts.actions.arango_next_page(picker)
          finder(function() end)
          h.eq(nil, cursors[3])
          h.eq(3, calls)
        end)
      end)
    end, debug.traceback)
    if not ok then
      error(err, 0)
    end
  end),

  h.test("going back keeps a document route that cannot be reopened", function()
    local original_notify = vim.notify
    local attempts = 0
    vim.notify = function() end
    local ok, err = xpcall(function()
      with_browser({
        get_document_async = function(_, _, done)
          attempts = attempts + 1
          done(require("arangodb.errors").new({ kind = "transport", message = "document unavailable" }))
          return { cancel = function() end }
        end,
      }, function(browser)
        browser.open_document(config, document())
        local source = vim.fn.bufnr("arangodb-buffer://test/items/a")
        assert(source ~= -1, "Document buffer was not opened")
        vim.api.nvim_buf_call(source, function()
          vim.cmd("ArangoDocumentDuplicate")
        end)
        h.eq(0, attempts)

        browser.back()
        h.eq(1, attempts)
        browser.back()
        h.eq(2, attempts)

        for _, buf in ipairs(vim.api.nvim_list_bufs()) do
          if vim.api.nvim_buf_is_valid(buf) and vim.b[buf].arangodb_document_is_new then
            vim.api.nvim_buf_delete(buf, { force = true })
          end
        end
        vim.api.nvim_buf_delete(source, { force = true })
      end)
    end, debug.traceback)
    vim.notify = original_notify
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

  h.test("dropping a collection is confirmed before any request and refreshes the list", function()
    local original_confirm = vim.fn.confirm
    local confirmations = {}
    local deleted = 0
    vim.fn.confirm = function(message, choices, default)
      confirmations[#confirmations + 1] = { message = message, choices = choices, default = default }
      return #confirmations == 1 and 2 or 1
    end

    local ok, err = xpcall(function()
      with_picker_harness(function(harness)
        with_browser({
          delete_collection_async = function(_, collection, done)
            deleted = deleted + 1
            vim.schedule(function()
              done(nil, { name = collection })
            end)
            return { cancel = function() end }
          end,
        }, function(browser)
          browser.open({ kind = "collections", config = config })
          local picker = harness.last()

          picker.opts.actions.arango_drop_collection(picker, { item = { name = "_users" } })
          h.eq(0, #confirmations)
          h.eq(0, deleted)

          browser.open_document(config, document())
          local buf = vim.fn.bufnr("arangodb-buffer://test/items/a")
          vim.bo[buf].modified = true
          picker.opts.actions.arango_drop_collection(picker, { item = { name = "items" } })
          h.eq(0, #confirmations)
          h.eq(0, deleted)
          vim.api.nvim_buf_delete(buf, { force = true })

          picker.opts.actions.arango_drop_collection(picker, { item = { name = "items" } })
          h.eq(1, #confirmations)
          h.matches("test/items", confirmations[1].message)
          h.matches("cannot be undone", confirmations[1].message)
          h.matches("&Cancel", confirmations[1].choices)
          h.eq(2, confirmations[1].default)
          h.eq(0, deleted)

          local finds = picker.find_calls
          picker.opts.actions.arango_drop_collection(picker, { item = { name = "items" } })
          assert(vim.wait(1000, function()
            return deleted == 1
          end))
          assert(
            vim.wait(1000, function()
              return picker.find_calls > finds
            end),
            "Dropping a collection did not refresh the collections list"
          )
        end)
      end)
    end, debug.traceback)

    vim.fn.confirm = original_confirm
    if not ok then
      error(err, 0)
    end
  end),

  h.test("sorting the document view restarts paging in the chosen order", function()
    local original_input = vim.ui.input
    local original_select = vim.ui.select
    local requests = {}
    local page = 0
    vim.ui.input = function(opts, done)
      h.eq("_key", opts.default)
      done("created")
    end
    vim.ui.select = function(items, _, done)
      h.eq("Descending", items[2])
      done("Descending")
    end

    local ok, err = xpcall(function()
      with_picker_harness(function(harness)
        with_browser({
          browse_collection_async = function(_, _, _, _, _, cursor_id, sort, done)
            requests[#requests + 1] = { cursor_id = cursor_id, sort = sort }
            page = page + 1
            done(nil, {
              total_count = 3,
              has_more = page < 3,
              cursor_id = "cursor-" .. page,
              items = {
                {
                  key = "a",
                  id = "items/a",
                  field = "_key",
                  field_value = "a",
                  field_value_text = "a",
                  preview = '{"_id":"items/a","_key":"a"}',
                },
              },
            })
            return { cancel = function() end }
          end,
          close_cursor_async = function() end,
        }, function(browser)
          browser.open({ kind = "collection", config = config, collection = "items", field = "_key" })
          local picker = harness.last()
          local function ctx()
            return {
              filter = { search = "" },
              async = {
                aborted = function()
                  return false
                end,
                on = function() end,
                suspend = function() end,
                resume = function() end,
              },
            }
          end

          picker.opts.finder(picker, ctx())(function() end)
          h.eq(nil, requests[1].sort)
          picker.opts.actions.arango_next_page(picker)
          picker.opts.finder(picker, ctx())(function() end)
          h.eq("cursor-1", requests[2].cursor_id)

          picker.opts.actions.arango_sort(picker)
          h.eq(2, #requests)
          picker.opts.finder(picker, ctx())(function() end)
          h.eq(3, #requests)
          h.eq(nil, requests[3].cursor_id)
          h.eq("created", requests[3].sort.field)
          h.eq("DESC", requests[3].sort.direction)
          h.matches("sort: created DESC", picker.title)
        end)
      end)
    end, debug.traceback)

    vim.ui.input = original_input
    vim.ui.select = original_select
    if not ok then
      error(err, 0)
    end
  end),

  h.test("Open in AQL hands the filtered and sorted view to the editor", function()
    local original_aql = package.loaded["arangodb.aql"]
    local real_client = require("arangodb.client")
    local opened
    package.loaded["arangodb.aql"] = {
      open = function(opts)
        opened = opts
      end,
    }

    local ok, err = xpcall(function()
      with_picker_harness(function(harness)
        with_browser({
          browse_query = real_client.browse_query,
        }, function(browser)
          browser.open({
            kind = "collection",
            config = config,
            collection = "items",
            field = "name",
            search = "alp",
            sort = { field = "created", direction = "DESC" },
          })
          local picker = harness.last()
          picker.opts.actions.arango_open_aql(picker)
          assert(
            vim.wait(1000, function()
              return opened ~= nil
            end),
            "Open in AQL did not open the AQL editor"
          )
          h.matches("FOR doc IN @@collection", opened.query)
          h.matches("FILTER CONTAINS", opened.query)
          h.matches('SORT doc%["created"%] DESC', opened.query)
          h.eq("items", opened.bind_vars["@collection"])
          h.eq("alp", opened.bind_vars.search)
          h.eq(require("arangodb.config").get().page_size, opened.bind_vars.limit)
          h.matches("LIMIT @limit", opened.query)
        end)
      end)
    end, debug.traceback)

    package.loaded["arangodb.aql"] = original_aql
    if not ok then
      error(err, 0)
    end
  end),
}
