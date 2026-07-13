local database = {
  scheme = "http",
  host = "localhost",
  port = 8529,
  database = "test",
}

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

require("snacks.picker.core.picker").get()[1]:close()
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
