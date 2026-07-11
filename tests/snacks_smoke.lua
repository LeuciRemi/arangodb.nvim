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
    return #vim.api.nvim_list_wins() > 1
  end),
  "Snacks picker did not open"
)
