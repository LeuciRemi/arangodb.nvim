local h = require("tests.helpers")

return {
  h.test("named AQL queries are scoped, replaced, and removable", function()
    local config = require("arangodb.config")
    local library = require("arangodb.aql_library")
    local path = vim.fn.tempname() .. ".json"
    config.setup({ aql = { library = { path = path }, history = { enabled = false } } })

    library.put({
      name = "active users",
      connection = "local",
      database = "app",
      query = "FOR user IN users RETURN user",
      bind_vars = { active = true },
    })
    library.put({
      name = "active users",
      connection = "local",
      database = "app",
      query = "FOR user IN users FILTER user.active RETURN user",
      bind_vars = {},
    })
    library.put({
      name = "active users",
      connection = "local",
      database = "analytics",
      query = "RETURN 1",
    })

    local entries = library.load()
    h.eq(2, #entries)
    local app_entry
    for _, entry in ipairs(entries) do
      if entry.database == "app" then
        app_entry = entry
      end
    end
    h.eq("FOR user IN users FILTER user.active RETURN user", app_entry.query)
    h.eq(true, library.remove(app_entry))
    h.eq(1, #library.load())
    h.eq(false, library.remove(app_entry))

    vim.fn.delete(path)
    config.setup()
  end),

  h.test("invalid AQL library files are reported without crashing", function()
    local config = require("arangodb.config")
    local library = require("arangodb.aql_library")
    local path = vim.fn.tempname() .. ".json"
    vim.fn.writefile({ "not-json" }, path)
    config.setup({ aql = { library = { path = path } } })
    local entries, warning = library.load()
    h.eq(0, #entries)
    h.matches("Invalid AQL library file", warning)
    vim.fn.delete(path)
    config.setup()
  end),
}
