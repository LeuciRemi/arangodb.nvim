local h = require("tests.helpers")

local uv = vim.uv or vim.loop

local function temporary_path()
  return vim.fs.joinpath("/tmp", "arangodb-nvim-history-" .. tostring(uv.hrtime()) .. ".json")
end

return {
  h.test("AQL history is bounded, deduplicated, and private", function()
    local config = require("arangodb.config")
    local path = temporary_path()
    config.setup({
      aql = {
        history = {
          path = path,
          max_entries = 2,
        },
      },
    })
    package.loaded["arangodb.aql_history"] = nil
    local history = require("arangodb.aql_history")
    history.add({ connection = "local", database = "test", query = "RETURN 1", bind_vars = { secret = "one" } })
    history.add({ connection = "local", database = "test", query = "RETURN 2", bind_vars = {} })
    history.add({ connection = "local", database = "test", query = "RETURN 1", bind_vars = { secret = "one" } })
    history.add({ connection = "local", database = "test", query = "RETURN 3", bind_vars = {} })

    local entries = history.load()
    h.eq(2, #entries)
    h.eq("RETURN 3", entries[1].query)
    h.eq("RETURN 1", entries[2].query)
    h.eq(nil, entries[1].url)
    h.eq(nil, entries[1].result)
    local stat = assert(uv.fs_stat(path))
    h.eq(384, stat.mode % 512)
    pcall(uv.fs_unlink, path)
    config.setup()
  end),

  h.test("AQL history can omit bind variables and reports corrupt files", function()
    local config = require("arangodb.config")
    local path = temporary_path()
    config.setup({
      aql = {
        history = {
          path = path,
          store_bind_vars = false,
        },
      },
    })
    package.loaded["arangodb.aql_history"] = nil
    local history = require("arangodb.aql_history")
    history.add({ connection = "local", database = "test", query = "RETURN @secret", bind_vars = { secret = "x" } })
    h.eq(nil, history.load()[1].bind_vars)
    vim.fn.writefile({ "not-json" }, path)
    local entries, warning = history.load()
    h.eq(0, #entries)
    h.matches("Invalid AQL history file", warning)
    pcall(uv.fs_unlink, path)
    config.setup()
  end),

  h.test("disabling bind variable storage scrubs existing history entries", function()
    local config = require("arangodb.config")
    local path = temporary_path()
    config.setup({
      aql = {
        history = {
          path = path,
          store_bind_vars = true,
        },
      },
    })
    package.loaded["arangodb.aql_history"] = nil
    local history = require("arangodb.aql_history")
    history.add({ connection = "local", database = "test", query = "RETURN @value", bind_vars = { secret = "x" } })

    config.setup({
      aql = {
        history = {
          path = path,
          store_bind_vars = false,
        },
      },
    })
    history.add({ connection = "local", database = "test", query = "RETURN 1", bind_vars = {} })

    local entries = history.load()
    h.eq(2, #entries)
    h.eq(nil, entries[1].bind_vars)
    h.eq(nil, entries[2].bind_vars)
    h.eq(nil, table.concat(vim.fn.readfile(path), ""):find("secret", 1, true))
    pcall(uv.fs_unlink, path)
    config.setup()
  end),
}
