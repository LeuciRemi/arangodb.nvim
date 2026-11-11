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

  h.test("adding history after a corrupt file keeps a backup", function()
    local config = require("arangodb.config")
    local path = temporary_path()
    vim.fn.writefile({ "not-json" }, path)
    config.setup({ aql = { history = { path = path } } })
    package.loaded["arangodb.aql_history"] = nil
    local history = require("arangodb.aql_history")

    history.add({ connection = "local", database = "test", query = "RETURN 9", bind_vars = {} })

    local backups = vim.fn.glob(path .. ".corrupt-*", false, true)
    h.eq(1, #backups)
    h.eq({ "not-json" }, vim.fn.readfile(backups[1]))
    local entries = history.load()
    h.eq(1, #entries)
    h.eq("RETURN 9", entries[1].query)
    pcall(uv.fs_unlink, backups[1])
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

  h.test("AQL history entries can be removed by identity and cleared", function()
    local config = require("arangodb.config")
    local path = temporary_path()
    config.setup({ aql = { history = { path = path } } })
    package.loaded["arangodb.aql_history"] = nil
    local history = require("arangodb.aql_history")
    history.add({ connection = "local", database = "test", query = "RETURN @value", bind_vars = { value = 1 } })
    history.add({ connection = "local", database = "test", query = "RETURN 2", bind_vars = {} })

    local identity = {
      connection = "local",
      database = "test",
      query = "RETURN @value",
      bind_vars = { value = 1 },
    }
    h.eq(false, history.remove(vim.tbl_extend("force", identity, { bind_vars = { value = 2 } })))
    h.eq(2, #history.load())
    h.eq(true, history.remove(identity))
    local entries = history.load()
    h.eq(1, #entries)
    h.eq("RETURN 2", entries[1].query)
    h.eq(false, history.remove(identity))

    h.eq(1, history.clear())
    h.eq(0, #history.load())
    h.eq(0, history.clear())
    pcall(uv.fs_unlink, path)
    config.setup()
  end),
}
