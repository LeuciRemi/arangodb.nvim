local h = require("tests.helpers")

return {
  h.test("connection URLs support encoded credentials and IPv6", function()
    local core = require("arangodb.core")
    h.eq({
      scheme = "https",
      user = "user@example.com",
      password = "p@ss:word",
      host = "2001:db8::1",
      port = 8530,
      database = "my db",
    }, core.parse_connection("https://user%40example.com:p%40ss%3Aword@[2001:db8::1]:8530/my%20db"))
  end),

  h.test("connection URLs can omit authentication", function()
    local core = require("arangodb.core")
    h.eq({
      scheme = "http",
      host = "localhost",
      port = 8529,
      database = "example",
    }, core.parse_connection("http://localhost:8529/example"))
  end),

  h.test("invalid connection URLs are rejected", function()
    local core = require("arangodb.core")
    for _, value in ipairs({
      "postgres://localhost/example",
      "http://localhost:0/example",
      "http://localhost:70000/example",
      "http://localhost/example?query=true",
      "http://localhost/",
      "http://localhost:/example",
      "http://[::1]8529/example",
      "http://[::1]:/example",
    }) do
      h.eq(nil, core.parse_connection(value), value)
    end
  end),

  h.test("configuration validates values and copies caller tables", function()
    local config = require("arangodb.config")
    local opts = { page_size = 25, layout = { preview = false } }
    config.setup(opts)
    opts.layout.preview = true
    h.eq(false, config.get().layout.preview)
    h.eq(25, config.get().page_size)
    h.fails("positive integer", function()
      config.setup({ page_size = 0 })
    end)
    h.fails("string or false", function()
      config.setup({ keymaps = { browse = 42 } })
    end)
    h.fails("keys must be strings and values must be strings or tables", function()
      config.setup({ connections = { example = 42 } })
    end)
    h.fails("must contain string `name` and `url`", function()
      config.setup({ default_database = { name = "example" } })
    end)
    h.fails("non%-negative integer", function()
      config.setup({ cache_ttl = -1 })
    end)
    h.fails("positive integer", function()
      config.setup({ diagnostics = { max_size = 0 } })
    end)
    h.fails("aql.batch_size", function()
      config.setup({ aql = { batch_size = 0 } })
    end)
    h.fails("aql.result_split", function()
      config.setup({ aql = { result_split = "left" } })
    end)
    h.fails("aql.max_runtime", function()
      config.setup({ aql = { max_runtime = -1 } })
    end)
    h.fails("aql.history.max_entries", function()
      config.setup({ aql = { history = { max_entries = 0 } } })
    end)
    h.fails("password_command.*non%-empty list", function()
      config.setup({ connections = { example = { url = "http://localhost/example", password_command = {} } } })
    end)
    h.fails("password_command%[2%].*string", function()
      config.setup({
        connections = { example = { url = "http://localhost/example", password_command = { "secret", 42 } } },
      })
    end)
    h.fails("password_command_timeout.*positive integer", function()
      config.setup({
        connections = { example = { url = "http://localhost/example", password_command_timeout = 0 } },
      })
    end)
    config.setup()
  end),

  h.test("picker write mappings are disabled by default", function()
    local picker_keymaps = require("arangodb.config").defaults.picker_keymaps
    for _, name in ipairs({
      "create",
      "create_collection",
      "duplicate_collection",
      "delete",
      "duplicate",
      "truncate",
      "rename",
    }) do
      h.eq(false, picker_keymaps[name], name)
    end
  end),

  h.test("structured connections resolve password providers only when used", function()
    local config = require("arangodb.config")
    local core = require("arangodb.core")
    local calls = 0
    config.setup({
      connections = {
        reporting = {
          url = "https://db.example.com:8530/reporting",
          username = "reader",
          password = function(context)
            calls = calls + 1
            h.eq("reporting", context.name)
            return "provider-secret"
          end,
        },
      },
    })

    local items = core.available_databases()
    h.eq(0, calls)
    h.eq("https://db.example.com:8530/reporting", items[1].url)
    local connection = core.resolve_connection(items[1])
    h.eq(1, calls)
    h.eq("reader", connection.user)
    h.eq("provider-secret", connection.password)

    vim.env.ARANGODB_TEST_PASSWORD = "environment-secret"
    local from_env = core.resolve_connection({
      url = "http://localhost:8529/example",
      user = "root",
      password_env = "ARANGODB_TEST_PASSWORD",
    })
    h.eq("environment-secret", from_env.password)
    vim.env.ARANGODB_TEST_PASSWORD = nil

    local from_command = core.resolve_connection({
      url = "http://localhost:8529/example",
      password_command = { "sh", "-c", "printf command-secret" },
    })
    h.eq("command-secret", from_command.password)
    config.setup()
  end),

  h.test("structured errors include server and request context", function()
    local errors = require("arangodb.errors")
    local err = errors.new({
      kind = "server",
      message = "document missing",
      status = 404,
      error_num = 1202,
      method = "GET",
      path = "/_api/document/items/missing",
    })
    h.eq(true, errors.is(err, "server"))
    h.matches("document missing", tostring(err))
    h.matches("HTTP 404", tostring(err))
    h.matches("ArangoDB 1202", tostring(err))
    h.matches("GET /_api/document/items/missing", tostring(err))
  end),

  h.test("database candidates do not trigger implicit network discovery", function()
    local config = require("arangodb.config")
    local core = require("arangodb.core")
    local original = vim.env.NVIM_ARANGO_REPORTING_URL
    vim.env.NVIM_ARANGO_REPORTING_URL = "https://reader:secret@db.example.com:8530/reporting"
    config.setup()

    local items = core.available_databases()
    h.eq(1, #items)
    h.eq("reporting", items[1].name)
    h.eq("https://reader:secret@db.example.com:8530/reporting", items[1].url)

    vim.env.NVIM_ARANGO_REPORTING_URL = original
  end),

  h.test("setup replaces plugin-owned global keymaps", function()
    local arangodb = require("arangodb")
    arangodb.setup({ keymaps = { browse = "<F8>" } })
    h.eq("ArangoDB Browse", vim.fn.maparg("<F8>", "n", false, true).desc)
    arangodb.setup({ keymaps = { browse = "<F9>" } })
    h.eq({}, vim.fn.maparg("<F8>", "n", false, true))
    h.eq("ArangoDB Browse", vim.fn.maparg("<F9>", "n", false, true).desc)
    vim.keymap.set("n", "<F9>", "<Nop>", { desc = "User replacement" })
    arangodb.setup()
    h.eq("User replacement", vim.fn.maparg("<F9>", "n", false, true).desc)
    vim.keymap.del("n", "<F9>")
  end),

  h.test("commands expose the AQL editor entry point", function()
    require("arangodb.commands").setup()
    h.eq(2, vim.fn.exists(":ArangoAql"))
    h.eq(2, vim.fn.exists(":ArangoAqlAttach"))
    h.eq(2, vim.fn.exists(":ArangoAqlLibrary"))
    h.eq(2, vim.fn.exists(":ArangoGraph"))
  end),

  h.test("JSON formatting is deterministic", function()
    local utils = require("arangodb.utils")
    h.eq('{\n  "a": [\n    true,\n    null\n  ],\n  "b": 2\n}', utils.json_pretty({ b = 2, a = { true, vim.NIL } }))
  end),

  h.test("field path escaping preserves literal dots and backslashes", function()
    local utils = require("arangodb.utils")
    h.eq({ "profile.name" }, utils.field_path_segments("profile\\.name"))
    h.eq({ "profile", "name" }, utils.field_path_segments("profile.name"))
    h.eq({ "path\\name" }, utils.field_path_segments("path\\\\name"))
    h.eq("profile\\.name", utils.escape_field_segment("profile.name"))
  end),
}
