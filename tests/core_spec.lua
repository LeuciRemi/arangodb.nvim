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
    h.fails("keys and values must be strings", function()
      config.setup({ connections = { example = 42 } })
    end)
    h.fails("must contain string `name` and `url`", function()
      config.setup({ default_database = { name = "example" } })
    end)
    config.setup()
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

  h.test("JSON formatting is deterministic", function()
    local utils = require("arangodb.utils")
    h.eq('{\n  "a": [\n    true,\n    null\n  ],\n  "b": 2\n}', utils.json_pretty({ b = 2, a = { true, vim.NIL } }))
  end),
}
