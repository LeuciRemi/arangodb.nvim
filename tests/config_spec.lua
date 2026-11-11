local h = require("tests.helpers")

return {
  h.test("setup rejects connection URLs that cannot be parsed", function()
    local config = require("arangodb.config")
    h.fails("prod", function()
      config.setup({ connections = { prod = "localhost:8529/mydb" } })
    end)
    h.fails("prod", function()
      config.setup({ connections = { prod = { url = "localhost:8529/mydb" } } })
    end)
    h.fails("prod", function()
      config.setup({ connections = { { name = "prod", url = "localhost:8529/mydb" } } })
    end)
    config.setup()
  end),
}
