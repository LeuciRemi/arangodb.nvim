local h = require("tests.helpers")

local envelope = {
  database = "test",
  mode = "execute",
  page = 1,
  count = 2,
  hasMore = false,
  result = {
    { name = "Alice", score = 10 },
    { name = "Bob, Jr.", score = 9 },
  },
}

return {
  h.test("AQL results render as tables and encode CSV safely", function()
    local result = require("arangodb.aql_result")
    local markdown = result.table_text(envelope)
    h.matches("| name | score |", markdown)
    h.matches("| Alice | 10 |", markdown)
    local csv = result.csv_text(envelope)
    h.matches("name,score", csv)
    h.matches('"Bob, Jr%."', csv)
  end),

  h.test("AQL result exports support JSON CSV and Markdown", function()
    local result = require("arangodb.aql_result")
    for _, extension in ipairs({ "json", "csv", "md" }) do
      local path = vim.fn.tempname() .. "." .. extension
      h.eq(path, result.export(envelope, path))
      h.eq(1, vim.fn.filereadable(path))
      h.eq(true, #vim.fn.readfile(path, "b") > 0)
      vim.fn.delete(path)
    end
  end),

  h.test("AQL exports refuse silent overwrite and unsupported paths", function()
    local result = require("arangodb.aql_result")
    local path = vim.fn.tempname() .. ".json"
    vim.fn.writefile({ "original" }, path)
    h.fails("already exists", function()
      result.export(envelope, path)
    end)
    h.eq("original", vim.fn.readfile(path)[1])
    h.eq(path, result.export(envelope, path, { overwrite = true }))
    h.matches("Alice", table.concat(vim.fn.readfile(path), "\n"))
    h.fails("Unsupported AQL export extension", function()
      result.export(envelope, vim.fn.tempname() .. ".xml")
    end)

    local parent = vim.fn.tempname()
    vim.fn.writefile({ "not a directory" }, parent)
    h.fails("Unable to create AQL export directory", function()
      result.export(envelope, parent .. "/result.json")
    end)
    vim.fn.delete(path)
    vim.fn.delete(parent)
  end),

  h.test("mixed tabular AQL values preserve complete rows", function()
    local result = require("arangodb.aql_result")
    local mixed = { result = { 1, { name = "Alice" }, { "nested", 2 }, vim.NIL } }
    local markdown = result.table_text(mixed)
    h.matches("| value |", markdown)
    h.matches("| 1 |", markdown)
    h.matches("name.*Alice", markdown)
    h.matches("nested", markdown)
    h.matches("null", markdown)
  end),
}
