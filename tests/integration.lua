local url = vim.env.ARANGODB_TEST_URL
assert(url and url ~= "", "ARANGODB_TEST_URL must point to a disposable ArangoDB database")

local core = require("arangodb.core")
local client = require("arangodb.client")
local config = assert(core.parse_connection(url), "invalid ARANGODB_TEST_URL")
local suffix = tostring((vim.uv or vim.loop).hrtime()):sub(-8)
local source = "arangodb_nvim_test_" .. suffix
local copy = source .. "_copy"
local renamed = source .. "_renamed"

local ok, err = xpcall(function()
  client.create_collection(config, source, "document")
  local created = client.create_document(config, source, {
    _key = "first",
    _id = source .. "/first",
    _rev = vim.NIL,
    name = "alpha",
    rank = 1,
  })
  assert(created.id == source .. "/first")

  created.document.rank = 2
  local saved = client.save_document(config, created.document)
  assert(saved.document.rank == 2)

  local fields = client.list_fields(config, source, 10)
  assert(vim.tbl_contains(fields, "name"))
  assert(vim.tbl_contains(fields, "rank"))

  local page = client.browse_collection(config, source, "name", "alp", 0, 10)
  assert(#page.items == 1)
  assert(page.items[1].id == created.id)

  local duplicated = client.duplicate_collection(config, source, copy)
  assert(duplicated.copied_count == 1)
  client.rename_collection(config, copy, renamed)
  client.truncate_collection(config, renamed)

  client.delete_document(config, created.id)
  assert(client.browse_collection(config, source, "_key", "", 0, 10).total_count == 0)
end, debug.traceback)

pcall(client.delete_collection, config, renamed)
pcall(client.delete_collection, config, copy)
pcall(client.delete_collection, config, source)

assert(ok, err)
vim.api.nvim_out_write("ArangoDB integration test passed\n")
