local url = vim.env.ARANGODB_TEST_URL
assert(url and url ~= "", "ARANGODB_TEST_URL must point to a disposable ArangoDB database")

local core = require("arangodb.core")
local client = require("arangodb.client")
local config = assert(core.parse_connection(url), "invalid ARANGODB_TEST_URL")
local suffix = tostring((vim.uv or vim.loop).hrtime()):sub(-8)
local source = "arangodb_nvim_test_" .. suffix
local copy = source .. "_copy"
local renamed = source .. "_renamed"

local function await(start)
  local completed = false
  local request_error
  local result
  start(function(err, data)
    request_error = err
    result = data
    completed = true
  end)
  assert(
    vim.wait(5000, function()
      return completed
    end),
    "asynchronous ArangoDB request timed out"
  )
  assert(not request_error, tostring(request_error))
  return result
end

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
  local saved = client.save_document(config, created.id, created.document)
  assert(saved.document.rank == 2)

  created.document.rank = 3
  local conflict_ok, conflict = pcall(client.save_document, config, created.id, created.document)
  assert(not conflict_ok and require("arangodb.errors").is(conflict, "conflict"), "stale revision was not rejected")

  local fields = client.list_fields(config, source, 10)
  assert(vim.tbl_contains(fields, "name"))
  assert(vim.tbl_contains(fields, "rank"))

  local page = client.browse_collection(config, source, "name", "alp", 0, 10)
  assert(#page.items == 1)
  assert(page.items[1].id == created.id)

  local validation = await(function(done)
    client.validate_aql_async(config, "RETURN @value", done)
  end)
  assert(validation.parsed == true)
  assert(vim.tbl_contains(validation.bindVars or {}, "value"))

  local explanation = await(function(done)
    client.explain_aql_async(config, "UPDATE @key WITH { rank: @rank } IN @@collection", {
      key = "first",
      rank = 4,
      ["@collection"] = source,
    }, done)
  end)
  assert(explanation.plan and explanation.plan.isModificationQuery == true)

  local first_aql_page = await(function(done)
    client.execute_aql_async(config, "FOR value IN 1..3 RETURN value", {}, {
      batch_size = 1,
      cursor_ttl = 60,
      profile = true,
    }, done)
  end)
  assert(first_aql_page.hasMore == true and first_aql_page.id)
  assert(first_aql_page.extra and first_aql_page.extra.stats)
  local second_aql_page = await(function(done)
    client.next_aql_page_async(config, first_aql_page.id, done)
  end)
  assert(second_aql_page.result[1] == 2)
  client.close_cursor_async(config, second_aql_page.id or first_aql_page.id)

  await(function(done)
    client.execute_aql_async(config, "UPDATE @key WITH { rank: @rank } IN @@collection", {
      key = "first",
      rank = 4,
      ["@collection"] = source,
    }, {
      batch_size = 10,
      cursor_ttl = 60,
      modification = true,
    }, done)
  end)
  assert(client.get_document(config, created.id).document.rank == 4)

  require("arangodb.config").setup({
    aql = {
      batch_size = 1,
      history = { enabled = false },
    },
  })
  local aql_session = require("arangodb.aql").open({
    config = config,
    connection = "integration",
    query = "FOR value IN 1..3 RETURN value",
  })
  vim.api.nvim_buf_call(aql_session.query_buf, function()
    vim.cmd("ArangoAqlExecute")
  end)
  assert(
    vim.wait(5000, function()
      return aql_session.result_buf ~= nil and aql_session.page_index == 1
    end),
    "AQL editor did not render its first real result page"
  )
  local first_editor_result =
    vim.json.decode(table.concat(vim.api.nvim_buf_get_lines(aql_session.result_buf, 0, -1, false), "\n"))
  assert(first_editor_result.result[1] == 1)
  vim.api.nvim_buf_call(aql_session.result_buf, function()
    vim.cmd("ArangoAqlNextPage")
  end)
  assert(
    vim.wait(5000, function()
      return aql_session.page_index == 2
    end),
    "AQL editor did not render its next real cursor page"
  )
  vim.api.nvim_buf_delete(aql_session.query_buf, { force = true })

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
