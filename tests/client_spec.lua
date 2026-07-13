local h = require("tests.helpers")

local function with_client(handler, callback)
  require("arangodb.cache").clear()
  local original_http = package.loaded["arangodb.http"]
  local original_client = package.loaded["arangodb.client"]
  package.loaded["arangodb.http"] = {
    request = handler,
    request_async = function(opts, done)
      local cancelled = false
      vim.schedule(function()
        if cancelled then
          return
        end
        local ok, response = pcall(handler, opts)
        if ok then
          done(nil, response)
        else
          done(response)
        end
      end)
      return {
        cancel = function()
          cancelled = true
        end,
      }
    end,
  }
  package.loaded["arangodb.client"] = nil

  local ok, err = xpcall(function()
    callback(require("arangodb.client"))
  end, debug.traceback)

  package.loaded["arangodb.client"] = original_client
  package.loaded["arangodb.http"] = original_http
  if not ok then
    error(err, 0)
  end
end

local config = {
  scheme = "http",
  host = "localhost",
  port = 8529,
  database = "test",
}

local function json_response(value, status)
  return { status = status or 200, body = vim.json.encode(value) }
end

return {
  h.test("new documents never send server-managed attributes", function()
    local requests = {}
    with_client(function(opts)
      requests[#requests + 1] = vim.deepcopy(opts)
      if opts.method == "POST" then
        return json_response({ _key = "new-key" }, 202)
      end
      return json_response({ _id = "items/new-key", _key = "new-key", _rev = "2" })
    end, function(client)
      local result = client.create_document(config, "items", {
        _id = "items/new-key",
        _key = "new-key",
        _rev = "old-revision",
        name = "example",
      })
      h.eq("items/new-key", result.id)
    end)

    local payload = vim.json.decode(requests[1].body)
    h.eq(nil, payload._id)
    h.eq(nil, payload._rev)
    h.eq("new-key", payload._key)
    h.eq("example", payload.name)
  end),

  h.test("saved documents cannot redirect writes by changing their identity", function()
    local requests = {}
    with_client(function(opts)
      requests[#requests + 1] = vim.deepcopy(opts)
      return json_response({})
    end, function(client)
      h.fails("_id cannot be changed", function()
        client.save_document(config, "items/original", {
          _id = "items/other",
          _key = "other",
          name = "example",
        })
      end)
      h.fails("_key cannot be changed", function()
        client.save_document(config, "items/original", {
          _id = "items/original",
          _key = "other",
          name = "example",
        })
      end)
      h.eq(0, #requests)

      client.save_document(config, "items/original", {
        _key = "original",
        name = "example",
      })
    end)
    h.eq("PUT", requests[1].method)
    h.matches("/_api/document/items/original$", requests[1].path)
  end),

  h.test("document revisions protect saves and conflicts are structured", function()
    local requests = {}
    local conflict = true
    with_client(function(opts)
      requests[#requests + 1] = vim.deepcopy(opts)
      if opts.method == "PUT" and conflict then
        return json_response({ error = true, errorNum = 1200, errorMessage = "revision conflict" }, 412)
      end
      return json_response({ _id = "items/a", _key = "a", _rev = "2", name = "saved" })
    end, function(client)
      local ok, err = pcall(client.save_document, config, "items/a", {
        _id = "items/a",
        _key = "a",
        _rev = "1",
        name = "local",
      })
      h.eq(false, ok)
      h.eq(true, require("arangodb.errors").is(err, "conflict"))
      h.eq(412, err.status)
      h.eq('"1"', requests[1].headers["If-Match"])

      conflict = false
      client.save_document(config, "items/a", {
        _id = "items/a",
        _key = "a",
        _rev = "1",
        name = "local",
      }, { force = true })
      h.eq(nil, requests[2].headers)
    end)
  end),

  h.test("cursor-backed browsing reads one page at a time asynchronously", function()
    local requests = {}
    with_client(function(opts)
      requests[#requests + 1] = vim.deepcopy(opts)
      if opts.method == "POST" then
        return json_response({
          result = {
            { _id = "items/a", _key = "a" },
            { _id = "items/b", _key = "b" },
          },
          count = 3,
          hasMore = true,
          id = "cursor-1",
        })
      end
      return json_response({
        result = { { _id = "items/c", _key = "c" } },
        hasMore = false,
      })
    end, function(client)
      local first
      client.browse_collection_async(config, "items", "_key", "", 2, nil, function(err, data)
        assert(not err, tostring(err))
        first = data
      end)
      assert(vim.wait(1000, function()
        return first ~= nil
      end))
      h.eq(2, #first.items)
      h.eq(3, first.total_count)
      h.eq("cursor-1", first.cursor_id)

      local second
      client.browse_collection_async(config, "items", "_key", "", 2, first.cursor_id, function(err, data)
        assert(not err, tostring(err))
        second = data
      end)
      assert(vim.wait(1000, function()
        return second ~= nil
      end))
      h.eq(1, #second.items)
      h.eq(false, second.has_more)
    end)
    h.eq("POST", requests[1].method)
    h.eq(2, vim.json.decode(requests[1].body).batchSize)
    h.eq(false, vim.json.decode(requests[1].body).query:find("LIMIT", 1, true) ~= nil)
    h.eq("PUT", requests[2].method)
    h.matches("/_api/cursor/cursor%-1$", requests[2].path)
  end),

  h.test("cached async requests can start from a fast event", function()
    local callback_result
    local started
    local start_error
    local was_fast_event
    with_client(function()
      return json_response({ result = { { name = "items", type = 2, status = 3 } } })
    end, function(client)
      local uv = vim.uv or vim.loop
      local timer = assert(uv.new_timer())
      timer:start(0, 0, function()
        timer:stop()
        timer:close()
        was_fast_event = vim.in_fast_event()
        started, start_error = pcall(client.list_collection_details_async, {
          scheme = "http",
          host = "localhost",
          port = 8529,
          database = "test",
          user = "root",
          password = "secret",
        }, function(err, data)
          callback_result = err or data
        end)
      end)

      assert(vim.wait(1000, function()
        return started ~= nil and (started == false or callback_result ~= nil)
      end))
      h.eq(true, was_fast_event)
      assert(started, tostring(start_error))
      h.eq("items", callback_result[1].name)
    end)
  end),

  h.test("AQL validation, explain, profile, and cursor pages use async endpoints", function()
    local requests = {}
    with_client(function(opts)
      requests[#requests + 1] = vim.deepcopy(opts)
      if opts.path:match("/_api/query$") then
        return json_response({ parsed = true, bindVars = { "value" } })
      end
      if opts.path:match("/_api/explain$") then
        return json_response({ plan = { isModificationQuery = false } })
      end
      if opts.path:match("/_api/cursor/cursor%-aql$") then
        return json_response({ result = { 3 }, hasMore = false })
      end
      return json_response({ result = { 1, 2 }, count = 3, hasMore = true, id = "cursor-aql" }, 201)
    end, function(client)
      local validated
      client.validate_aql_async(config, "RETURN @value", function(err, data)
        assert(not err, tostring(err))
        validated = data
      end)
      assert(vim.wait(1000, function()
        return validated ~= nil
      end))
      h.eq(true, validated.parsed)

      local explained
      client.explain_aql_async(config, "RETURN @value", { value = 42 }, function(err, data)
        assert(not err, tostring(err))
        explained = data
      end)
      assert(vim.wait(1000, function()
        return explained ~= nil
      end))

      local first
      client.execute_aql_async(config, "RETURN @value", { value = 42 }, {
        batch_size = 2,
        cursor_ttl = 90,
        profile = true,
        max_runtime = 5,
      }, function(err, data)
        assert(not err, tostring(err))
        first = data
      end)
      assert(vim.wait(1000, function()
        return first ~= nil
      end))

      local second
      client.next_aql_page_async(config, first.id, function(err, data)
        assert(not err, tostring(err))
        second = data
      end)
      assert(vim.wait(1000, function()
        return second ~= nil
      end))
      h.eq({ 3 }, second.result)
    end)

    local validation = vim.json.decode(requests[1].body)
    h.eq("RETURN @value", validation.query)
    h.eq(nil, validation.bindVars)
    local explanation = vim.json.decode(requests[2].body)
    h.eq(42, explanation.bindVars.value)
    local execution = vim.json.decode(requests[3].body)
    h.eq(2, execution.batchSize)
    h.eq(90, execution.ttl)
    h.eq(2, execution.options.profile)
    h.eq(5, execution.options.maxRuntime)
    h.eq("POST", requests[4].method)
    h.matches("/_api/cursor/cursor%-aql$", requests[4].path)
  end),

  h.test("missing AQL cursor ids fail asynchronously", function()
    with_client(function()
      return json_response({})
    end, function(client)
      local received
      client.next_aql_page_async(config, nil, function(err)
        received = err
      end)
      assert(vim.wait(1000, function()
        return received ~= nil
      end))
      h.eq(true, require("arangodb.errors").is(received, "protocol"))
    end)
  end),

  h.test("successful AQL writes invalidate cached metadata", function()
    local collection_calls = 0
    with_client(function(opts)
      if opts.path:match("/_api/collection$") then
        collection_calls = collection_calls + 1
        return json_response({ result = { { name = "items", type = 2, status = 3 } } })
      end
      return json_response({ result = {}, hasMore = false }, 201)
    end, function(client)
      client.list_collection_details(config)
      client.list_collection_details(config)
      h.eq(1, collection_calls)

      local completed = false
      client.execute_aql_async(config, "UPDATE {} IN items", {}, { modification = true }, function(err)
        assert(not err, tostring(err))
        completed = true
      end)
      assert(vim.wait(1000, function()
        return completed
      end))
      client.list_collection_details(config)
      h.eq(2, collection_calls)
    end)
  end),

  h.test("metadata uses the TTL cache and figures stay lazy", function()
    local collection_calls = 0
    local figure_calls = 0
    with_client(function(opts)
      if opts.path:match("/figures$") then
        figure_calls = figure_calls + 1
        return json_response({ count = 4, figures = { documentsSize = 100, indexSize = 20 } })
      end
      if opts.path:match("/_api/collection$") then
        collection_calls = collection_calls + 1
        return json_response({ result = { { name = "items", type = 2, status = 3 } } })
      end
      return json_response({ result = { name = "test" } })
    end, function(client)
      h.eq(1, #client.list_collection_details(config))
      h.eq(1, #client.list_collection_details(config))
      h.eq(1, collection_calls)

      client.database_overview(config)
      h.eq(0, figure_calls)
      local metrics = client.collection_metrics(config, "items")
      h.eq(4, metrics.count)
      h.eq(120, metrics.size)
      client.collection_metrics(config, "items")
      h.eq(1, figure_calls)
    end)
  end),

  h.test("escaped field paths distinguish literal dots from nested fields", function()
    local queries = {}
    with_client(function(opts)
      if opts.path:match("/_api/cursor$") then
        local payload = vim.json.decode(opts.body)
        queries[#queries + 1] = payload.query
        return json_response({
          result = {
            {
              _id = "items/a",
              _key = "a",
              ["profile.name"] = "literal",
              profile = { name = "nested" },
              ["items[0]"] = "bracketed",
            },
          },
          hasMore = false,
        })
      end
      return json_response({ count = 1 })
    end, function(client)
      local literal = client.browse_collection(config, "items", "profile\\.name", "value", 0, 10)
      local nested = client.browse_collection(config, "items", "profile.name", "value", 0, 10)
      local bracketed = client.browse_collection(config, "items", "items[0]", "value", 0, 10)
      local fields = client.list_fields(config, "items", 10)
      h.eq("literal", literal.items[1].field_value)
      h.eq("nested", nested.items[1].field_value)
      h.eq("bracketed", bracketed.items[1].field_value)
      h.eq(true, vim.tbl_contains(fields, "profile\\.name"))
      h.eq(true, vim.tbl_contains(fields, "profile.name"))
    end)
    h.eq(true, queries[1]:find('doc["profile.name"]', 1, true) ~= nil)
    h.eq(true, queries[2]:find('doc["profile"]["name"]', 1, true) ~= nil)
    h.eq(true, queries[3]:find('doc["items[0]"]', 1, true) ~= nil)
  end),

  h.test("collection browsing keeps one look-ahead item for pagination", function()
    local cursor_payload
    with_client(function(opts)
      if opts.path:match("/_api/cursor$") then
        cursor_payload = vim.json.decode(opts.body)
        return json_response({
          result = {
            { _id = "items/a", _key = "a" },
            { _id = "items/b", _key = "b" },
            { _id = "items/c", _key = "c" },
          },
          hasMore = false,
        })
      end
      return json_response({ count = 3 })
    end, function(client)
      local result = client.browse_collection(config, "items", "_key", "", 0, 2)
      h.eq(2, #result.items)
      h.eq(true, result.has_more)
      h.eq(3, result.total_count)
    end)
    h.eq(3, cursor_payload.bindVars.limit)
  end),

  h.test("incomplete cursor responses fail instead of losing results", function()
    with_client(function()
      return json_response({ result = {}, hasMore = true })
    end, function(client)
      h.fails("incomplete cursor response", function()
        client.list_fields(config, "items", 10)
      end)
    end)
  end),

  h.test("failed collection copies remove the partially created target", function()
    local deleted_path
    with_client(function(opts)
      if opts.method == "GET" then
        return json_response({ name = "source", type = 2 })
      end
      if opts.method == "DELETE" then
        deleted_path = opts.path
        return json_response({ id = "2" })
      end
      if opts.path:match("/_api/collection$") then
        return json_response({ name = "target", type = 2 })
      end
      return json_response({ error = true, errorMessage = "copy failed" }, 500)
    end, function(client)
      h.fails("copy failed", function()
        client.duplicate_collection(config, "source", "target")
      end)
    end)
    h.matches("/_api/collection/target$", deleted_path)
  end),
}
