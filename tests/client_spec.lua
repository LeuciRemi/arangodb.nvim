local h = require("tests.helpers")

local function with_client(handler, callback)
  local original_http = package.loaded["arangodb.http"]
  local original_client = package.loaded["arangodb.client"]
  package.loaded["arangodb.http"] = { request = handler }
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
