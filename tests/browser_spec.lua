local h = require("tests.helpers")

local config = {
  scheme = "http",
  host = "localhost",
  port = 8529,
  database = "test",
}

local function with_browser(client, callback)
  local original_client = package.loaded["arangodb.client"]
  local original_browser = package.loaded["arangodb.browser"]
  package.loaded["arangodb.client"] = client
  package.loaded["arangodb.browser"] = nil

  local ok, err = xpcall(function()
    callback(require("arangodb.browser"))
  end, debug.traceback)

  package.loaded["arangodb.browser"] = original_browser
  package.loaded["arangodb.client"] = original_client
  if not ok then
    error(err, 0)
  end
end

local function document()
  return {
    database = "test",
    id = "items/a",
    key = "a",
    collection = "items",
    document = { _id = "items/a", _key = "a", _rev = "1", name = "local" },
    preview = '{"_id":"items/a","_key":"a","_rev":"1","name":"local"}',
    show = false,
  }
end

return {
  h.test("document buffers save against their canonical identity", function()
    local saved_id
    with_browser({
      save_document = function(_, id, payload)
        saved_id = id
        return {
          database = "test",
          id = id,
          key = "a",
          collection = "items",
          document = payload,
          preview = require("arangodb.utils").json_pretty(payload),
        }
      end,
    }, function(browser)
      local doc = document()
      browser.open_document(config, doc)
      local buf = vim.fn.bufnr("arangodb-buffer://test/items/a")
      vim.api.nvim_buf_call(buf, function()
        vim.cmd("ArangoDocumentSave")
      end)
      h.eq("items/a", saved_id)
      vim.api.nvim_buf_delete(buf, { force = true })
    end)
  end),

  h.test("revision conflicts can be explicitly force-overwritten", function()
    local calls = {}
    local original_select = vim.ui.select
    vim.ui.select = function(_, _, done)
      done("Force overwrite")
    end

    local ok, err = xpcall(function()
      with_browser({
        save_document = function(_, id, payload, opts)
          calls[#calls + 1] = { id = id, opts = opts }
          if #calls == 1 then
            error(
              require("arangodb.errors").new({
                kind = "conflict",
                message = "revision conflict",
                status = 412,
              }),
              0
            )
          end
          return {
            database = "test",
            id = id,
            key = "a",
            collection = "items",
            document = payload,
            preview = require("arangodb.utils").json_pretty(payload),
          }
        end,
      }, function(browser)
        browser.open_document(config, document())
        local buf = vim.fn.bufnr("arangodb-buffer://test/items/a")
        vim.api.nvim_buf_call(buf, function()
          vim.cmd("ArangoDocumentSave")
        end)
        h.eq(2, #calls)
        h.eq(nil, calls[1].opts)
        h.eq(true, calls[2].opts.force)
        vim.api.nvim_buf_delete(buf, { force = true })
      end)
    end, debug.traceback)
    vim.ui.select = original_select
    if not ok then
      error(err, 0)
    end
  end),
}
