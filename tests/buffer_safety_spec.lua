local h = require("tests.helpers")
local utils = require("arangodb.utils")

local connection = { scheme = "http", host = "alpha.invalid", port = 8529, database = "shared" }

local function doc(value, revision)
  return {
    id = "items/a",
    document = { _id = "items/a", _key = "a", _rev = revision or "1", value = value },
    show = false,
  }
end

local function buffer(config)
  return vim.fn.bufnr(utils.connection_buffer_name("arangodb-buffer://shared/items/a", config))
end

local function replace(buf, text)
  vim.api.nvim_buf_set_lines(buf, 0, -1, false, { text })
end

local function contents(buf)
  return table.concat(vim.api.nvim_buf_get_lines(buf, 0, -1, false), "\n")
end

local function write(buf)
  vim.api.nvim_buf_call(buf, function()
    vim.cmd("write")
  end)
end

local function with_browser(client, callback)
  local previous_client = package.loaded["arangodb.client"]
  local previous_browser = package.loaded["arangodb.browser"]
  local existing = {}
  for _, buf in ipairs(vim.api.nvim_list_bufs()) do
    existing[buf] = true
  end
  package.loaded["arangodb.client"] = client
  package.loaded["arangodb.browser"] = nil
  local ok, err = xpcall(function()
    callback(require("arangodb.browser"))
  end, debug.traceback)
  for _, buf in ipairs(vim.api.nvim_list_bufs()) do
    if not existing[buf] then
      pcall(vim.api.nvim_buf_delete, buf, { force = true })
    end
  end
  package.loaded["arangodb.client"] = previous_client
  package.loaded["arangodb.browser"] = previous_browser
  if not ok then
    error(err, 0)
  end
end

return {
  h.test("document buffers isolate configured endpoints and credentials", function()
    local saved = {}
    local variants = {
      connection,
      vim.tbl_extend("force", connection, { host = "beta.invalid" }),
      vim.tbl_extend("force", connection, { port = 8530 }),
      vim.tbl_extend("force", connection, { scheme = "https" }),
      vim.tbl_extend("force", connection, { user = "another-user" }),
      vim.tbl_extend("force", connection, { password = "dummy-secret" }),
    }
    with_browser({
      save_document_async = function(config, _, payload, _, done)
        saved[#saved + 1] = vim.deepcopy(config)
        done(nil, doc(payload.value, "2"))
      end,
    }, function(browser)
      local seen = {}
      for index, config in ipairs(variants) do
        browser.open_document(config, doc(index))
        local buf = buffer(config)
        h.eq(nil, seen[buf])
        seen[buf] = true
        h.eq(nil, vim.api.nvim_buf_get_name(buf):find("dummy-secret", 1, true))
        write(buf)
      end
      h.eq(variants, saved)
      for index, config in ipairs(variants) do
        browser.open_document(vim.deepcopy(config), doc(index))
        h.eq(true, seen[buffer(config)])
      end
    end)
  end),

  h.test("deleting a document leaves another endpoint's modified buffer intact", function()
    local confirm = vim.fn.confirm
    vim.fn.confirm = function()
      return 1
    end
    local ok, err = pcall(function()
      with_browser({
        delete_document_async = function(_, _, done)
          vim.schedule(function()
            done(nil, {})
          end)
          return { cancel = function() end }
        end,
      }, function(browser)
        local other = vim.tbl_extend("force", connection, { host = "beta.invalid" })
        browser.open_document(connection, doc(1))
        browser.open_document(other, doc(2))
        local first, second = buffer(connection), buffer(other)
        replace(second, '{"unfinished":')
        vim.api.nvim_buf_call(first, function()
          vim.cmd("ArangoDocumentDelete")
        end)
        vim.wait(100, function()
          return not vim.api.nvim_buf_is_valid(first)
        end)
        h.eq(false, vim.api.nvim_buf_is_valid(first))
        h.eq(true, vim.api.nvim_buf_is_valid(second))
        h.eq('{"unfinished":', contents(second))
        h.eq(true, vim.bo[second].modified)
      end)
    end)
    vim.fn.confirm = confirm
    if not ok then
      error(err, 0)
    end
  end),

  h.test("destructive guards include other credentials for the same database endpoint", function()
    local deletes = 0
    with_browser({
      delete_document_async = function()
        deletes = deletes + 1
      end,
    }, function(browser)
      local other = vim.tbl_extend("force", connection, { user = "another-user" })
      browser.open_document(connection, doc(1))
      browser.open_document(other, doc(2))
      replace(buffer(other), '{"unfinished":')
      vim.api.nvim_buf_call(buffer(connection), function()
        vim.cmd("ArangoDocumentDelete")
      end)
      h.eq(0, deletes)
      h.eq(true, vim.api.nvim_buf_is_valid(buffer(connection)))
    end)
  end),

  h.test("reopening a modified document preserves text and revision baseline", function()
    with_browser({}, function(browser)
      browser.open_document(connection, doc(1))
      local buf = buffer(connection)
      replace(buf, '{"unfinished":')
      browser.open_document(connection, doc(9, "9"))
      h.eq('{"unfinished":', contents(buf))
      h.eq(true, vim.bo[buf].modified)
      h.eq("1", vim.b[buf].arangodb_document._rev)
    end)
  end),

  h.test("document save preserves later edits and uses the saved revision on the next write", function()
    local complete
    local payloads = {}
    with_browser({
      save_document_async = function(_, _, payload, _, done)
        payloads[#payloads + 1] = payload
        complete = done
        return { cancel = function() end }
      end,
    }, function(browser)
      browser.open_document(connection, doc(1))
      local buf = buffer(connection)
      write(buf)
      local edited = '{"_id":"items/a","_key":"a","_rev":"1","value":2}'
      replace(buf, edited)
      browser.open_document(connection, doc(9, "9"))
      local focused = vim.api.nvim_get_current_buf()
      complete(nil, doc(1, "2"))
      h.eq(focused, vim.api.nvim_get_current_buf())
      h.eq(edited, contents(buf))
      h.eq(true, vim.bo[buf].modified)
      write(buf)
      h.eq(2, payloads[2].value)
      h.eq("2", payloads[2]._rev)
      complete(nil, doc(2, "3"))
      h.eq(false, vim.bo[buf].modified)
      h.eq("3", vim.json.decode(contents(buf))._rev)
    end)
  end),

  h.test("draft creation preserves later edits and the next write updates the created document", function()
    local complete
    local creates, saves = 0, 0
    with_browser({
      create_document_async = function(_, _, _, done)
        creates = creates + 1
        complete = done
        return { cancel = function() end }
      end,
      save_document_async = function(_, id, payload, _, done)
        saves = saves + 1
        h.eq("items/a", id)
        h.eq("2", payload._rev)
        done(nil, doc(payload.value, "3"))
      end,
    }, function(browser)
      browser.open_document(connection, vim.tbl_extend("force", doc(1), { is_new = true }))
      local buf = buffer(connection)
      write(buf)
      replace(buf, '{"_id":"items/a","_key":"a","_rev":null,"value":2}')
      complete(nil, doc(1, "2"))
      h.eq(false, vim.b[buf].arangodb_document_is_new)
      h.eq(true, vim.bo[buf].modified)
      h.eq(2, vim.json.decode(contents(buf)).value)
      write(buf)
      h.eq(1, creates)
      h.eq(1, saves)
      h.eq(false, vim.bo[buf].modified)
    end)
  end),
}
