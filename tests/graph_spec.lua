local h = require("tests.helpers")

local config = {
  scheme = "http",
  host = "localhost",
  port = 8529,
  database = "test",
}

local function async(value, done)
  vim.schedule(function()
    done(nil, vim.deepcopy(value))
  end)
  return { cancel = function() end }
end

return {
  h.test("graph explorer cancels stale requests and supports configurable navigation", function()
    local original_client = package.loaded["arangodb.client"]
    local original_graph = package.loaded["arangodb.graph"]
    local original_input = vim.ui.input
    local config_module = require("arangodb.config")
    local requests = {}
    local inputs = { "users/carol", "4" }
    config_module.setup({ graph_keymaps = { direction = "x" } })
    package.loaded["arangodb.client"] = {
      list_graphs_async = function(_, done)
        done(nil, { { name = "social", edgeDefinitions = {} } })
      end,
      traverse_graph_async = function(_, _, start, opts, done)
        local request = { start = start, opts = vim.deepcopy(opts), done = done, cancelled = false }
        requests[#requests + 1] = request
        return {
          cancel = function()
            request.cancelled = true
          end,
        }
      end,
      close_cursor_async = function() end,
    }
    package.loaded["arangodb.graph"] = nil
    vim.ui.input = function(_, done)
      done(table.remove(inputs, 1))
    end

    local ok, err = xpcall(function()
      local graph = require("arangodb.graph")
      graph.open({ config = config, graph = "social", start = "users/alice" })
      local session = graph.session(vim.api.nvim_get_current_buf())
      h.eq(1, #requests)
      vim.api.nvim_buf_call(session.buf, function()
        vim.cmd("ArangoGraphRefresh")
      end)
      h.eq(true, requests[1].cancelled)
      h.eq(2, #requests)
      requests[2].done(nil, { result = { { vertex = { _id = "users/bob" }, depth = 0 } } })
      requests[1].done(nil, { result = { { vertex = { _id = "users/stale" }, depth = 0 } } })
      local text = table.concat(vim.api.nvim_buf_get_lines(session.buf, 0, -1, false), "\n")
      h.matches("users/bob", text)
      h.eq(nil, text:match("users/stale"))

      vim.api.nvim_buf_call(session.buf, function()
        vim.cmd("ArangoGraphRoot")
      end)
      h.eq("users/carol", requests[3].start)
      requests[3].done(nil, { result = {} })
      vim.api.nvim_buf_call(session.buf, function()
        vim.cmd("ArangoGraphDepth")
      end)
      h.eq(4, requests[4].opts.depth)
      requests[4].done(nil, { result = {} })

      vim.api.nvim_buf_call(session.buf, function()
        local mapping = vim.fn.maparg("x", "n", false, true)
        h.eq("function", type(mapping.callback))
        mapping.callback()
      end)
      h.eq("OUTBOUND", requests[5].opts.direction)
      vim.api.nvim_buf_delete(session.buf, { force = true })
      h.eq(true, requests[5].cancelled)
    end, debug.traceback)

    vim.ui.input = original_input
    package.loaded["arangodb.graph"] = original_graph
    package.loaded["arangodb.client"] = original_client
    config_module.setup({})
    if not ok then
      error(err, 0)
    end
  end),

  h.test("graph explorer selects a named graph and renders reachable vertices", function()
    local original_client = package.loaded["arangodb.client"]
    local original_graph = package.loaded["arangodb.graph"]
    local original_select = vim.ui.select
    local original_input = vim.ui.input
    package.loaded["arangodb.client"] = {
      list_graphs_async = function(_, done)
        return async({ { name = "social", edgeDefinitions = { { collection = "knows" } } } }, done)
      end,
      traverse_graph_async = function(_, graph, start, opts, done)
        h.eq("social", graph)
        h.eq("users/alice", start)
        h.eq(2, opts.depth)
        return async({
          result = {
            { vertex = { _id = "users/alice", name = "Alice" }, edge = vim.NIL, depth = 0 },
            {
              vertex = { _id = "users/bob", name = "Bob" },
              edge = { _id = "knows/1", _from = "users/alice", _to = "users/bob" },
              depth = 1,
            },
          },
          hasMore = false,
        }, done)
      end,
      close_cursor_async = function() end,
    }
    package.loaded["arangodb.graph"] = nil
    vim.ui.select = function(items, _, done)
      done(items[1])
    end
    vim.ui.input = function(_, done)
      done("users/alice")
    end

    local ok, err = xpcall(function()
      local graph = require("arangodb.graph")
      graph.open({ config = config })
      local session
      assert(vim.wait(1000, function()
        session = graph.session(vim.api.nvim_get_current_buf())
        return session ~= nil and not session.request
      end))
      local text = table.concat(vim.api.nvim_buf_get_lines(session.buf, 0, -1, false), "\n")
      h.matches("users/alice", text)
      h.matches("users/bob", text)
      h.matches("knows/1", text)
      vim.api.nvim_buf_delete(session.buf, { force = true })
    end, debug.traceback)

    vim.ui.select = original_select
    vim.ui.input = original_input
    package.loaded["arangodb.graph"] = original_graph
    package.loaded["arangodb.client"] = original_client
    if not ok then
      error(err, 0)
    end
  end),
}
