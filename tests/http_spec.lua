local h = require("tests.helpers")

local function with_http_server(on_request, callback)
  local uv = vim.uv or vim.loop
  local server = assert(uv.new_tcp())
  local clients = {}
  local state = { requests = 0, disconnected = 0 }
  local server_error
  assert(server:bind("127.0.0.1", 0))
  assert(server:listen(8, function(err)
    if err then
      server_error = err
      return
    end
    local client = assert(uv.new_tcp())
    clients[#clients + 1] = client
    server:accept(client)
    local received = ""
    local handled = false
    client:read_start(function(read_err, chunk)
      if read_err then
        server_error = read_err
      elseif not chunk then
        state.disconnected = state.disconnected + 1
      elseif not handled then
        received = received .. chunk
        if received:find("\r\n\r\n", 1, true) then
          handled = true
          state.requests = state.requests + 1
          local ok, response = pcall(on_request, received)
          if not ok then
            server_error = response
          elseif response then
            client:write(response, function()
              if not client:is_closing() then
                client:close()
              end
            end)
          end
        end
      end
    end)
  end))

  local ok, err = xpcall(function()
    callback(server:getsockname().port, state)
  end, debug.traceback)
  server:close()
  for _, client in ipairs(clients) do
    if not client:is_closing() then
      client:close()
    end
  end
  if not ok then
    error(err, 0)
  end
  if server_error then
    error(server_error, 0)
  end
end

return {
  h.test("plain HTTP resolves localhost and reaches a local server", function()
    with_http_server(function(request)
      h.matches("^GET /_api/version HTTP/1.1", request)
      return "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}"
    end, function(port)
      local response = require("arangodb.http").request({ host = "localhost", port = port, path = "/_api/version" })
      h.eq(200, response.status)
      h.eq("{}", response.body)
    end)
  end),

  h.test("plain HTTP tries the next resolved address after a connection failure", function()
    local uv = vim.uv or vim.loop
    local original_resolve = uv.getaddrinfo
    uv.getaddrinfo = function(_, _, _, callback)
      vim.schedule(function()
        callback(nil, { { addr = "::1" }, { addr = "127.0.0.1" } })
      end)
      return {}
    end
    local ok, err = xpcall(function()
      with_http_server(function()
        return "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}"
      end, function(port)
        local response = require("arangodb.http").request({ host = "localhost", port = port, timeout = 1000 })
        h.eq(200, response.status)
      end)
    end, debug.traceback)
    uv.getaddrinfo = original_resolve
    if not ok then
      error(err, 0)
    end
  end),

  h.test("cancelling a pending DNS lookup ignores its late result", function()
    local uv = vim.uv or vim.loop
    local original_resolve, original_cancel = uv.getaddrinfo, uv.cancel
    local lookup = {}
    local lookup_callback
    local lookup_cancelled = false
    uv.getaddrinfo = function(_, _, _, callback)
      lookup_callback = callback
      return lookup
    end
    uv.cancel = function(value)
      h.eq(lookup, value)
      lookup_cancelled = true
    end
    local ok, err = xpcall(function()
      with_http_server(function()
        error("a cancelled DNS lookup must not open a connection")
      end, function(port, state)
        local calls = 0
        local result
        local handle = require("arangodb.http").request_async({ host = "localhost", port = port }, function(failure)
          calls = calls + 1
          result = failure
        end)
        handle.cancel()
        lookup_callback(nil, { { addr = "127.0.0.1" } })
        assert(vim.wait(1000, function()
          return result ~= nil
        end))
        h.eq(true, lookup_cancelled)
        h.eq(true, require("arangodb.errors").is(result, "cancelled"))
        vim.wait(20, function()
          return calls > 1 or state.requests > 0
        end)
        h.eq(1, calls)
        h.eq(0, state.requests)
      end)
    end, debug.traceback)
    uv.getaddrinfo, uv.cancel = original_resolve, original_cancel
    if not ok then
      error(err, 0)
    end
  end),

  h.test("cancelling a connected HTTP request closes its socket once", function()
    with_http_server(function() end, function(port, state)
      local calls = 0
      local result
      local handle = require("arangodb.http").request_async({ host = "localhost", port = port }, function(err)
        calls = calls + 1
        result = err
      end)
      assert(vim.wait(1000, function()
        return state.requests == 1
      end))
      handle.cancel()
      handle.cancel()
      assert(vim.wait(1000, function()
        return result ~= nil and state.disconnected == 1
      end))
      h.eq(true, require("arangodb.errors").is(result, "cancelled"))
      h.eq(1, calls)
    end)
  end),

  h.test("curl transport preserves raw chunked framing and body byte lengths", function()
    local original_system = vim.system
    -- Use real curl against a local HTTP server: TLS does not affect transfer
    -- decoding, and replacing only the URL avoids a test certificate dependency.
    vim.system = function(args, opts, callback)
      local forwarded = vim.deepcopy(args)
      for index, arg in ipairs(forwarded) do
        if arg == "--url" then
          forwarded[index + 1] = forwarded[index + 1]:gsub("^https://", "http://")
          break
        end
      end
      return original_system(forwarded, opts, callback)
    end
    local ok, err = xpcall(function()
      local body = '{\r\n  "result": true\r\n}'
      with_http_server(function()
        return "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
          .. string.format("%x\r\n%s\r\n0\r\n\r\n", #body, body)
      end, function(port)
        local response = require("arangodb.http").request({ scheme = "https", host = "localhost", port = port })
        h.eq(200, response.status)
        h.eq(body, response.body)
        h.eq(true, vim.json.decode(response.body).result)
      end)
    end, debug.traceback)
    vim.system = original_system
    if not ok then
      error(err, 0)
    end
  end),

  h.test("HTTPS requests keep headers and credentials out of process arguments", function()
    local original_system = vim.system
    local captured_args
    local header_contents
    local header_permissions

    vim.system = function(args, _, callback)
      captured_args = vim.deepcopy(args)
      for index, arg in ipairs(args) do
        if arg == "--header" then
          local path = args[index + 1]:sub(2)
          header_contents = table.concat(vim.fn.readfile(path), "\n")
          header_permissions = vim.fn.getfperm(path)
          break
        end
      end
      vim.schedule(function()
        callback({
          stdout = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}",
          stderr = "",
          code = 0,
        })
      end)
      return { kill = function() end }
    end

    local ok, result = xpcall(function()
      package.loaded["arangodb.http"] = nil
      return require("arangodb.http").request({
        scheme = "https",
        host = "localhost",
        port = 8529,
        path = "/_api/version",
        user = "reader",
        password = "very-secret",
      })
    end, debug.traceback)

    vim.system = original_system
    package.loaded["arangodb.http"] = nil
    if not ok then
      error(vim.inspect(result), 0)
    end

    h.eq(200, result.status)
    local command = table.concat(captured_args, "\n")
    h.eq(nil, command:find("reader", 1, true))
    h.eq(nil, command:find("very-secret", 1, true))
    h.eq(nil, command:find("Authorization:", 1, true))
    h.matches("Authorization: Basic ", header_contents)
    h.eq("rw-------", header_permissions)

    local header_file
    for index, arg in ipairs(captured_args) do
      if arg == "--header" then
        header_file = captured_args[index + 1]
        break
      end
    end
    h.matches("^@", header_file)
    h.eq(0, vim.fn.filereadable(header_file:sub(2)))
  end),

  h.test("asynchronous HTTPS requests can be cancelled", function()
    local original_system = vim.system
    local killed = false
    local header_file
    vim.system = function(args)
      for index, arg in ipairs(args) do
        if arg == "--header" then
          header_file = args[index + 1]:sub(2)
          break
        end
      end
      return {
        kill = function()
          killed = true
        end,
      }
    end

    package.loaded["arangodb.http"] = nil
    local result
    local handle = require("arangodb.http").request_async({
      scheme = "https",
      host = "localhost",
      port = 8529,
      path = "/slow",
      user = "reader",
      password = "very-secret",
    }, function(err)
      result = err
    end)
    handle.cancel()
    assert(vim.wait(1000, function()
      return result ~= nil
    end))

    vim.system = original_system
    package.loaded["arangodb.http"] = nil
    h.eq(true, killed)
    h.eq(true, require("arangodb.errors").is(result, "cancelled"))
    h.eq(0, vim.fn.filereadable(header_file))
  end),

  h.test("asynchronous HTTPS requests can start from a fast event", function()
    local original_system = vim.system
    local callback_error
    local response
    local was_fast_event

    vim.system = function(_, _, callback)
      vim.schedule(function()
        callback({
          stdout = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}",
          stderr = "",
          code = 0,
        })
      end)
      return { kill = function() end }
    end

    local ok, result = xpcall(function()
      package.loaded["arangodb.http"] = nil
      local uv = vim.uv or vim.loop
      local timer = assert(uv.new_timer())
      timer:start(0, 0, function()
        timer:stop()
        timer:close()
        was_fast_event = vim.in_fast_event()
        require("arangodb.http").request_async({
          scheme = "https",
          host = "localhost",
          port = 8529,
          path = "/_db/configured/_api/collection",
        }, function(err, value)
          callback_error = err
          response = value
        end)
      end)

      assert(vim.wait(1000, function()
        return callback_error ~= nil or response ~= nil
      end))
      h.eq(true, was_fast_event)
      h.eq(nil, callback_error)
      h.eq(200, response.status)
    end, debug.traceback)

    vim.system = original_system
    package.loaded["arangodb.http"] = nil
    if not ok then
      error(result, 0)
    end
  end),

  h.test("diagnostic journal records sanitized request metadata", function()
    local original_system = vim.system
    local log_path = vim.fn.tempname()
    require("arangodb.config").setup({
      diagnostics = {
        enabled = true,
        path = log_path,
        max_size = 4096,
      },
    })
    vim.system = function(_, _, callback)
      vim.schedule(function()
        callback({
          stdout = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}",
          stderr = "",
          code = 0,
        })
      end)
      return { kill = function() end }
    end

    package.loaded["arangodb.http"] = nil
    require("arangodb.http").request({
      scheme = "https",
      host = "localhost",
      port = 8529,
      path = "/_api/version",
      user = "reader",
      password = "very-secret",
    })

    vim.system = original_system
    package.loaded["arangodb.http"] = nil
    require("arangodb.config").setup()
    local contents = table.concat(vim.fn.readfile(log_path), "\n")
    local event = vim.json.decode(contents)
    h.eq("success", event.outcome)
    h.eq("/_api/version", event.path)
    h.eq(nil, contents:find("reader", 1, true))
    h.eq(nil, contents:find("very-secret", 1, true))
    vim.fn.delete(log_path)
  end),

  h.test("diagnostic failures never prevent request completion", function()
    local original_system = vim.system
    local original_diagnostics = package.loaded["arangodb.diagnostics"]
    vim.system = function(_, _, callback)
      vim.schedule(function()
        callback({
          stdout = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}",
          stderr = "",
          code = 0,
        })
      end)
      return { kill = function() end }
    end
    package.loaded["arangodb.diagnostics"] = {
      record = function()
        error("diagnostic disk unavailable")
      end,
    }
    package.loaded["arangodb.http"] = nil

    local ok, result = xpcall(function()
      return require("arangodb.http").request({
        scheme = "https",
        host = "localhost",
        port = 8529,
        path = "/_api/version",
        timeout = 100,
      })
    end, debug.traceback)

    vim.system = original_system
    package.loaded["arangodb.http"] = nil
    package.loaded["arangodb.diagnostics"] = original_diagnostics
    if not ok then
      error(result, 0)
    end
    h.eq(200, result.status)
  end),
}
