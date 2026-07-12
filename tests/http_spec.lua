local h = require("tests.helpers")

return {
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
}
