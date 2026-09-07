local h = require("tests.helpers")

local sequence = 0

local function open_editor(opts)
  sequence = sequence + 1
  opts.name = "arangodb-json-editor-test://" .. sequence
  opts.title = "ArangoDB JSON Editor Test"
  return require("arangodb.browser.json_editor").open(opts)
end

local function replace(buf, text)
  vim.bo[buf].modifiable = true
  vim.api.nvim_buf_set_lines(buf, 0, -1, false, vim.split(text, "\n", { plain = true }))
end

local function write_buffer(buf)
  return pcall(vim.api.nvim_buf_call, buf, function()
    vim.cmd("write")
  end)
end

return {
  h.test("JSON metadata editors preserve later edits across save completion and reopening", function()
    local complete
    local values = {}
    local opts = {
      value = { value = 1 },
      on_save = function(value, done)
        values[#values + 1] = value
        complete = done
        return { cancel = function() end }
      end,
    }
    local buf = open_editor(opts)
    h.eq(true, write_buffer(buf))
    local editor = require("arangodb.browser.json_editor")
    h.eq(buf, editor.open({ name = opts.name, value = { value = 99 } }))
    h.eq(1, vim.json.decode(table.concat(vim.api.nvim_buf_get_lines(buf, 0, -1, false), "\n")).value)
    replace(buf, '{"value":2}')
    h.eq(buf, editor.open({ name = opts.name, value = { value = 99 } }))
    complete(nil, { value = 1 })
    h.eq('{"value":2}', table.concat(vim.api.nvim_buf_get_lines(buf, 0, -1, false), "\n"))
    h.eq(true, vim.bo[buf].modified)
    h.eq(true, write_buffer(buf))
    h.eq(2, values[2].value)
    complete(nil, { value = 2 })
    h.eq(false, vim.bo[buf].modified)
    vim.api.nvim_buf_delete(buf, { force = true })
  end),

  h.test("JSON metadata editors isolate connections and keep their save callbacks", function()
    local editor = require("arangodb.browser.json_editor")
    local connections = {
      { host = "alpha.invalid", database = "shared" },
      { host = "beta.invalid", database = "shared" },
    }
    local buffers, saved = {}, {}
    for index, config in ipairs(connections) do
      buffers[index] = editor.open({
        name = "arangodb-json-editor-test://connections",
        config = config,
        value = { value = index },
        on_save = function(value, done)
          saved[index] = value.value
          done(nil, value)
        end,
      })
    end
    h.eq(false, buffers[1] == buffers[2])
    for index, buf in ipairs(buffers) do
      replace(buf, vim.json.encode({ value = index + 10 }))
      h.eq(true, write_buffer(buf))
    end
    h.eq({ 11, 12 }, saved)
    for _, buf in ipairs(buffers) do
      vim.api.nvim_buf_delete(buf, { force = true })
    end
  end),

  h.test("JSON metadata editors reject invalid JSON and arrays", function()
    local saves = 0
    local buf = open_editor({
      value = { enabled = true },
      on_save = function()
        saves = saves + 1
      end,
    })
    replace(buf, "{")
    h.eq(false, write_buffer(buf))
    h.eq(0, saves)
    h.eq(true, vim.bo[buf].modified)

    replace(buf, "[]")
    h.eq(false, write_buffer(buf))
    h.eq(0, saves)
    h.eq(true, vim.bo[buf].modified)
    vim.api.nvim_buf_delete(buf, { force = true })
  end),

  h.test("JSON metadata editors prevent concurrent writes", function()
    local saves = 0
    local complete
    local buf = open_editor({
      value = { enabled = true },
      on_save = function(_, done)
        saves = saves + 1
        complete = done
        return { cancel = function() end }
      end,
    })
    replace(buf, '{"enabled":false}')
    h.eq(true, write_buffer(buf))
    h.eq(true, write_buffer(buf))
    h.eq(1, saves)
    complete(nil, { enabled = false })
    h.eq(false, vim.bo[buf].modified)
    vim.api.nvim_buf_delete(buf, { force = true })
  end),

  h.test("closing a JSON metadata editor cancels its request", function()
    local cancelled = false
    local buf = open_editor({
      value = { enabled = true },
      on_save = function()
        return {
          cancel = function()
            cancelled = true
          end,
        }
      end,
    })
    replace(buf, '{"enabled":false}')
    h.eq(true, write_buffer(buf))
    vim.api.nvim_buf_delete(buf, { force = true })
    h.eq(true, cancelled)
  end),

  h.test("JSON metadata editors keep changes after server errors", function()
    local buf = open_editor({
      value = { enabled = true },
      on_save = function(_, done)
        done("server rejected metadata")
      end,
    })
    replace(buf, '{"enabled":false}')
    h.eq(false, write_buffer(buf))
    h.eq(true, vim.bo[buf].modified)
    h.matches("false", table.concat(vim.api.nvim_buf_get_lines(buf, 0, -1, false), "\n"))
    vim.api.nvim_buf_delete(buf, { force = true })
  end),
}
