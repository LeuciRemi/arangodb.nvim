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
