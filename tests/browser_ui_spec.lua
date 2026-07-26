local h = require("tests.helpers")

local function with_screen(columns, lines, opts, callback)
  local original_columns = vim.o.columns
  local original_lines = vim.o.lines
  local config = require("arangodb.config")
  vim.o.columns = columns
  vim.o.lines = lines
  config.setup({ layout = opts })
  local ok, err = xpcall(function()
    callback(require("arangodb.browser.ui"))
  end, debug.traceback)
  vim.o.columns = original_columns
  vim.o.lines = original_lines
  config.setup({})
  if not ok then
    error(err, 0)
  end
end

return {
  h.test("automatic picker layouts account for width and height", function()
    with_screen(180, 50, { preset = "auto", preview = true }, function(ui)
      h.eq("default", ui.picker_layout().preset)
    end)
    with_screen(180, 24, { preset = "auto", preview = true }, function(ui)
      local layout = ui.picker_layout()
      h.eq("vertical", layout.preset)
      h.eq(0.95, layout.layout.height)
    end)
    with_screen(70, 40, { preset = "auto", preview = true }, function(ui)
      local layout = ui.picker_layout()
      h.eq("vertical", layout.preset)
      h.eq(0.95, layout.layout.width)
      h.eq(nil, layout.layout.min_width)
      h.eq(nil, layout.layout.min_height)
    end)
  end),

  h.test("vertical picker layouts omit the preview when disabled", function()
    with_screen(80, 24, { preset = "vertical", preview = false }, function(ui)
      local layout = ui.picker_layout()
      h.eq(false, layout.preview)
      h.eq(2, #layout.layout)
      h.eq("input", layout.layout[1].win)
      h.eq("list", layout.layout[2].win)
    end)
  end),

  h.test("backdrop restoration tolerates API changes", function()
    local ui = require("arangodb.browser.ui")
    ui.restore_backdrop({ layout = { root = { opts = { backdrop = true } } } })
    local dropped = false
    local root = {
      opts = { backdrop = true },
      drop = function()
        dropped = true
      end,
    }
    ui.restore_backdrop({ layout = { root = root } })
    h.eq(true, dropped)
  end),
}
