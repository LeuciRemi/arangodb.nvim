--- Shared Snacks picker and prompt primitives for the ArangoDB browser.
local M = {}

local backdrop_augroup = vim.api.nvim_create_augroup("arangodb_nvim_picker_backdrop", { clear = true })

local function plugin_options()
  return require("arangodb.config").get()
end

local function resolve_picker_preset(preset)
  if type(preset) == "string" and preset ~= "" and preset ~= "auto" then
    return preset
  end
  return vim.o.columns >= 160 and vim.o.lines >= 32 and "default" or "vertical"
end

function M.picker_layout()
  local layout = plugin_options().layout or {}
  local preset = resolve_picker_preset(layout.preset)
  local preview = layout.preview ~= false
  if preset == "vertical" then
    local definition = {
      backdrop = true,
      width = vim.o.columns < 100 and 0.95 or 0.7,
      height = vim.o.lines < 35 and 0.95 or 0.85,
      box = "vertical",
      border = true,
      title = "{title} {live} {flags}",
      title_pos = "center",
      { win = "input", height = 1, border = "bottom" },
      { win = "list", border = "none" },
    }
    if preview then
      definition[#definition + 1] = { win = "preview", title = "{preview}", height = 0.4, border = "top" }
    end
    return {
      preset = "vertical",
      preview = preview,
      layout = definition,
    }
  end
  return { preset = preset, preview = preview }
end

function M.restore_backdrop(picker)
  if not picker or picker.closed or not picker.layout or not picker.layout.root then
    return
  end
  local root = picker.layout.root
  if root.opts and root.opts.backdrop and not root.backdrop and type(root.drop) == "function" then
    pcall(root.drop, root)
  end
end

function M.watch_backdrop(picker)
  if not picker or picker.closed or not picker.layout or not picker.layout.root then
    return
  end
  local root_win = picker.layout.root.win
  if not root_win or not vim.api.nvim_win_is_valid(root_win) then
    return
  end
  vim.api.nvim_clear_autocmds({ group = backdrop_augroup })
  vim.api.nvim_create_autocmd({ "FocusGained", "WinEnter" }, {
    group = backdrop_augroup,
    callback = function()
      if picker.closed then
        return true
      end
      vim.schedule(function()
        M.restore_backdrop(picker)
      end)
    end,
  })
end

function M.select_options(opts)
  opts = vim.deepcopy(opts or {})
  opts.snacks = vim.tbl_deep_extend("force", {
    layout = { preset = "select", layout = { backdrop = true } },
    win = {
      input = { wo = { winhighlight = "Normal:Pmenu,NormalFloat:Pmenu,FloatBorder:FloatBorder,FloatTitle:Title" } },
      list = { wo = { winhighlight = "Normal:Pmenu,NormalFloat:Pmenu,CursorLine:PmenuSel" } },
    },
  }, opts.snacks or {})
  return opts
end

function M.picker_key(lhs, action, mode, desc, enabled)
  if enabled == false or type(lhs) ~= "string" or lhs == "" then
    return nil
  end
  return { [lhs] = { action, mode = mode, desc = desc } }
end

function M.merge_keymaps(...)
  local merged = {}
  for index = 1, select("#", ...) do
    local mappings = select(index, ...)
    if type(mappings) == "table" then
      merged = vim.tbl_extend("force", merged, mappings)
    end
  end
  return merged
end

function M.key_label(lhs)
  if type(lhs) ~= "string" or lhs == "" then
    return nil
  end
  return lhs
end

function M.action_label(label, lhs)
  local key = M.key_label(lhs)
  return key and string.format("%s (%s)", label, key) or label
end

function M.hint_text(parts)
  local visible = {}
  for _, part in ipairs(parts) do
    if type(part) == "string" and part ~= "" then
      visible[#visible + 1] = part
    end
  end
  return #visible > 0 and ("  [" .. table.concat(visible, "  ") .. "]") or ""
end

function M.get_snacks()
  local ok, snacks = pcall(require, "snacks")
  if ok then
    return snacks
  end
  require("arangodb.core").notify_error("`folke/snacks.nvim` is required to use the ArangoDB browser")
end

function M.close_picker(picker)
  if picker and not picker.closed then
    picker:close()
  end
end

function M.refresh_picker(picker, opts)
  if picker and not picker.closed then
    picker:find(opts or { refresh = true })
  end
end

function M.execute_action(picker, action)
  if picker and not picker.closed and picker.list and picker.list.win then
    picker.list.win:execute(action)
  end
end

function M.restore_input_focus(picker)
  if not picker or picker.closed then
    return
  end
  vim.schedule(function()
    if not picker or picker.closed or not picker.input or not picker.input.win then
      return
    end
    local input_win = picker.input.win.win
    if not input_win or not vim.api.nvim_win_is_valid(input_win) then
      return
    end
    picker:focus("input", { show = true })
    if vim.api.nvim_get_current_win() == input_win and vim.fn.mode():sub(1, 1) ~= "i" then
      vim.cmd("startinsert!")
    end
  end)
end

function M.set_search(picker, value)
  if picker and not picker.closed and picker.input then
    picker.input:set(nil, value or "")
  end
end

function M.prompt_select(items, opts, callback)
  vim.ui.select(items, M.select_options(opts), function(choice)
    if choice then
      callback(choice)
    end
  end)
end

function M.prompt_input(opts, callback)
  vim.ui.input(opts, function(value)
    if value ~= nil then
      callback(value)
    end
  end)
end

function M.format_count(value)
  if type(value) ~= "number" then
    return "unavailable"
  end
  local text = tostring(math.floor(value + 0.5))
  local groups = {}
  while #text > 3 do
    groups[#groups + 1] = text:sub(-3)
    text = text:sub(1, -4)
  end
  groups[#groups + 1] = text
  local formatted = {}
  for index = #groups, 1, -1 do
    formatted[#formatted + 1] = groups[index]
  end
  return table.concat(formatted, " ")
end

function M.format_bytes(value)
  if type(value) ~= "number" then
    return "unavailable"
  end
  local units = { "B", "KB", "MB", "GB", "TB" }
  local size, unit = value, 1
  while size >= 1024 and unit < #units do
    size = size / 1024
    unit = unit + 1
  end
  return unit == 1 and string.format("%d %s", size, units[unit]) or string.format("%.1f %s", size, units[unit])
end

function M.format_flag(value)
  if value == nil then
    return "n/a"
  end
  return value and "yes" or "no"
end

function M.preview_line(label, value)
  if value == nil or value == "" then
    return nil
  end
  return string.format("%-14s %s", label .. ":", value)
end

return M
