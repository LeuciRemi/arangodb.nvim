--- Public entry points exposed by the plugin.
local config = require("arangodb.config")

local M = {}

local applied_keymaps = {}

local function set_keymap(lhs, rhs, desc)
  if type(lhs) ~= "string" or lhs == "" then
    return false
  end

  vim.keymap.set("n", lhs, rhs, { desc = desc })
  return true
end

local function remove_keymaps()
  for _, mapping in pairs(applied_keymaps) do
    local current = vim.fn.maparg(mapping.lhs, "n", false, true)
    if type(current) == "table" and current.desc == mapping.desc then
      pcall(vim.keymap.del, "n", mapping.lhs)
    end
  end
  applied_keymaps = {}
end

local function apply_keymaps()
  remove_keymaps()
  local options = config.get()
  local keymaps = options.keymaps or {}
  if set_keymap(keymaps.browse, function()
    M.browse()
  end, "ArangoDB Browse") then
    applied_keymaps.browse = { lhs = keymaps.browse, desc = "ArangoDB Browse" }
  end
  if set_keymap(keymaps.resume, function()
    M.resume()
  end, "ArangoDB Resume") then
    applied_keymaps.resume = { lhs = keymaps.resume, desc = "ArangoDB Resume" }
  end
  if set_keymap(keymaps.back, function()
    M.back()
  end, "ArangoDB Back") then
    applied_keymaps.back = { lhs = keymaps.back, desc = "ArangoDB Back" }
  end
end

--- Merge user options into the plugin configuration and install global keymaps.
function M.setup(opts)
  config.setup(opts)
  apply_keymaps()
  return config.get()
end

--- Open the ArangoDB browser UI.
function M.browse(opts)
  return require("arangodb.browser").open(opts)
end

--- Reopen the last ArangoDB picker when it is still available.
function M.resume()
  return require("arangodb.browser").resume()
end

--- Return to the previous ArangoDB picker or document view.
function M.back()
  return require("arangodb.browser").back()
end

return M
