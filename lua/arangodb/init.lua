local config = require("arangodb.config")

local M = {}

local keymaps_applied = false

local function set_keymap(lhs, rhs, desc)
  if type(lhs) ~= "string" or lhs == "" then
    return
  end

  vim.keymap.set("n", lhs, rhs, { desc = desc })
end

local function apply_keymaps()
  if keymaps_applied then
    return
  end

  local options = config.get()
  local keymaps = options.keymaps or {}
  set_keymap(keymaps.browse, function()
    M.browse()
  end, "ArangoDB Browse")
  set_keymap(keymaps.resume, function()
    M.resume()
  end, "ArangoDB Resume")

  keymaps_applied = true
end

function M.setup(opts)
  config.setup(opts)
  apply_keymaps()
  return config.get()
end

function M.browse(opts)
  return require("arangodb.browser").open(opts)
end

function M.resume()
  return require("arangodb.browser").resume()
end

function M.back()
  return require("arangodb.browser").back()
end

return M
