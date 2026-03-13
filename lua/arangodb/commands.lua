--- Register global user commands for the plugin entry points.
local M = {}

local created = false

--- Offer completion based on the configured and discovered database names.
local function complete_databases(arg_lead)
  local items = require("arangodb.core").available_databases()
  local matches = {}

  for _, item in ipairs(items) do
    if arg_lead == "" or vim.startswith(item.name, arg_lead) then
      matches[#matches + 1] = item.name
    end
  end

  return matches
end

--- Create global commands once per Neovim session.
function M.setup()
  if created then
    return
  end

  created = true

  vim.api.nvim_create_user_command("ArangoBrowse", function(opts)
    local options = {}
    if opts.args ~= "" then
      options.database = opts.args
      options.pick_database = false
    end

    require("arangodb").browse(options)
  end, {
    nargs = "?",
    complete = complete_databases,
    desc = "Open live ArangoDB browser",
  })

  vim.api.nvim_create_user_command("ArangoResume", function()
    require("arangodb").resume()
  end, {
    desc = "Resume the last ArangoDB browser",
  })

  vim.api.nvim_create_user_command("ArangoBack", function()
    require("arangodb").back()
  end, {
    desc = "Go back to the previous ArangoDB view",
  })
end

return M
