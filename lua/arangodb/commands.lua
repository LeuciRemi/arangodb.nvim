--- Register global user commands for the plugin entry points.
local M = {}

local created = false

--- Offer completion based on the configured and discovered database names.
--- Never performs network I/O: discovery happens elsewhere, on demand.
local function complete_databases(arg_lead)
  local matches = {}

  for _, name in ipairs(require("arangodb.core").completion_databases()) do
    if arg_lead == "" or vim.startswith(name, arg_lead) then
      matches[#matches + 1] = name
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

  vim.api.nvim_create_user_command("ArangoAql", function(opts)
    local options = {}
    if opts.args ~= "" then
      options.database = opts.args
    end
    require("arangodb").aql(options)
  end, {
    nargs = "?",
    complete = complete_databases,
    desc = "Open a new ArangoDB AQL editor",
  })

  vim.api.nvim_create_user_command("ArangoAqlAttach", function(opts)
    require("arangodb").aql_attach({ database = opts.args ~= "" and opts.args or nil })
  end, {
    nargs = "?",
    complete = complete_databases,
    desc = "Attach ArangoDB AQL tools to the current .aql file",
  })

  vim.api.nvim_create_user_command("ArangoAqlLibrary", function(opts)
    require("arangodb").aql_library({ database = opts.args ~= "" and opts.args or nil })
  end, {
    nargs = "?",
    complete = complete_databases,
    desc = "Open the named ArangoDB AQL query library",
  })

  vim.api.nvim_create_user_command("ArangoGraph", function(opts)
    require("arangodb").graph({ database = opts.args ~= "" and opts.args or nil })
  end, {
    nargs = "?",
    complete = complete_databases,
    desc = "Explore an ArangoDB named graph",
  })
end

return M
