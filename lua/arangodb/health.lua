local health = vim.health

local start = health.start or health.report_start
local ok = health.ok or health.report_ok
local warn = health.warn or health.report_warn
local error = health.error or health.report_error
local info = health.info or health.report_info

local M = {}

local function join_names(items)
  local names = {}
  for _, item in ipairs(items) do
    names[#names + 1] = item.name
  end
  return table.concat(names, ", ")
end

function M.check()
  local core = require("arangodb.core")
  local config = require("arangodb.config").get()

  start("arangodb.nvim")

  info("python command: `" .. core.python_command_display() .. "`")

  local binary = core.python_binary()
  if vim.fn.executable(binary) == 1 then
    ok("python executable found: `" .. binary .. "`")
  else
    error("python executable not found: `" .. binary .. "`", {
      "Set `python_command` in `require('arangodb').setup()`.",
    })
  end

  local runner = core.runner_script()
  if runner and core.script_exists() then
    ok("runner script found: `" .. runner .. "`")
  else
    error("runner script not found", {
      "Make sure the plugin is installed with `python/arango_browser.py`.",
    })
  end

  if pcall(require, "snacks") then
    ok("`folke/snacks.nvim` is available")
  else
    warn("`folke/snacks.nvim` not found", {
      "Install `folke/snacks.nvim` to use the live browser picker.",
    })
  end

  local items = core.available_databases()
  if #items > 0 then
    info("database candidates: " .. join_names(items))
  else
    warn("No database candidates found", {
      "Configure `connections` in `require('arangodb').setup()`.",
      "Or set `NVIM_ARANGO_*` environment variables.",
    })
  end

  if config.legacy_globals then
    info("legacy globals enabled: `vim.g.arango_connections`, `vim.g.dbs`")
  end
end

return M
