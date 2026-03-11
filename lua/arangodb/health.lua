local health = vim.health
local uv = vim.uv or vim.loop

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

  info("transport: `" .. core.transport_display() .. "`")
  info("http timeout: `" .. tostring(config.http_timeout or 30000) .. "ms`")

  if uv then
    ok("libuv transport available")
  else
    error("libuv transport unavailable")
  end

  if config.python_command ~= nil or config.runner ~= nil then
    warn("Legacy Python options are ignored", {
      "Remove `python_command` and `runner` from `require('arangodb').setup()`.",
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
