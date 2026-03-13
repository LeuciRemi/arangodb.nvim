--- Healthcheck entry point reported by :checkhealth arangodb.
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

--- Report transport support, dependencies, and detected database candidates.
function M.check()
  local core = require("arangodb.core")
  local config = require("arangodb.config").get()
  local items = core.available_databases()
  local https_requested = false

  for _, item in ipairs(items) do
    local connection = core.parse_connection(item.url)
    if connection and connection.scheme == "https" then
      https_requested = true
      break
    end
  end

  start("arangodb.nvim")

  info("transport: `" .. core.transport_display() .. "`")
  info("http timeout: `" .. tostring(config.http_timeout or 30000) .. "ms`")
  info("tls verify: `" .. tostring(config.tls_verify ~= false) .. "`")
  if type(config.tls_ca_file) == "string" and config.tls_ca_file ~= "" then
    info("tls ca file: `" .. config.tls_ca_file .. "`")
  end

  if uv then
    ok("libuv transport available")
  else
    error("libuv transport unavailable")
  end

  if core.https_transport_available() then
    ok("curl available for HTTPS transport")
  elseif https_requested then
    warn("curl not found; https:// connections will fail", {
      "Install `curl` to enable HTTPS ArangoDB connections.",
    })
  else
    info("install `curl` to enable https:// connections")
  end

  if pcall(require, "snacks") then
    ok("`folke/snacks.nvim` is available")
  else
    warn("`folke/snacks.nvim` not found", {
      "Install `folke/snacks.nvim` to use the live browser picker.",
    })
  end

  if #items > 0 then
    info("database candidates: " .. join_names(items))
  else
    warn("No database candidates found", {
      "Configure `connections` in `require('arangodb').setup()`.",
      "Or set `NVIM_ARANGO_*` environment variables.",
    })
  end
end

return M
