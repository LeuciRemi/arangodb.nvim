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

  if vim.fn.has("nvim-0.10") == 1 then
    ok("Neovim >= 0.10")
  else
    error("Neovim >= 0.10 is required")
  end

  info("transport: `" .. core.transport_display() .. "`")
  info("http timeout: `" .. tostring(config.http_timeout or 30000) .. "ms`")
  info("automatic database discovery: `" .. tostring(config.auto_discover == true) .. "`")
  info("tls verify: `" .. tostring(config.tls_verify ~= false) .. "`")
  info("metadata cache ttl: `" .. tostring(config.cache_ttl or 0) .. "ms`")
  info("diagnostic journal: `" .. tostring(config.diagnostics and config.diagnostics.enabled == true) .. "`")
  if config.diagnostics and config.diagnostics.enabled == true then
    info("diagnostic path: `" .. require("arangodb.diagnostics").path() .. "`")
  end
  local aql = config.aql or {}
  local aql_history = aql.history or {}
  info("AQL batch size: `" .. tostring(aql.batch_size or 100) .. "`")
  info("AQL cursor ttl: `" .. tostring(aql.cursor_ttl or 300) .. "s`")
  info("AQL max runtime: `" .. tostring(aql.max_runtime or "server default") .. "`")
  info("AQL result format: `" .. tostring(aql.result_format or "json") .. "`")
  info("AQL history: `" .. tostring(aql_history.enabled ~= false) .. "`")
  if aql_history.enabled ~= false then
    info("AQL history path: `" .. require("arangodb.aql_history").path() .. "`")
  end
  info("AQL library path: `" .. require("arangodb.aql_library").path() .. "`")
  local graph = config.graph or {}
  info("graph traversal depth: `" .. tostring(graph.depth or 2) .. "`")
  info("graph result limit: `" .. tostring(graph.max_nodes or 100) .. "`")
  if type(config.tls_ca_file) == "string" and config.tls_ca_file ~= "" then
    info("tls ca file: `" .. config.tls_ca_file .. "`")
    if vim.fn.filereadable(config.tls_ca_file) ~= 1 then
      warn("TLS CA file is not readable")
    end
  end
  if config.tls_verify == false then
    warn("TLS certificate verification is disabled")
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
