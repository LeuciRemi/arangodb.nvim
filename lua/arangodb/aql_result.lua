--- AQL result formatting and export helpers.
local M = {}

local utils = require("arangodb.utils")

local function scalar_text(value)
  if value == vim.NIL or value == nil then
    return "null"
  end
  if type(value) == "table" then
    return vim.json.encode(value)
  end
  return tostring(value)
end

local function columns(rows)
  local found = {}
  local names = {}
  for _, row in ipairs(rows) do
    if type(row) ~= "table" or utils.is_list(row) then
      return { "value" }, true
    end
    for name, _ in pairs(row) do
      name = tostring(name)
      if not found[name] then
        found[name] = true
        names[#names + 1] = name
      end
    end
  end
  table.sort(names)
  return #names > 0 and names or { "value" }, false
end

local function cell(row, name, scalar_rows)
  if scalar_rows then
    return scalar_text(row)
  end
  return scalar_text(row[name])
end

local function markdown_cell(value)
  return value:gsub("\n", "\\n"):gsub("|", "\\|")
end

--- Render a result envelope as a compact Markdown table.
function M.table_text(envelope)
  local rows = type(envelope.result) == "table" and envelope.result or {}
  local names, scalar_rows = columns(rows)
  local lines = {
    string.format(
      "<!-- database=%s mode=%s page=%s count=%s hasMore=%s -->",
      tostring(envelope.database or "?"),
      tostring(envelope.mode or "?"),
      tostring(envelope.page or 1),
      tostring(envelope.count or #rows),
      tostring(envelope.hasMore == true)
    ),
    "| " .. table.concat(names, " | ") .. " |",
    "| " .. table.concat(
      vim.tbl_map(function()
        return "---"
      end, names),
      " | "
    ) .. " |",
  }
  for _, row in ipairs(rows) do
    local values = {}
    for _, name in ipairs(names) do
      values[#values + 1] = markdown_cell(cell(row, name, scalar_rows))
    end
    lines[#lines + 1] = "| " .. table.concat(values, " | ") .. " |"
  end
  return table.concat(lines, "\n")
end

local function csv_cell(value)
  value = value:gsub('"', '""')
  if value:find('[,\n\r"]') then
    return '"' .. value .. '"'
  end
  return value
end

--- Encode the current result rows as RFC 4180-style CSV.
function M.csv_text(envelope)
  local rows = type(envelope.result) == "table" and envelope.result or {}
  local names, scalar_rows = columns(rows)
  local lines = { table.concat(vim.tbl_map(csv_cell, names), ",") }
  for _, row in ipairs(rows) do
    local values = {}
    for _, name in ipairs(names) do
      values[#values + 1] = csv_cell(cell(row, name, scalar_rows))
    end
    lines[#lines + 1] = table.concat(values, ",")
  end
  return table.concat(lines, "\r\n") .. "\r\n"
end

--- Render an envelope in a supported interactive format.
function M.render(envelope, format)
  if format == "table" then
    return M.table_text(envelope), "markdown"
  end
  return utils.json_pretty(envelope), "json"
end

local function export_content(envelope, path)
  local extension = vim.fs.basename(path):match("%.([^.]+)$")
  extension = extension and extension:lower() or "json"
  if extension == "csv" then
    return M.csv_text(envelope)
  elseif extension == "md" or extension == "markdown" then
    return M.table_text(envelope) .. "\n"
  elseif extension == "json" then
    return utils.json_pretty(envelope.result or {}) .. "\n"
  end
  error("Unsupported AQL export extension: ." .. extension, 0)
end

function M.path_exists(path)
  local uv = vim.uv or vim.loop
  return uv.fs_stat(vim.fn.expand(path)) ~= nil
end

--- Export the current page atomically as JSON, CSV, or Markdown.
function M.export(envelope, path, opts)
  opts = opts or {}
  path = vim.fn.expand(vim.trim(path or ""))
  if path == "" then
    error("Missing AQL export path", 0)
  end
  local content = export_content(envelope, path)
  if M.path_exists(path) and opts.overwrite ~= true then
    error("AQL export already exists: " .. path, 0)
  end

  local absolute = vim.fn.fnamemodify(path, ":p")
  local directory = vim.fs.dirname(absolute)
  local mkdir_ok, mkdir_result = pcall(vim.fn.mkdir, directory, "p")
  if not mkdir_ok or (mkdir_result == 0 and vim.fn.isdirectory(directory) ~= 1) then
    error("Unable to create AQL export directory: " .. directory, 0)
  end

  local uv = vim.uv or vim.loop
  local temporary = string.format("%s.tmp.%s.%s", absolute, tostring(vim.fn.getpid()), tostring(uv.hrtime()))
  local fd, open_error = uv.fs_open(temporary, "wx", 420)
  if not fd then
    error("Unable to create temporary AQL export: " .. tostring(open_error), 0)
  end
  local written, write_error = uv.fs_write(fd, content, 0)
  local closed, close_error = uv.fs_close(fd)
  if not written or not closed then
    pcall(uv.fs_unlink, temporary)
    error("Unable to write AQL export: " .. tostring(write_error or close_error), 0)
  end
  local renamed, rename_error = uv.fs_rename(temporary, absolute)
  if not renamed then
    pcall(uv.fs_unlink, temporary)
    error("Unable to replace AQL export: " .. tostring(rename_error), 0)
  end
  return path
end

return M
