--- Reusable JSON acwrite buffer for asynchronous ArangoDB metadata editors.
local M = {}

local utils = require("arangodb.utils")
local editors = {}
local pending = {}

local function set_value(buf, value)
  local modifiable = vim.bo[buf].modifiable
  vim.bo[buf].modifiable = true
  vim.api.nvim_buf_set_lines(buf, 0, -1, false, vim.split(utils.json_pretty(value), "\n", { plain = true }))
  vim.bo[buf].modified = false
  vim.bo[buf].modifiable = modifiable
end

local function buffer_value(buf)
  local text = table.concat(vim.api.nvim_buf_get_lines(buf, 0, -1, false), "\n")
  local ok, value = pcall(vim.json.decode, text)
  if not ok or type(value) ~= "table" or require("arangodb.utils").is_list(value) then
    error("The editor must contain a JSON object", 0)
  end
  return value
end

--- Open a JSON editor whose writes are completed by an asynchronous callback.
function M.open(opts)
  opts = opts or {}
  local name = assert(opts.name, "JSON editor name is required")
  if opts.config then
    name = utils.connection_buffer_name(name, opts.config)
  end
  local buf = vim.fn.bufadd(name)
  vim.bo[buf].swapfile = false
  vim.fn.bufload(buf)
  if editors[buf] and (vim.bo[buf].modified or pending[buf]) then
    vim.cmd("buffer " .. buf)
    return buf
  end
  if opts.config then
    vim.b[buf].arangodb_connection_id = utils.connection_id(opts.config)
  end
  editors[buf] = opts
  set_value(buf, opts.value or vim.empty_dict())
  vim.bo[buf].filetype = "json"
  vim.bo[buf].buftype = opts.readonly and "nofile" or "acwrite"
  vim.bo[buf].buflisted = true
  vim.bo[buf].bufhidden = "hide"
  vim.bo[buf].swapfile = false
  vim.bo[buf].modifiable = opts.readonly ~= true
  vim.bo[buf].readonly = opts.readonly == true

  if opts.title then
    vim.b[buf].arangodb_editor_title = opts.title
  end

  if not opts.readonly and not vim.b[buf].arangodb_json_editor_initialized then
    local active_request
    vim.api.nvim_create_autocmd("BufWriteCmd", {
      buffer = buf,
      callback = function()
        local current = editors[buf] or opts
        if active_request then
          vim.notify("An ArangoDB metadata update is already in progress", vim.log.levels.INFO)
          return
        end
        local ok, value = pcall(buffer_value, buf)
        if not ok then
          require("arangodb.core").notify_error(value, current.title)
          return
        end
        local completed = false
        local changedtick = vim.api.nvim_buf_get_changedtick(buf)
        pending[buf] = true
        local started, handle = pcall(current.on_save, value, function(err, saved)
          completed = true
          active_request = nil
          pending[buf] = nil
          if err then
            require("arangodb.core").notify_error(err, current.title)
            return
          end
          if vim.api.nvim_buf_is_valid(buf) and vim.api.nvim_buf_get_changedtick(buf) == changedtick then
            set_value(buf, current.normalize and current.normalize(saved, value) or value)
          end
          vim.notify(current.success_message or "ArangoDB metadata updated", vim.log.levels.INFO)
          if current.on_success then
            current.on_success(saved)
          end
        end)
        if not started then
          pending[buf] = nil
          require("arangodb.core").notify_error(handle, current.title)
        elseif not completed then
          active_request = handle
        end
      end,
      desc = opts.title or "Save ArangoDB JSON metadata",
    })
    vim.api.nvim_create_autocmd("BufWipeout", {
      buffer = buf,
      once = true,
      callback = function()
        if active_request and active_request.cancel then
          active_request.cancel()
        end
        active_request = nil
        pending[buf] = nil
        editors[buf] = nil
      end,
    })
    vim.b[buf].arangodb_json_editor_initialized = true
  end

  vim.cmd("buffer " .. buf)
  vim.cmd("normal! gg")
  return buf
end

return M
