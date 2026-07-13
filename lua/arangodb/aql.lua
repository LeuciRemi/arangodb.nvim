--- Interactive AQL editor, result buffers, pagination, and history UI.
local M = {}

local client = require("arangodb.client")
local core = require("arangodb.core")
local history = require("arangodb.aql_history")
local utils = require("arangodb.utils")

local session_sequence = 0
local sessions = {}
local setup_bind_actions

local function options()
  return require("arangodb.config").get().aql or {}
end

local function keymaps()
  return require("arangodb.config").get().aql_keymaps or {}
end

local function valid_buffer(buf)
  return type(buf) == "number" and vim.api.nvim_buf_is_valid(buf)
end

local function buffer_text(buf)
  return table.concat(vim.api.nvim_buf_get_lines(buf, 0, -1, false), "\n")
end

local function set_buffer_text(buf, text)
  local modifiable = vim.bo[buf].modifiable
  local readonly = vim.bo[buf].readonly
  vim.bo[buf].readonly = false
  vim.bo[buf].modifiable = true
  vim.api.nvim_buf_set_lines(buf, 0, -1, false, vim.split(text, "\n", { plain = true }))
  vim.bo[buf].modified = false
  vim.bo[buf].modifiable = modifiable
  vim.bo[buf].readonly = readonly
end

local function bind_vars_text(bind_vars)
  if type(bind_vars) == "table" and vim.tbl_isempty(bind_vars) then
    bind_vars = vim.empty_dict()
  end
  return utils.json_pretty(bind_vars or vim.empty_dict())
end

local function notify_error(err, title)
  core.notify_error(err, title or "ArangoDB AQL")
end

local function set_local_keymap(buf, mode, lhs, callback, desc)
  if type(lhs) ~= "string" or lhs == "" then
    return
  end
  vim.keymap.set(mode, lhs, callback, { buffer = buf, desc = desc, silent = true })
end

local function parse_bind_vars(session)
  local value = vim.deepcopy(session.bind_vars or {})
  if valid_buffer(session.bind_buf) then
    local text = vim.trim(buffer_text(session.bind_buf))
    if text == "" then
      text = "{}"
    end
    if text:sub(1, 1) ~= "{" then
      error("AQL bind variables must be a JSON object", 0)
    end
    local ok, decoded = pcall(vim.json.decode, text)
    if not ok or type(decoded) ~= "table" then
      error("Invalid AQL bind variables JSON", 0)
    end
    value = decoded
  end
  session.bind_vars = vim.deepcopy(value)
  return value
end

local function query_lines(buf, first, last)
  local lines = vim.api.nvim_buf_get_lines(buf, first - 1, last, false)
  return vim.trim(table.concat(lines, "\n"))
end

local function full_query(session)
  return vim.trim(buffer_text(session.query_buf))
end

local function visual_query(session)
  local anchor = vim.fn.getpos("v")
  local cursor = vim.api.nvim_win_get_cursor(0)
  local start_row, start_col = anchor[2], math.max(anchor[3] - 1, 0)
  local end_row, end_col = cursor[1], cursor[2]
  if start_row > end_row or (start_row == end_row and start_col > end_col) then
    start_row, end_row = end_row, start_row
    start_col, end_col = end_col, start_col
  end
  local mode = vim.fn.mode(1):sub(1, 1)
  if mode == "V" or mode == "\22" then
    return query_lines(session.query_buf, start_row, end_row)
  end
  local lines = vim.api.nvim_buf_get_text(session.query_buf, start_row - 1, start_col, end_row - 1, end_col + 1, {})
  return vim.trim(table.concat(lines, "\n"))
end

local function require_query(query)
  if type(query) ~= "string" or vim.trim(query) == "" then
    error("AQL query is empty", 0)
  end
  return vim.trim(query)
end

local function close_cursor(session)
  if session.cursor_id then
    client.close_cursor_async(session.config, session.cursor_id)
    session.cursor_id = nil
  end
end

local function cancel_request(session, notify)
  session.request_generation = (session.request_generation or 0) + 1
  if session.request and session.request.cancel then
    session.request.cancel()
  end
  session.request = nil
  if notify then
    vim.notify("AQL request cancelled", vim.log.levels.INFO, { title = "ArangoDB AQL" })
  end
end

local function reset_operation(session)
  cancel_request(session, false)
  close_cursor(session)
end

local function request(session, start, callback)
  cancel_request(session, false)
  local generation = session.request_generation
  local completed = false
  local ok, handle = pcall(start, function(err, data)
    completed = true
    if session.request_generation ~= generation or not valid_buffer(session.query_buf) then
      return
    end
    session.request = nil
    callback(err, data)
  end)
  if not ok then
    session.request = nil
    callback(handle)
    return
  end
  if not completed then
    session.request = handle
  end
end

local function result_direction()
  local split = options().result_split or "auto"
  if split == "auto" then
    return vim.o.columns >= 160 and "right" or "bottom"
  end
  return split
end

local function find_window(buf)
  local windows = valid_buffer(buf) and vim.fn.win_findbuf(buf) or {}
  return windows[1]
end

local function find_window_in_tab(buf, tabpage)
  if not valid_buffer(buf) then
    return nil
  end
  for _, win in ipairs(vim.api.nvim_tabpage_list_wins(tabpage)) do
    if vim.api.nvim_win_get_buf(win) == buf then
      return win
    end
  end
end

local function window_role(win)
  return vim.w[win].arangodb_aql_role
end

local function set_window_role(win, role)
  vim.w[win].arangodb_aql_role = role
end

local function find_query_window(tabpage, exclude)
  local fallback
  for _, win in ipairs(vim.api.nvim_tabpage_list_wins(tabpage)) do
    if win ~= exclude then
      if window_role(win) == "query" then
        return win
      end
      local buf = vim.api.nvim_win_get_buf(win)
      if not fallback and vim.b[buf].arangodb_aql and window_role(win) ~= "bind_vars" then
        fallback = win
      end
    end
  end
  return fallback
end

local function is_bind_window(win)
  local buf = vim.api.nvim_win_get_buf(win)
  return window_role(win) == "bind_vars" or vim.b[buf].arangodb_aql_bind_vars == true
end

local function find_bind_window(tabpage, exclude)
  for _, win in ipairs(vim.api.nvim_tabpage_list_wins(tabpage)) do
    if win ~= exclude and is_bind_window(win) then
      return win
    end
  end
end

local function render_page(session, index)
  local page = session.pages[index]
  if not page then
    return
  end
  session.page_index = index
  local envelope = {
    database = session.config.database,
    mode = session.mode,
    page = index,
    count = session.total_count,
    hasMore = page.hasMore == true,
    cached = page.cached,
    result = page.result or {},
    extra = page.extra,
  }
  M.render_result(session, envelope)
end

local function next_page(session)
  if session.request then
    vim.notify("An AQL request is already in progress", vim.log.levels.INFO, { title = "ArangoDB AQL" })
    return
  end
  if session.page_index < #session.pages then
    render_page(session, session.page_index + 1)
    return
  end
  local current = session.pages[session.page_index]
  if not current or current.hasMore ~= true then
    vim.notify("No next AQL result page", vim.log.levels.INFO)
    return
  end
  if not session.cursor_id then
    notify_error("ArangoDB returned an incomplete AQL cursor response")
    return
  end
  request(session, function(done)
    return client.next_aql_page_async(session.config, session.cursor_id, done)
  end, function(err, data)
    if err then
      notify_error(err)
      return
    end
    if data.hasMore == true and (type(data.id) ~= "string" or data.id == "") and not session.cursor_id then
      notify_error("ArangoDB returned an incomplete AQL cursor response")
      return
    end
    session.cursor_id = data.hasMore == true and (data.id or session.cursor_id) or nil
    session.pages[#session.pages + 1] = data
    render_page(session, #session.pages)
  end)
end

local function prev_page(session)
  if session.page_index <= 1 then
    vim.notify("No previous AQL result page", vim.log.levels.INFO)
    return
  end
  render_page(session, session.page_index - 1)
end

local function setup_result_actions(session, buf)
  vim.api.nvim_buf_create_user_command(buf, "ArangoAqlNextPage", function()
    next_page(session)
  end, { desc = "Show the next AQL result page" })
  vim.api.nvim_buf_create_user_command(buf, "ArangoAqlPrevPage", function()
    prev_page(session)
  end, { desc = "Show the previous AQL result page" })
  local maps = keymaps()
  set_local_keymap(buf, "n", maps.next_page, function()
    next_page(session)
  end, "Next AQL Result Page")
  set_local_keymap(buf, "n", maps.prev_page, function()
    prev_page(session)
  end, "Previous AQL Result Page")
end

local function ensure_result_buffer(session)
  if valid_buffer(session.result_buf) then
    return session.result_buf
  end
  local buf = vim.api.nvim_create_buf(false, true)
  session.result_buf = buf
  pcall(
    vim.api.nvim_buf_set_name,
    buf,
    string.format("arangodb-aql-result://%s/%d", session.config.database, session.id)
  )
  vim.bo[buf].buftype = "nofile"
  vim.bo[buf].bufhidden = "wipe"
  vim.bo[buf].swapfile = false
  vim.bo[buf].filetype = "json"
  vim.bo[buf].modifiable = false
  vim.bo[buf].readonly = true
  vim.b[buf].arangodb_aql_query_buf = session.query_buf
  setup_result_actions(session, buf)
  vim.api.nvim_create_autocmd("BufWipeout", {
    buffer = buf,
    once = true,
    callback = function()
      if session.result_buf == buf then
        session.result_buf = nil
        cancel_request(session, false)
        close_cursor(session)
      end
    end,
  })
  return buf
end

--- Render a JSON value in the adaptive, read-only result split.
function M.render_result(session, value)
  local buf = ensure_result_buffer(session)
  set_buffer_text(buf, utils.json_pretty(value))
  local result_win = find_window(buf)
  if not result_win then
    local query_win = find_window(session.query_buf) or vim.api.nvim_get_current_win()
    vim.api.nvim_set_current_win(query_win)
    if result_direction() == "right" then
      vim.cmd("belowright vsplit")
    else
      vim.cmd("belowright split")
    end
    result_win = vim.api.nvim_get_current_win()
    vim.api.nvim_win_set_buf(result_win, buf)
    if vim.api.nvim_win_is_valid(query_win) then
      vim.api.nvim_set_current_win(query_win)
    end
  end
end

local function history_entry(session, query, bind_vars)
  local ok, err = pcall(history.add, {
    connection = session.connection,
    database = session.config.database,
    query = query,
    bind_vars = bind_vars,
  })
  if not ok then
    vim.notify(tostring(err), vim.log.levels.WARN, { title = "ArangoDB AQL History" })
  end
end

local function write_collections(plan)
  local names = {}
  for _, collection in ipairs((plan or {}).collections or {}) do
    if type(collection) == "table" and collection.type ~= "read" and collection.name then
      names[#names + 1] = collection.name
    end
  end
  table.sort(names)
  return names
end

local function execute_after_explain(session, query, bind_vars, profile, explanation)
  local plan = explanation.plan or {}
  local modification = plan.isModificationQuery == true
  if modification then
    local collections = write_collections(plan)
    local target = #collections > 0 and ("\nCollections: " .. table.concat(collections, ", ")) or ""
    local answer = vim.fn.confirm(
      string.format("Execute a data-modifying AQL query in %s?%s", session.config.database, target),
      "&Execute\n&Cancel",
      2
    )
    if answer ~= 1 then
      return
    end
  end

  history_entry(session, query, bind_vars)
  session.mode = profile and "profile" or "execute"
  request(session, function(done)
    local opts = options()
    return client.execute_aql_async(session.config, query, bind_vars, {
      batch_size = opts.batch_size,
      cursor_ttl = opts.cursor_ttl,
      max_runtime = opts.max_runtime,
      profile = profile,
      modification = modification,
    }, done)
  end, function(err, data)
    if err then
      notify_error(err)
      return
    end
    if data.hasMore == true and (type(data.id) ~= "string" or data.id == "") then
      notify_error("ArangoDB returned an incomplete AQL cursor response")
      return
    end
    session.pages = { data }
    session.page_index = 1
    session.total_count = data.count
    session.cursor_id = data.hasMore == true and data.id or nil
    render_page(session, 1)
  end)
end

local function execute(session, query, profile)
  local ok, bind_vars = pcall(parse_bind_vars, session)
  if not ok then
    notify_error(bind_vars)
    return
  end
  local query_ok, normalized = pcall(require_query, query)
  if not query_ok then
    notify_error(normalized)
    return
  end
  reset_operation(session)
  request(session, function(done)
    return client.explain_aql_async(session.config, normalized, bind_vars, done)
  end, function(err, explanation)
    if err then
      notify_error(err)
      return
    end
    execute_after_explain(session, normalized, bind_vars, profile, explanation)
  end)
end

local function inspect_query(session, mode, query)
  local query_ok, normalized = pcall(require_query, query)
  if not query_ok then
    notify_error(normalized)
    return
  end
  local bind_vars = {}
  if mode == "explain" then
    local ok, value = pcall(parse_bind_vars, session)
    if not ok then
      notify_error(value)
      return
    end
    bind_vars = value
  end
  reset_operation(session)
  request(session, function(done)
    if mode == "validate" then
      return client.validate_aql_async(session.config, normalized, done)
    end
    return client.explain_aql_async(session.config, normalized, bind_vars, done)
  end, function(err, data)
    if err then
      notify_error(err)
      return
    end
    session.mode = mode
    M.render_result(session, {
      database = session.config.database,
      mode = mode,
      response = data,
    })
  end)
end

local function ensure_bind_buffer(session)
  if valid_buffer(session.bind_buf) then
    return session.bind_buf
  end
  local buf = vim.api.nvim_create_buf(false, true)
  session.bind_buf = buf
  pcall(
    vim.api.nvim_buf_set_name,
    buf,
    string.format("arangodb-aql-bindvars://%s/%d", session.config.database, session.id)
  )
  vim.bo[buf].buftype = "acwrite"
  vim.bo[buf].bufhidden = "hide"
  vim.bo[buf].swapfile = false
  vim.bo[buf].filetype = "json"
  vim.bo[buf].modifiable = true
  vim.b[buf].arangodb_aql_query_buf = session.query_buf
  vim.b[buf].arangodb_aql_bind_vars = true
  set_buffer_text(buf, bind_vars_text(session.bind_vars))
  vim.api.nvim_create_autocmd("BufWriteCmd", {
    buffer = buf,
    callback = function()
      local ok, value = pcall(parse_bind_vars, session)
      if not ok then
        notify_error(value, "ArangoDB AQL Bind Variables")
        return
      end
      session.bind_vars = value
      vim.bo[buf].modified = false
      vim.notify("AQL bind variables updated", vim.log.levels.INFO)
    end,
  })
  vim.api.nvim_create_autocmd("BufWipeout", {
    buffer = buf,
    once = true,
    callback = function()
      if session.bind_buf == buf then
        local ok, value = pcall(parse_bind_vars, session)
        if ok then
          session.bind_vars = value
        end
        session.bind_buf = nil
      end
    end,
  })
  setup_bind_actions(session, buf)
  return buf
end

local function show_bind_vars(session, focus)
  local buf = ensure_bind_buffer(session)
  local tabpage = vim.api.nvim_get_current_tabpage()
  local query_win = find_window_in_tab(session.query_buf, tabpage)
  if not query_win then
    return
  end
  local original_win = vim.api.nvim_get_current_win()
  local relocated_query = false
  if window_role(query_win) == "bind_vars" then
    local main_win = find_query_window(tabpage, query_win)
    if main_win then
      vim.api.nvim_win_set_buf(main_win, session.query_buf)
      query_win = main_win
      relocated_query = true
    else
      set_window_role(query_win, "query")
    end
  end
  set_window_role(query_win, "query")
  local bind_win = find_window_in_tab(buf, tabpage) or find_bind_window(tabpage, query_win)
  if bind_win then
    if vim.api.nvim_win_get_buf(bind_win) ~= buf then
      vim.api.nvim_win_set_buf(bind_win, buf)
    end
  else
    vim.api.nvim_set_current_win(query_win)
    vim.cmd("belowright 10split")
    bind_win = vim.api.nvim_get_current_win()
    vim.api.nvim_win_set_buf(bind_win, buf)
  end
  set_window_role(bind_win, "bind_vars")
  for _, win in ipairs(vim.api.nvim_tabpage_list_wins(tabpage)) do
    if win ~= bind_win and win ~= query_win and is_bind_window(win) then
      pcall(vim.api.nvim_win_close, win, true)
    end
  end
  if focus then
    vim.api.nvim_set_current_win(bind_win)
  elseif relocated_query then
    vim.api.nvim_set_current_win(query_win)
  elseif vim.api.nvim_win_is_valid(original_win) and vim.api.nvim_win_get_tabpage(original_win) == tabpage then
    vim.api.nvim_set_current_win(original_win)
  else
    vim.api.nvim_set_current_win(query_win)
  end
end

local function open_bind_vars(session)
  show_bind_vars(session, true)
end

local function restore_history(session, entry)
  set_buffer_text(session.query_buf, entry.query)
  session.bind_vars = vim.deepcopy(entry.bind_vars or {})
  if valid_buffer(session.bind_buf) then
    set_buffer_text(session.bind_buf, bind_vars_text(session.bind_vars))
  end
  local win = find_window(session.query_buf)
  if win then
    vim.api.nvim_set_current_win(win)
  end
end

local function open_history(session)
  local entries, warning = history.load()
  if warning then
    vim.notify(warning, vim.log.levels.WARN, { title = "ArangoDB AQL History" })
  end
  if #entries == 0 then
    vim.notify("AQL history is empty", vim.log.levels.INFO)
    return
  end
  local ok, snacks = pcall(require, "snacks")
  if not ok then
    notify_error("`folke/snacks.nvim` is required to browse AQL history")
    return
  end
  local items = {}
  for _, entry in ipairs(entries) do
    local first_line = vim.split(entry.query, "\n", { plain = true })[1] or ""
    items[#items + 1] = {
      text = string.format("%s  %s  %s", entry.timestamp or "", entry.database or "?", first_line),
      preview = table.concat({
        "Connection: " .. tostring(entry.connection or "?"),
        "Database: " .. tostring(entry.database or "?"),
        "Timestamp: " .. tostring(entry.timestamp or "?"),
        "",
        entry.query,
        "",
        "Bind variables",
        bind_vars_text(entry.bind_vars),
      }, "\n"),
      entry = entry,
    }
  end
  snacks.picker({
    title = "ArangoDB AQL History",
    finder = function()
      return function(cb)
        for _, item in ipairs(items) do
          cb(item)
        end
      end
    end,
    format = "text",
    preview = "preview",
    confirm = function(picker, item)
      local selected = item and (item.item or item)
      if selected and selected.entry then
        picker:close()
        restore_history(session, selected.entry)
      end
    end,
  })
end

local function command_query(session, command_opts)
  return query_lines(session.query_buf, command_opts.line1, command_opts.line2)
end

setup_bind_actions = function(session, buf)
  local function add_command(name, callback, desc)
    vim.api.nvim_buf_create_user_command(buf, name, callback, { desc = desc })
  end
  add_command("ArangoAqlExecute", function()
    execute(session, full_query(session), false)
  end, "Execute the associated AQL query")
  add_command("ArangoAqlValidate", function()
    inspect_query(session, "validate", full_query(session))
  end, "Validate the associated AQL query")
  add_command("ArangoAqlExplain", function()
    inspect_query(session, "explain", full_query(session))
  end, "Explain the associated AQL query")
  add_command("ArangoAqlProfile", function()
    execute(session, full_query(session), true)
  end, "Profile the associated AQL query")
  add_command("ArangoAqlBindVars", function()
    open_bind_vars(session)
  end, "Edit AQL bind variables")
  add_command("ArangoAqlHistory", function()
    open_history(session)
  end, "Browse AQL history")
  add_command("ArangoAqlCancel", function()
    cancel_request(session, session.request ~= nil)
    close_cursor(session)
  end, "Cancel the active AQL operation")

  local maps = keymaps()
  set_local_keymap(buf, "n", maps.execute, function()
    execute(session, full_query(session), false)
  end, "Execute AQL Query")
  set_local_keymap(buf, "n", maps.validate, function()
    inspect_query(session, "validate", full_query(session))
  end, "Validate AQL Query")
  set_local_keymap(buf, "n", maps.explain, function()
    inspect_query(session, "explain", full_query(session))
  end, "Explain AQL Query")
  set_local_keymap(buf, "n", maps.profile, function()
    execute(session, full_query(session), true)
  end, "Profile AQL Query")
  set_local_keymap(buf, "n", maps.bind_vars, function()
    open_bind_vars(session)
  end, "Edit AQL Bind Variables")
  set_local_keymap(buf, "n", maps.history, function()
    open_history(session)
  end, "Browse AQL History")
  set_local_keymap(buf, "n", maps.cancel, function()
    cancel_request(session, session.request ~= nil)
    close_cursor(session)
  end, "Cancel AQL Operation")
end

local function setup_query_actions(session)
  local buf = session.query_buf
  local function add_query_command(name, action, desc)
    vim.api.nvim_buf_create_user_command(buf, name, function(command_opts)
      action(command_query(session, command_opts))
    end, { range = "%", desc = desc })
  end
  add_query_command("ArangoAqlExecute", function(query)
    execute(session, query, false)
  end, "Execute the current AQL query")
  add_query_command("ArangoAqlValidate", function(query)
    inspect_query(session, "validate", query)
  end, "Validate the current AQL query")
  add_query_command("ArangoAqlExplain", function(query)
    inspect_query(session, "explain", query)
  end, "Explain the current AQL query")
  add_query_command("ArangoAqlProfile", function(query)
    execute(session, query, true)
  end, "Profile the current AQL query")
  vim.api.nvim_buf_create_user_command(buf, "ArangoAqlBindVars", function()
    open_bind_vars(session)
  end, { desc = "Edit AQL bind variables" })
  vim.api.nvim_buf_create_user_command(buf, "ArangoAqlHistory", function()
    open_history(session)
  end, { desc = "Browse AQL history" })
  vim.api.nvim_buf_create_user_command(buf, "ArangoAqlCancel", function()
    cancel_request(session, session.request ~= nil)
    close_cursor(session)
  end, { desc = "Cancel the active AQL operation" })

  local maps = keymaps()
  set_local_keymap(buf, "n", maps.execute, function()
    execute(session, full_query(session), false)
  end, "Execute AQL Query")
  set_local_keymap(buf, "x", maps.execute, function()
    execute(session, visual_query(session), false)
  end, "Execute Selected AQL")
  set_local_keymap(buf, { "n", "x" }, maps.validate, function()
    inspect_query(session, "validate", vim.fn.mode(1):sub(1, 1) == "n" and full_query(session) or visual_query(session))
  end, "Validate AQL Query")
  set_local_keymap(buf, { "n", "x" }, maps.explain, function()
    inspect_query(session, "explain", vim.fn.mode(1):sub(1, 1) == "n" and full_query(session) or visual_query(session))
  end, "Explain AQL Query")
  set_local_keymap(buf, { "n", "x" }, maps.profile, function()
    execute(session, vim.fn.mode(1):sub(1, 1) == "n" and full_query(session) or visual_query(session), true)
  end, "Profile AQL Query")
  set_local_keymap(buf, "n", maps.bind_vars, function()
    open_bind_vars(session)
  end, "Edit AQL Bind Variables")
  set_local_keymap(buf, "n", maps.history, function()
    open_history(session)
  end, "Browse AQL History")
  set_local_keymap(buf, "n", maps.cancel, function()
    cancel_request(session, session.request ~= nil)
    close_cursor(session)
  end, "Cancel AQL Operation")
end

local function cleanup(session)
  cancel_request(session, false)
  close_cursor(session)
  sessions[session.query_buf] = nil
  for _, buf in ipairs({ session.bind_buf, session.result_buf }) do
    if valid_buffer(buf) then
      pcall(vim.api.nvim_buf_delete, buf, { force = true })
    end
  end
end

local function current_tab_has_aql_session()
  for _, win in ipairs(vim.api.nvim_tabpage_list_wins(0)) do
    local buf = vim.api.nvim_win_get_buf(win)
    if sessions[buf] or vim.b[buf].arangodb_aql_query_buf then
      return true
    end
  end
  return false
end

local function create_session(config, connection, opts)
  local open_in_new_tab = current_tab_has_aql_session()
  session_sequence = session_sequence + 1
  local buf = vim.api.nvim_create_buf(true, false)
  local session = {
    id = session_sequence,
    config = vim.deepcopy(config),
    connection = connection or config.database,
    query_buf = buf,
    bind_vars = vim.deepcopy(opts.bind_vars or {}),
    pages = {},
    page_index = 0,
    request_generation = 0,
  }
  sessions[buf] = session
  pcall(vim.api.nvim_buf_set_name, buf, string.format("arangodb-aql://%s/%d.aql", config.database, session.id))
  vim.bo[buf].buftype = "nofile"
  vim.bo[buf].bufhidden = "hide"
  vim.bo[buf].swapfile = false
  vim.bo[buf].buflisted = true
  vim.bo[buf].filetype = "aql"
  vim.bo[buf].modifiable = true
  vim.b[buf].arangodb_aql = true
  vim.b[buf].arangodb_aql_database = config.database
  vim.b[buf].arangodb_aql_connection = session.connection
  set_buffer_text(buf, opts.query or "")
  setup_query_actions(session)
  vim.api.nvim_create_autocmd("BufWipeout", {
    buffer = buf,
    once = true,
    callback = function()
      cleanup(session)
    end,
  })
  if open_in_new_tab then
    vim.cmd("tab sbuffer " .. buf)
  else
    vim.api.nvim_set_current_buf(buf)
  end
  session.tabpage = vim.api.nvim_get_current_tabpage()
  set_window_role(vim.api.nvim_get_current_win(), "query")
  open_bind_vars(session)
  local query_win = find_window(buf)
  if query_win then
    vim.api.nvim_set_current_win(query_win)
  end
  vim.api.nvim_create_autocmd("BufEnter", {
    buffer = buf,
    callback = function()
      show_bind_vars(session, false)
    end,
  })
  return session
end

local function open_item(item, opts)
  local config = core.parse_connection(item.url)
  if not config then
    notify_error("Invalid ArangoDB connection URL: " .. tostring(item.url))
    return
  end
  return create_session(config, item.name, opts)
end

--- Open a new AQL editor session, selecting a database when needed.
function M.open(opts)
  opts = opts or {}
  if type(opts.config) == "table" then
    return create_session(opts.config, opts.connection or opts.config.database, opts)
  end
  if type(opts.database) == "string" and opts.database ~= "" then
    local item = core.find_database(opts.database)
      or {
        name = opts.database,
        url = core.arango_url(opts.database),
      }
    return open_item(item, opts)
  end
  local items = core.available_databases()
  if #items == 0 then
    notify_error("No ArangoDB database configured")
    return
  end
  vim.ui.select(items, {
    prompt = "Arango database for AQL",
    format_item = function(item)
      return item.name
    end,
  }, function(item)
    if item then
      open_item(item, opts)
    end
  end)
end

--- Return an editor session for tests and internal integrations.
function M.session(buf)
  return sessions[buf]
end

return M
