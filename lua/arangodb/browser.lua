--- Picker and document-buffer UI for browsing ArangoDB from Neovim.
local M = {}

local arango = require("arangodb.core")
local collection_admin = require("arangodb.browser.collection_admin")
local browser_ui = require("arangodb.browser.ui")
local client = require("arangodb.client")
local errors = require("arangodb.errors")
local utils = require("arangodb.utils")

-- Keep track of the active picker and of the back stack shared by picker views.
local state = {
  picker = nil,
  history = {},
  current_view = nil,
  view_meta = nil,
}

local ns = vim.api.nvim_create_namespace("arangodb.nvim")
local browse_collection
local browse_collections
local go_back
local open_new_document
local open_duplicate_document
local refresh_collection_document_buffers

local function plugin_options()
  return require("arangodb.config").get()
end

local function picker_options()
  return plugin_options().picker_keymaps or {}
end

local picker_layout = browser_ui.picker_layout
local restore_picker_backdrop = browser_ui.restore_backdrop
local watch_picker_backdrop = browser_ui.watch_backdrop
local picker_select_options = browser_ui.select_options
local picker_key = browser_ui.picker_key
local merge_keymaps = browser_ui.merge_keymaps
local key_label = browser_ui.key_label
local action_label = browser_ui.action_label
local hint_text = browser_ui.hint_text
local get_snacks = browser_ui.get_snacks

local function hrtime()
  local uv = vim.uv or vim.loop
  if uv and uv.hrtime then
    return uv.hrtime()
  end
  return math.floor(vim.fn.reltimefloat(vim.fn.reltime()) * 1000000000)
end

local function generate_uuid()
  local seed = table.concat({
    tostring(hrtime()),
    tostring(vim.fn.localtime()),
    tostring(vim.fn.getpid()),
    tostring({}),
    tostring(math.random()),
  }, ":")
  local hash = vim.fn.sha256(seed)
  local variant = ({ "8", "9", "a", "b" })[(tonumber(hash:sub(17, 17), 16) % 4) + 1]
  return string.format(
    "%s-%s-4%s-%s%s-%s",
    hash:sub(1, 8),
    hash:sub(9, 12),
    hash:sub(13, 15),
    variant,
    hash:sub(18, 20),
    hash:sub(21, 32)
  )
end

--- Start a cancellable client operation and normalize synchronous setup failures.
local function start_async(title, starter, on_success, on_error)
  local completed = false
  local ok, handle = pcall(starter, function(err, value)
    completed = true
    if err then
      if on_error then
        on_error(err)
      else
        arango.notify_error(err, title)
      end
      return
    end
    if on_success then
      on_success(value)
    end
  end)
  if not ok then
    if on_error then
      on_error(handle)
    else
      arango.notify_error(handle, title)
    end
    return nil
  end
  return completed and nil or handle
end

local function cancel_picker_request(picker)
  local handle = picker and picker._arangodb_request or nil
  if handle and handle.cancel then
    handle.cancel()
  end
  if picker then
    picker._arangodb_request = nil
  end
end

local function picker_request(picker, title, starter, on_success, on_error)
  if picker and picker._arangodb_request then
    vim.notify("An ArangoDB operation is already in progress", vim.log.levels.INFO)
    return
  end
  local handle
  handle = start_async(title, starter, function(value)
    if picker and picker._arangodb_request == handle then
      picker._arangodb_request = nil
    end
    if on_success then
      on_success(value)
    end
  end, function(err)
    if picker and picker._arangodb_request == handle then
      picker._arangodb_request = nil
    end
    if on_error then
      on_error(err)
    else
      arango.notify_error(err, title)
    end
  end)
  if picker and handle then
    picker._arangodb_request = handle
  end
  return handle
end

--- Suspend a Snacks finder coroutine until a cancellable client request completes.
local function await_picker_request(ctx, start)
  local request_error
  local result
  local completed = false
  local handle

  handle = start(function(err, value)
    if completed or ctx.async:aborted() then
      return
    end
    completed = true
    request_error = err
    result = value
    ctx.async:resume()
  end)

  ctx.async:on("abort", function()
    if completed then
      return
    end
    completed = true
    if handle and handle.cancel then
      handle.cancel()
    end
  end)
  ctx.async:suspend()
  return request_error, result
end

local close_picker = browser_ui.close_picker

local function close_picker_then(picker, callback)
  close_picker(picker)
  vim.schedule(callback)
end

local function refresh_picker(picker, opts)
  browser_ui.refresh_picker(picker or state.picker, opts)
end

--- Save the current picker route so :ArangoBack can restore it later.
local function push_history(route)
  if type(route) ~= "table" then
    return
  end
  state.history[#state.history + 1] = vim.deepcopy(route)
end

--- Restore the previous picker route from the navigation stack.
local function pop_history()
  if #state.history == 0 then
    return nil
  end
  local route = state.history[#state.history]
  state.history[#state.history] = nil
  return route
end

local function clear_history()
  state.history = {}
end

local execute_picker_action = browser_ui.execute_action
local restore_picker_input_focus = browser_ui.restore_input_focus
local set_picker_search = browser_ui.set_search

local function field_label(field)
  if type(field) == "table" then
    return table.concat(field, ", ")
  end
  return field or "_key"
end

local function title(database, collection, field, meta)
  local first = #meta.items > 0 and (meta.offset + 1) or 0
  local last = meta.offset + #meta.items
  local keymaps = picker_options()
  local hint = hint_text({
    key_label(keymaps.execute) and (keymaps.execute .. " actions") or nil,
    key_label(keymaps.prev_page)
        and key_label(keymaps.next_page)
        and string.format("%s/%s pages", keymaps.prev_page, keymaps.next_page)
      or nil,
    #state.history > 0 and key_label(keymaps.back) and (keymaps.back .. " back") or nil,
  })
  local field_text = field_label(field)
  if meta.total_count ~= nil and (meta.search == nil or meta.search == "") then
    return string.format(
      "Arango %s/%s - %s (%d-%d/%d)%s%s",
      database,
      collection,
      field_text,
      first,
      last,
      meta.total_count,
      meta.has_more and "+" or "",
      hint
    )
  end

  return string.format(
    "Arango %s/%s - %s (%d-%d)%s%s",
    database,
    collection,
    field_text,
    first,
    last,
    meta.has_more and "+" or "",
    hint
  )
end

local prompt_select = browser_ui.prompt_select
local prompt_input = browser_ui.prompt_input

local function collection_picker_title(database, search, allow_database_back)
  local keymaps = picker_options()
  local hint = hint_text({
    key_label(keymaps.execute) and (keymaps.execute .. " actions") or nil,
    allow_database_back and key_label(keymaps.back) and (keymaps.back .. " back") or nil,
  })
  if search and search ~= "" then
    return string.format("Arango %s collections - %s%s", database, search, hint)
  end
  return string.format("Arango %s collections%s", database, hint)
end

local format_count = browser_ui.format_count
local format_bytes = browser_ui.format_bytes
local format_flag = browser_ui.format_flag
local preview_line = browser_ui.preview_line

local function collection_preview_text(config, collection, meta)
  local overview = meta.overview or {}
  local details = meta.collection_lookup and meta.collection_lookup[collection] or nil
  local lines = {
    "Database",
    preview_line("Name", overview.name or config.database),
    preview_line("Endpoint", overview.endpoint or string.format("%s:%s", tostring(config.host), tostring(config.port))),
    preview_line("Collections", format_count(overview.collection_count or meta.collection_count)),
    preview_line("Documents", format_count(overview.total_documents)),
    preview_line("Approx. size", format_bytes(overview.total_size)),
    preview_line("Path", overview.path),
    preview_line("Sharding", overview.sharding),
    preview_line("Repl. factor", overview.replication_factor),
    preview_line("Write concern", overview.write_concern),
    "",
    "Selected collection",
    preview_line("Name", collection),
    preview_line("Type", details and details.type or nil),
    preview_line("Status", details and details.status or nil),
    preview_line("Documents", details and format_count(details.count) or "unavailable"),
    preview_line("Approx. size", details and format_bytes(details.size) or "unavailable"),
    preview_line("WaitForSync", details and format_flag(details.wait_for_sync) or nil),
    preview_line("Cache", details and format_flag(details.cache_enabled) or nil),
    preview_line("Engine", details and details.engine or nil),
    preview_line("Id", details and details.id or nil),
  }

  local result = {}
  for _, line in ipairs(lines) do
    if line ~= nil then
      result[#result + 1] = line
    end
  end

  return table.concat(result, "\n")
end

local function collection_route(config, collection, field, search)
  return {
    kind = "collection",
    config = config,
    collection = collection,
    field = field or "_key",
    search = search or "",
  }
end

local function collections_route(config, opts, search)
  opts = opts or {}
  return {
    kind = "collections",
    config = config,
    search = search or "",
    document_field = opts.document_field or "_key",
    document_search = opts.document_search or "",
    allow_database_back = opts.allow_database_back == true,
  }
end

local function draft_document_id(collection, key)
  return string.format("%s/%s", collection, key)
end

local function draft_document_payload(collection, key)
  return {
    _id = draft_document_id(collection, key),
    _key = key,
    _rev = vim.NIL,
  }
end

local function draft_document_preview(collection, key)
  local document_id = draft_document_id(collection, key)
  return table.concat({
    "{",
    string.format('  "_id": %s,', vim.json.encode(document_id)),
    string.format('  "_key": %s,', vim.json.encode(key)),
    '  "_rev": null',
    "}",
  }, "\n")
end

local function duplicated_document_payload(collection, document)
  if type(document) ~= "table" then
    error("Document payload must be a JSON object")
  end

  local target_collection = collection
  if type(target_collection) ~= "string" or vim.trim(target_collection) == "" then
    target_collection = type(document._id) == "string" and document._id:match("^([^/]+)/") or nil
  end
  target_collection = target_collection and vim.trim(target_collection) or ""
  if target_collection == "" then
    error("Missing collection name")
  end

  local key = generate_uuid()
  local payload = vim.deepcopy(document)
  payload._key = key
  payload._id = draft_document_id(target_collection, key)
  payload._rev = vim.NIL

  return payload, key, target_collection
end

local function picker_current_item(current, item)
  if item and item.item then
    return item
  end
  if not current then
    return nil
  end

  local ok, selected = pcall(function()
    return current:current()
  end)
  if ok then
    return selected
  end
end

local function arangodb_document_buffers(opts)
  opts = opts or {}
  local buffers = {}

  for _, buf in ipairs(vim.api.nvim_list_bufs()) do
    local is_draft = vim.b[buf].arangodb_document_is_new == true
    local has_document = vim.b[buf].arangodb_document_id ~= nil
      or (opts.include_drafts ~= false and is_draft and vim.b[buf].arangodb_document_collection ~= nil)
    if vim.api.nvim_buf_is_valid(buf) and has_document then
      local matches = true
      if opts.database and vim.b[buf].arangodb_database ~= opts.database then
        matches = false
      end
      if opts.collection and vim.b[buf].arangodb_document_collection ~= opts.collection then
        matches = false
      end
      if opts.id and vim.b[buf].arangodb_document_id ~= opts.id then
        matches = false
      end
      if opts.include_drafts == false and is_draft then
        matches = false
      end

      if matches then
        buffers[#buffers + 1] = buf
      end
    end
  end

  return buffers
end

local function ensure_unmodified_document_buffers(opts, action)
  local modified = {}

  for _, buf in ipairs(arangodb_document_buffers(opts)) do
    if vim.bo[buf].modified then
      modified[#modified + 1] = vim.b[buf].arangodb_document_id or vim.api.nvim_buf_get_name(buf) or ("buffer " .. buf)
    end
  end

  if #modified == 0 then
    return true
  end

  vim.notify(
    string.format("Save or close modified Arango buffers before %s:\n%s", action, table.concat(modified, "\n")),
    vim.log.levels.WARN,
    { title = "ArangoDB" }
  )
  return false
end

local function close_document_buffers(opts)
  for _, buf in ipairs(arangodb_document_buffers(opts)) do
    pcall(vim.api.nvim_buf_delete, buf, { force = true })
  end
end

local function confirm_delete_document(config, document_id)
  return vim.fn.confirm(string.format("Delete document %s/%s?", config.database, document_id), "&Delete\n&Cancel", 2)
    == 1
end

local function confirm_truncate_collection(config, collection, callback, picker)
  if
    vim.fn.confirm(
      string.format("Permanently delete every document in %s/%s?\nThis cannot be undone.", config.database, collection),
      "&Truncate\n&Cancel",
      2
    ) ~= 1
  then
    restore_picker_input_focus(picker)
    return
  end

  if callback then
    callback()
  end
  restore_picker_input_focus(picker)
end

local function choose_database(callback)
  local items = arango.available_databases()
  if #items == 0 then
    arango.notify_error("No ArangoDB connections found")
    return
  end

  prompt_select(items, {
    prompt = "Arango database",
    format_item = function(item)
      return item.name
    end,
  }, callback)
end

local function prompt_collection_type(picker, callback)
  vim.ui.select(
    { "document", "edge" },
    picker_select_options({
      prompt = "Collection type",
    }),
    function(choice)
      if choice then
        callback(choice)
      end
      restore_picker_input_focus(picker)
    end
  )
end

local function create_collection_with_prompt(config, picker, callback)
  vim.ui.input({
    prompt = string.format("Create collection in %s: ", config.database),
  }, function(value)
    local collection = value and vim.trim(value) or ""
    if value == nil or collection == "" then
      restore_picker_input_focus(picker)
      return
    end

    prompt_collection_type(picker, function(collection_type)
      picker_request(picker, "ArangoDB Create Collection", function(done)
        return client.create_collection_async(config, collection, collection_type, done)
      end, function(result)
        if callback then
          callback(result, collection, collection_type)
        end
      end)
    end)
  end)
end

local function rename_collection_with_prompt(config, collection, callback, picker)
  if
    not ensure_unmodified_document_buffers({
      database = config.database,
      collection = collection,
    }, "renaming this collection")
  then
    restore_picker_input_focus(picker)
    return
  end

  close_picker_then(picker, function()
    vim.ui.input({
      prompt = string.format("Rename collection %s to: ", collection),
      default = collection,
    }, function(value)
      local new_name = value and vim.trim(value) or ""
      if value == nil or new_name == "" or new_name == collection then
        restore_picker_input_focus(picker)
        return
      end

      local previous = collection
      picker_request(picker, "ArangoDB Rename Collection", function(done)
        return client.rename_collection_async(config, previous, new_name, done)
      end, function(result)
        local final_name = result.name or new_name
        refresh_collection_document_buffers(config, previous, final_name)
        if callback then
          callback(result, final_name, previous)
        end
        restore_picker_input_focus(picker)
      end, function(err)
        arango.notify_error(err, "ArangoDB Rename Collection")
        restore_picker_input_focus(picker)
      end)
    end)
  end)
end

local function truncate_collection_with_prompt(config, collection, callback, picker)
  if
    not ensure_unmodified_document_buffers({
      database = config.database,
      collection = collection,
    }, "truncating this collection")
  then
    restore_picker_input_focus(picker)
    return
  end

  confirm_truncate_collection(config, collection, function()
    picker_request(picker, "ArangoDB Truncate Collection", function(done)
      return client.truncate_collection_async(config, collection, done)
    end, function(result)
      close_document_buffers({
        database = config.database,
        collection = collection,
        include_drafts = false,
      })
      if callback then
        callback(result)
      end
    end, function(err)
      arango.notify_error(err, "ArangoDB Truncate Collection")
      restore_picker_input_focus(picker)
    end)
  end, picker)
end

local function duplicate_collection_with_prompt(config, collection, callback, picker)
  if
    not ensure_unmodified_document_buffers({
      database = config.database,
      collection = collection,
    }, "duplicating this collection")
  then
    restore_picker_input_focus(picker)
    return
  end

  vim.ui.input({
    prompt = string.format("Duplicate collection %s to: ", collection),
    default = collection .. "_clone",
  }, function(value)
    local new_name = value and vim.trim(value) or ""
    if value == nil or new_name == "" or new_name == collection then
      restore_picker_input_focus(picker)
      return
    end

    picker_request(picker, "ArangoDB Duplicate Collection", function(done)
      return client.duplicate_collection_async(config, collection, new_name, done)
    end, function(result)
      if callback then
        callback(result, result.name or new_name)
      end
      restore_picker_input_focus(picker)
    end, function(err)
      arango.notify_error(err, "ArangoDB Duplicate Collection")
      restore_picker_input_focus(picker)
    end)
  end)
end

local function choose_field(config, collection, picker, callback)
  picker_request(picker, "ArangoDB Fields", function(done)
    return client.list_fields_async(config, collection, plugin_options().field_sample_size, done)
  end, function(fields)
    if #fields == 0 then
      fields = { "_key" }
    end
    vim.ui.select(
      fields,
      picker_select_options({
        prompt = string.format("Filter field (%s/%s)", config.database, collection),
      }),
      function(choice)
        if choice then
          callback(choice)
        end
        restore_picker_input_focus(picker)
      end
    )
  end)
end

local function open_related_selector(config, relations, on_choice)
  if #relations == 0 then
    vim.notify("No related values found in document", vim.log.levels.INFO)
    return
  end

  prompt_select(relations, {
    prompt = "Open related document",
    format_item = function(item)
      return item.label
    end,
  }, function(choice)
    if choice then
      on_choice(choice)
    end
  end)
end

local function document_buffer_name(doc)
  return string.format("arangodb-buffer://%s/%s", doc.database, doc.id)
end

local function set_buffer_json(buf, text)
  local win = vim.fn.bufwinid(buf)
  local view
  if win ~= -1 and vim.api.nvim_win_is_valid(win) then
    view = vim.api.nvim_win_call(win, vim.fn.winsaveview)
  end
  vim.bo[buf].modifiable = true
  vim.api.nvim_buf_set_lines(buf, 0, -1, false, vim.split(text, "\n", { plain = true }))
  vim.bo[buf].modified = false
  if view then
    pcall(vim.api.nvim_win_call, win, function()
      vim.fn.winrestview(view)
    end)
  end
end

local function get_current_document_payload(buf)
  local lines = vim.api.nvim_buf_get_lines(buf, 0, -1, false)
  local text = table.concat(lines, "\n")
  local ok, decoded = pcall(vim.json.decode, text)
  if not ok or type(decoded) ~= "table" then
    error("Current buffer does not contain valid JSON")
  end
  return decoded
end

refresh_collection_document_buffers = function(config, old_collection, new_collection)
  for _, buf in
    ipairs(arangodb_document_buffers({
      database = config.database,
      collection = old_collection,
    }))
  do
    if vim.b[buf].arangodb_document_is_new == true then
      local ok, payload = pcall(get_current_document_payload, buf)
      local key = ok and vim.trim(payload._key or "") or nil
      if key and key ~= "" then
        M.open_document(config, {
          database = config.database,
          id = draft_document_id(new_collection, key),
          key = key,
          collection = new_collection,
          document = draft_document_payload(new_collection, key),
          preview = draft_document_preview(new_collection, key),
          buf = buf,
          show = false,
          is_new = true,
        })
      end
    else
      local document_id = vim.b[buf].arangodb_document_id
      local key = type(document_id) == "string" and document_id:match("^[^/]+/(.+)$") or nil
      if key then
        start_async("ArangoDB Rename Collection", function(done)
          return client.get_document_async(config, new_collection .. "/" .. key, done)
        end, function(payload)
          if vim.api.nvim_buf_is_valid(buf) then
            M.open_document(
              config,
              vim.tbl_extend("force", payload, {
                database = config.database,
                buf = buf,
                show = false,
              })
            )
          end
        end)
      end
    end
  end
end

local function try_decode_preview(item)
  local ok, decoded = pcall(vim.json.decode, item.preview)
  if ok and type(decoded) == "table" then
    return decoded
  end
  return item.document or {}
end

local function scalar_text(value)
  local value_type = type(value)
  if value_type ~= "string" and value_type ~= "number" and value_type ~= "boolean" then
    return nil
  end

  local text = tostring(value)
  if text == "" then
    return nil
  end
  return text
end

local function parse_related_id(value)
  local text = scalar_text(value)
  if not text then
    return nil
  end

  local collection, key = text:match("^([^/]+)/(.+)$")
  if not collection or not key then
    return nil
  end
  return text, collection, key
end

local function path_tail(path)
  if path == nil or path == "" then
    return nil
  end
  local segments = utils.field_path_segments(path)
  return segments[#segments]
end

local function collection_aliases(name)
  local aliases = {}

  local function add(alias)
    if alias and alias ~= "" and not aliases[alias] then
      aliases[#aliases + 1] = alias
      aliases[alias] = true
    end
  end

  add(name)
  if name:sub(-3) == "ies" then
    add(name:sub(1, -4) .. "y")
  end
  if name:sub(-1) == "s" then
    add(name:sub(1, -2))
  else
    add(name .. "s")
  end

  return aliases
end

local function collection_lookup(collections)
  local lookup = {}
  for _, collection in ipairs(collections or {}) do
    lookup[collection] = collection
  end
  return lookup
end

local function resolve_collection_name(name, collections)
  if not name or name == "" then
    return nil
  end

  local lookup = type(collections) == "table" and collections.lookup or nil
  if not lookup then
    return nil
  end

  for _, candidate in ipairs(collection_aliases(name)) do
    if lookup[candidate] then
      return lookup[candidate]
    end
  end

  return nil
end

local function foreign_key_info(field_name)
  local base = field_name:match("^(.-)_ids$")
  if base and base ~= "" then
    return { base = base, multiple = true }
  end

  base = field_name:match("^(.-)_keys$")
  if base and base ~= "" then
    return { base = base, multiple = true }
  end

  base = field_name:match("^(.-)_id$")
  if base and base ~= "" then
    return { base = base, multiple = false }
  end

  base = field_name:match("^(.-)_key$")
  if base and base ~= "" then
    return { base = base, multiple = false }
  end

  return nil
end

local function reverse_relation_fields(collection_name)
  if not collection_name or collection_name == "" then
    return {}
  end

  local fields = {}
  local seen = {}
  local function add(field)
    if field and field ~= "" and not seen[field] then
      seen[field] = true
      fields[#fields + 1] = field
    end
  end

  for _, alias in ipairs(collection_aliases(collection_name)) do
    add(alias .. "_id")
    add(alias .. "_ids")
    add(alias .. "_key")
    add(alias .. "_keys")
  end

  return fields
end

local function related_search_values(relation)
  local values = {}
  local seen = {}

  local function add(value)
    local text = scalar_text(value)
    if not text or seen[text] then
      return
    end
    seen[text] = true
    values[#values + 1] = text
  end

  if type(relation.values) == "table" and utils.is_list(relation.values) then
    for _, value in ipairs(relation.values) do
      add(value)
    end
  else
    add(relation.value)
  end

  return values
end

--- Scan a document recursively and infer outgoing relations from ids and keys.
local function related_values(document, collections)
  local values = {}
  local entries = {}
  local resolved_collections = {
    lookup = collection_lookup(collections),
  }

  local function source_depth(source)
    local ok, segments = pcall(utils.field_path_segments, tostring(source or ""))
    return ok and (#segments - 1) or 0
  end

  local function format_sources(sources)
    local items = vim.deepcopy(sources or {})
    table.sort(items, function(a, b)
      local depth_a = source_depth(a)
      local depth_b = source_depth(b)
      if depth_a ~= depth_b then
        return depth_a < depth_b
      end
      return a < b
    end)
    return table.concat(items, ", ")
  end

  local function add(source, relation)
    local relation_id = relation.id
    local relation_collection = relation.collection
    local relation_key = relation.key

    if relation_id then
      local _, parsed_collection, parsed_key = parse_related_id(relation_id)
      relation_collection = relation_collection or parsed_collection
      relation_key = relation_key or parsed_key
    elseif relation_collection and relation_key then
      relation_id = relation_collection .. "/" .. relation_key
    end

    local display = relation_id or relation_key
    if not display or display == "" then
      return
    end

    local key = (relation_collection or "") .. "\0" .. display
    local entry = entries[key]
    if entry then
      if source and source ~= "" and not entry.source_lookup[source] then
        entry.source_lookup[source] = true
        entry.sources[#entry.sources + 1] = source
        entry.label = string.format("%s (%s)", display, format_sources(entry.sources))
      end
      return entry
    end

    local label = display
    local sources = {}
    local source_lookup = {}
    if source and source ~= "" and source ~= display then
      sources[1] = source
      source_lookup[source] = true
      label = string.format("%s (%s)", display, source)
    end

    entry = {
      label = label,
      field = relation.field or "_key",
      value = relation_key or display,
      values = relation.values,
      id = relation_id,
      collection = relation_collection,
      sources = sources,
      source_lookup = source_lookup,
    }
    entries[key] = entry
    values[#values + 1] = entry
    return entry
  end

  local function add_field_relation(source, value, inferred_collection)
    local relation_id = parse_related_id(value)
    if relation_id then
      add(source, { id = relation_id })
      return true
    end

    local key_text = scalar_text(value)
    if key_text and inferred_collection then
      add(source, {
        collection = inferred_collection,
        key = key_text,
      })
      return true
    end

    return false
  end

  local function add_field_relations(scope, field_name, value)
    local info = foreign_key_info(field_name)
    if not info then
      return false
    end

    local segment = utils.escape_field_segment(field_name)
    local source = scope ~= "" and (scope .. "." .. segment) or segment
    local inferred_collection = resolve_collection_name(info.base, resolved_collections)

    if info.multiple then
      if type(value) == "table" and utils.is_list(value) then
        for _, item in ipairs(value) do
          add_field_relation(source, item, inferred_collection)
        end
        return true
      end
      return add_field_relation(source, value, inferred_collection)
    end

    return add_field_relation(source, value, inferred_collection)
  end

  local function add_relation_node(scope, node)
    if scope == "" or type(node) ~= "table" or utils.is_list(node) then
      return false
    end

    local source = scope
    local scope_collection = resolve_collection_name(path_tail(scope), resolved_collections)
    local added = false

    local document_id, parsed_collection, document_key = parse_related_id(node._id)
    if document_id then
      add(source, {
        id = document_id,
        collection = parsed_collection,
        key = document_key,
      })
      added = true
    elseif add_field_relation(source, node._id, scope_collection) then
      added = true
    end

    local key_text = scalar_text(node._key)
    if key_text and key_text ~= document_key and scope_collection then
      add(source, {
        collection = scope_collection,
        key = key_text,
      })
      added = true
    end

    local ids_source = source
    local ids = node._ids
    if type(ids) == "table" and utils.is_list(ids) then
      for _, item in ipairs(ids) do
        if add_field_relation(ids_source, item, scope_collection) then
          added = true
        end
      end
    end

    local keys = node._keys
    if type(keys) == "table" and utils.is_list(keys) then
      for _, item in ipairs(keys) do
        if add_field_relation(ids_source, item, scope_collection) then
          added = true
        end
      end
    end

    return added
  end

  local function walk(scope, value)
    if type(value) ~= "table" then
      return
    end

    local scope_collection = scope ~= "" and resolve_collection_name(path_tail(scope), resolved_collections) or nil

    if utils.is_list(value) then
      if scope_collection then
        for _, item in ipairs(value) do
          if type(item) == "table" and not utils.is_list(item) then
            add_relation_node(scope, item)
          else
            add_field_relation(scope, item, scope_collection)
          end
        end
        return
      end

      for _, item in ipairs(value) do
        if type(item) == "table" then
          walk(scope, item)
        end
      end
      return
    end

    if add_relation_node(scope, value) then
      return
    end

    if scope_collection then
      return
    end

    for key, nested in pairs(value) do
      if not add_field_relations(scope, key, nested) and type(nested) == "table" then
        local segment = utils.escape_field_segment(key)
        local path = scope ~= "" and (scope .. "." .. segment) or segment
        walk(path, nested)
      end
    end
  end

  if type(document) ~= "table" then
    return values
  end

  walk("", document)
  return values
end

--- Infer outgoing and reverse relations without blocking Neovim.
local function document_relations_async(config, document, callback)
  local handles = {}
  local cancelled = false
  local finished = false
  local task = {}

  local function track(handle)
    if handle then
      handles[#handles + 1] = handle
    end
    return handle
  end

  function task.cancel()
    if cancelled then
      return
    end
    cancelled = true
    for _, handle in ipairs(handles) do
      if handle.cancel then
        handle.cancel()
      end
    end
  end

  local function complete(err, values)
    if cancelled or finished then
      return
    end
    finished = true
    callback(err, values)
  end

  track(client.list_collections_async(config, function(err, collections)
    if err then
      complete(err)
      return
    end

    local values = related_values(document, collections)
    if type(document) ~= "table" then
      complete(nil, values)
      return
    end
    local document_id, document_collection, document_key = parse_related_id(document._id)
    if not document_collection or not document_key then
      complete(nil, values)
      return
    end

    local candidate_lookup = {}
    for _, field_name in ipairs(reverse_relation_fields(document_collection)) do
      candidate_lookup[field_name] = true
    end
    if vim.tbl_isempty(candidate_lookup) then
      complete(nil, values)
      return
    end

    local relation_values = { document_key }
    if document_id then
      relation_values[#relation_values + 1] = document_id
    end

    local pending = 0
    local function complete_one()
      pending = pending - 1
      if pending == 0 then
        table.sort(values, function(left, right)
          return tostring(left.label) < tostring(right.label)
        end)
        complete(nil, values)
      end
    end

    for _, collection_name in ipairs(collections) do
      if collection_name ~= document_collection then
        pending = pending + 1
        track(
          client.list_fields_async(
            config,
            collection_name,
            plugin_options().field_sample_size,
            function(field_err, fields)
              if cancelled or finished then
                return
              end
              if field_err then
                arango.notify_error(field_err, "ArangoDB Relations")
                complete_one()
                return
              end
              local matched_fields = {}
              for _, field_path in ipairs(fields) do
                local tail = path_tail(field_path)
                if tail and candidate_lookup[tail] then
                  matched_fields[#matched_fields + 1] = field_path
                end
              end
              if #matched_fields == 0 then
                complete_one()
                return
              end
              track(
                client.search_related_async(
                  config,
                  matched_fields,
                  relation_values,
                  2,
                  collection_name,
                  function(search_err, result)
                    if search_err then
                      arango.notify_error(search_err, "ArangoDB Relations")
                    elseif result and result.matches and #result.matches > 0 then
                      local display = result.matches[1].id or collection_name
                      local fields_label = table.concat(matched_fields, ", ")
                      values[#values + 1] = {
                        label = string.format("%s (%s)", display, fields_label),
                        prompt = fields_label,
                        field = matched_fields,
                        values = relation_values,
                        value = document_key,
                        collection = collection_name,
                        id = #result.matches == 1 and result.matches[1].id or nil,
                      }
                    end
                    complete_one()
                  end
                )
              )
            end
          )
        )
      end
    end

    if pending == 0 then
      complete(nil, values)
    end
  end))

  return task
end

local function relation_prompt_label(relation)
  if type(relation.prompt) == "string" and relation.prompt ~= "" then
    return relation.prompt
  end
  if type(relation.field) == "table" then
    return table.concat(relation.field, ", ")
  end
  return relation.field or relation.label or "relation"
end

local function direct_relation_key(relation)
  if relation.id and type(relation.id) == "string" then
    return relation.id:match("^[^/]+/(.+)$")
  end
  if relation.collection and relation.value and type(relation.field) ~= "table" then
    return relation.value
  end
end

local function relation_browse_route(config, relation)
  local key = direct_relation_key(relation)
  if key then
    return {
      kind = "collection",
      config = config,
      collection = relation.collection,
      field = "_key",
      search = key,
    }
  end

  return {
    kind = "related",
    config = config,
    collection = relation.collection,
    field = relation.field,
    values = related_search_values(relation),
    search = "",
    prompt = relation_prompt_label(relation),
    title = relation.label,
  }
end

local function related_route_title(route)
  local value = route.prompt or route.title
  if type(value) == "string" and value ~= "" then
    return value
  end
  if type(route.field) == "table" then
    return table.concat(route.field, ", ")
  end
  return route.field or "related"
end

local function open_route(route, prev_picker)
  if not route or type(route) ~= "table" then
    return
  end

  if route.kind == "collections" then
    browse_collections(route.config, {
      search = route.search or "",
      document_field = route.document_field or "_key",
      document_search = route.document_search or "",
      allow_database_back = route.allow_database_back == true,
    }, prev_picker)
    return
  end

  if route.kind == "document" then
    start_async("ArangoDB", function(done)
      return client.get_document_async(route.config, route.id, done)
    end, function(payload)
      close_picker_then(prev_picker, function()
        M.open_document(route.config, payload)
      end)
    end)
    return
  end

  if route.kind == "related" then
    browse_collection(route.config, route.collection, route.field, route.search or "", {
      kind = "related",
      title = string.format("Arango %s/%s - %s", route.config.database, route.collection, related_route_title(route)),
      offset = route.offset,
      values = route.values,
      prompt = route.prompt,
    }, prev_picker)
    return
  end

  browse_collection(route.config, route.collection, route.field or "_key", route.search or "", {
    kind = route.kind or "collection",
    title = route.title,
    offset = route.offset,
  }, prev_picker)
end

--- Reopen the previous ArangoDB route from the shared history stack.
go_back = function(current)
  local route = pop_history()
  if not route then
    vim.notify("No previous ArangoDB view", vim.log.levels.INFO)
    return
  end

  open_route(route, current)
end

--- Global entry point for :ArangoBack.
function M.back()
  go_back(state.picker)
end

local function jump_to_related(config, relation, current, context)
  local route = relation_browse_route(config, relation)
  if not route.collection then
    vim.notify("No related collection found", vim.log.levels.WARN)
    return
  end

  if context then
    push_history(context)
  end

  open_route(route, current)
end

local function open_conflict_diff(buf, local_payload, remote_payload)
  local local_text = utils.json_pretty(local_payload)
  local remote_text = utils.json_pretty(remote_payload)
  local diff = vim.diff(remote_text, local_text, {
    result_type = "unified",
    ctxlen = 3,
  })
  diff = "--- remote\n+++ local\n" .. diff
  vim.cmd("vnew")
  local diff_buf = vim.api.nvim_get_current_buf()
  vim.api.nvim_buf_set_lines(diff_buf, 0, -1, false, vim.split(diff, "\n", { plain = true }))
  vim.bo[diff_buf].buftype = "nofile"
  vim.bo[diff_buf].bufhidden = "wipe"
  vim.bo[diff_buf].swapfile = false
  vim.bo[diff_buf].modifiable = false
  vim.bo[diff_buf].filetype = "diff"
  pcall(vim.api.nvim_buf_set_name, diff_buf, "arangodb-conflict://" .. (vim.b[buf].arangodb_document_id or "document"))
end

--- Attach buffer-local commands, keymaps, and :write integration to a document.
local function document_actions(config, buf)
  if vim.b[buf].arangodb_actions_initialized then
    return
  end

  local active_request

  local function buffer_request(title, starter, on_success, on_error)
    if active_request then
      vim.notify("An ArangoDB document operation is already in progress", vim.log.levels.INFO)
      return
    end
    local handle
    handle = start_async(title, starter, function(value)
      if active_request == handle then
        active_request = nil
      end
      if vim.api.nvim_buf_is_valid(buf) and on_success then
        on_success(value)
      end
    end, function(err)
      if active_request == handle then
        active_request = nil
      end
      if on_error then
        on_error(err)
      else
        arango.notify_error(err, title)
      end
    end)
    active_request = handle
    return handle
  end

  local function apply_saved_result(result, is_new)
    M.open_document(config, vim.tbl_extend("force", result, { database = config.database, buf = buf }))
    refresh_picker()
    vim.notify(is_new and "Document created" or "Document saved", vim.log.levels.INFO)
  end

  local function save_existing_document(payload, force)
    buffer_request("ArangoDB Save", function(done)
      return client.save_document_async(
        config,
        vim.b[buf].arangodb_document_id,
        payload,
        force and { force = true } or nil,
        done
      )
    end, function(result)
      apply_saved_result(result, false)
    end, function(result)
      if not errors.is(result, "conflict") then
        arango.notify_error(result, "ArangoDB Save")
        return
      end
      vim.ui.select(
        { "Reload remote version", "Compare versions", "Force overwrite" },
        picker_select_options({
          prompt = "Document changed on the server",
        }),
        function(choice)
          if not choice then
            return
          end
          if choice == "Force overwrite" then
            save_existing_document(payload, true)
            return
          end
          buffer_request("ArangoDB Conflict", function(done)
            return client.get_document_async(config, vim.b[buf].arangodb_document_id, done)
          end, function(remote)
            if choice == "Reload remote version" then
              M.open_document(config, vim.tbl_extend("force", remote, { database = config.database, buf = buf }))
              vim.notify("Remote document reloaded", vim.log.levels.INFO)
            else
              open_conflict_diff(buf, payload, remote.document or remote)
            end
          end)
        end
      )
    end)
  end

  local function save_document()
    local ok, payload = pcall(get_current_document_payload, buf)
    if not ok then
      arango.notify_error(payload, "ArangoDB Save")
      return
    end

    local is_new = vim.b[buf].arangodb_document_is_new == true
    local action = is_new and "ArangoDB Create" or "ArangoDB Save"
    if is_new then
      buffer_request(action, function(done)
        return client.create_document_async(config, vim.b[buf].arangodb_document_collection, payload, done)
      end, function(result)
        apply_saved_result(result, true)
      end)
    else
      save_existing_document(payload, false)
    end
  end

  local function open_related_picker()
    if vim.b[buf].arangodb_document_is_new == true then
      vim.notify("Save the draft document before browsing related documents", vim.log.levels.INFO)
      return
    end

    local ok, payload = pcall(get_current_document_payload, buf)
    if not ok then
      arango.notify_error(payload, "ArangoDB Relations")
      return
    end

    buffer_request("ArangoDB Relations", function(done)
      return document_relations_async(config, payload, done)
    end, function(relations)
      open_related_selector(config, relations, function(choice)
        push_history({
          kind = "document",
          config = config,
          id = vim.b[buf].arangodb_document_id,
        })
        jump_to_related(config, choice, nil, nil)
      end)
    end)
  end

  local function duplicate_document()
    local ok, payload = pcall(get_current_document_payload, buf)
    if not ok then
      arango.notify_error(payload, "ArangoDB Duplicate")
      return
    end

    local collection = vim.b[buf].arangodb_document_collection
    if not collection then
      arango.notify_error("Missing current collection", "ArangoDB Duplicate")
      return
    end

    local document_id = vim.b[buf].arangodb_document_id
    if vim.b[buf].arangodb_document_is_new ~= true and document_id then
      push_history({
        kind = "document",
        config = config,
        id = document_id,
      })
    end

    open_duplicate_document(config, collection, payload)
  end

  local function delete_document()
    if vim.b[buf].arangodb_document_is_new == true then
      if vim.fn.confirm("Discard this draft document?", "&Discard\n&Cancel", 2) ~= 1 then
        return
      end
      pcall(vim.api.nvim_buf_delete, buf, { force = true })
      vim.notify("Draft document discarded", vim.log.levels.INFO)
      return
    end

    local document_id = vim.b[buf].arangodb_document_id
    if not document_id then
      arango.notify_error("Missing current document id", "ArangoDB Delete")
      return
    end

    if
      not ensure_unmodified_document_buffers({
        database = config.database,
        id = document_id,
        include_drafts = false,
      }, "deleting this document")
    then
      return
    end

    if not confirm_delete_document(config, document_id) then
      return
    end

    buffer_request("ArangoDB Delete", function(done)
      return client.delete_document_async(config, document_id, done)
    end, function()
      close_document_buffers({ database = config.database, id = document_id, include_drafts = false })
      refresh_picker()
      vim.notify("Document deleted", vim.log.levels.INFO)
    end)
  end

  local function explore_graph()
    if vim.b[buf].arangodb_document_is_new == true then
      vim.notify("Save the draft document before exploring a graph", vim.log.levels.INFO)
      return
    end
    require("arangodb.graph").open({
      config = config,
      start = vim.b[buf].arangodb_document_id,
    })
  end

  local keymaps = plugin_options().document_keymaps or {}
  if keymaps.save then
    vim.keymap.set("n", keymaps.save, save_document, { buffer = buf, desc = "Save Arango document" })
  end
  if keymaps.delete then
    vim.keymap.set("n", keymaps.delete, delete_document, { buffer = buf, desc = "Delete Arango document" })
  end
  if keymaps.duplicate then
    vim.keymap.set("n", keymaps.duplicate, duplicate_document, { buffer = buf, desc = "Duplicate Arango document" })
  end
  if keymaps.related then
    vim.keymap.set("n", keymaps.related, open_related_picker, { buffer = buf, desc = "Open related Arango document" })
  end
  if keymaps.graph then
    vim.keymap.set("n", keymaps.graph, explore_graph, { buffer = buf, desc = "Explore Arango graph" })
  end

  vim.api.nvim_buf_create_user_command(
    buf,
    "ArangoDocumentSave",
    save_document,
    { desc = "Save current Arango document" }
  )
  vim.api.nvim_buf_create_user_command(buf, "ArangoDocumentDelete", delete_document, {
    desc = "Delete current Arango document",
  })
  vim.api.nvim_buf_create_user_command(buf, "ArangoDocumentDuplicate", duplicate_document, {
    desc = "Duplicate current Arango document",
  })
  vim.api.nvim_buf_create_user_command(buf, "ArangoDocumentRelated", open_related_picker, {
    desc = "Open related Arango document",
  })
  vim.api.nvim_buf_create_user_command(buf, "ArangoDocumentGraph", explore_graph, {
    desc = "Explore named graphs from this Arango document",
  })
  vim.api.nvim_buf_create_user_command(buf, "ArangoBack", function()
    go_back(nil)
  end, {
    desc = "Return to previous ArangoDB view",
  })
  vim.api.nvim_create_autocmd("BufWriteCmd", {
    buffer = buf,
    callback = save_document,
    desc = "Save current Arango document on :write",
  })
  vim.api.nvim_create_autocmd("BufWipeout", {
    buffer = buf,
    once = true,
    callback = function()
      if active_request and active_request.cancel then
        active_request.cancel()
      end
      active_request = nil
    end,
  })

  vim.b[buf].arangodb_actions_initialized = true
end

--- Open an ArangoDB document inside a regular JSON buffer.
function M.open_document(config, doc)
  doc = doc or {}
  local document_id = doc.id or doc._id
  local collection = doc.collection or (type(document_id) == "string" and document_id:match("^([^/]+)/")) or nil
  local key = doc.key or (type(document_id) == "string" and document_id:match("^[^/]+/(.+)$")) or nil
  local database = doc.database or config.database
  local is_new = doc.is_new == true
  local display_id = document_id or (collection and key and draft_document_id(collection, key)) or collection or "draft"

  local buf
  local target = doc.buf
  if target and vim.api.nvim_buf_is_valid(target) then
    buf = target
  else
    buf = vim.fn.bufadd(document_buffer_name({
      database = database,
      id = display_id,
    }))
  end

  local preview = doc.preview
  if not preview and type(doc.document) == "table" then
    preview = vim.json.encode(doc.document)
  end
  preview = preview or "{}"

  vim.fn.bufload(buf)
  pcall(
    vim.api.nvim_buf_set_name,
    buf,
    document_buffer_name({
      database = database,
      id = display_id,
    })
  )
  set_buffer_json(buf, preview)

  vim.bo[buf].filetype = "json"
  vim.bo[buf].buftype = "acwrite"
  vim.bo[buf].buflisted = true
  vim.bo[buf].bufhidden = "hide"
  vim.bo[buf].swapfile = false
  vim.bo[buf].modifiable = true
  vim.bo[buf].modified = false

  vim.b[buf].arangodb_config = config
  vim.b[buf].arangodb_document = doc.document
  vim.b[buf].arangodb_document_id = display_id
  vim.b[buf].arangodb_document_collection = collection
  vim.b[buf].arangodb_database = database
  vim.b[buf].arangodb_document_is_new = is_new

  vim.api.nvim_buf_clear_namespace(buf, ns, 0, -1)
  vim.api.nvim_buf_set_extmark(buf, ns, 0, 0, {
    virt_text = {
      {
        string.format(" ArangoDB%s %s/%s ", is_new and " draft" or "", vim.b[buf].arangodb_database, display_id),
        "Title",
      },
      {
        is_new and "  :ArangoDocumentSave  :ArangoDocumentDuplicate  :ArangoDocumentDelete"
          or "  :ArangoDocumentSave  :ArangoDocumentDuplicate  :ArangoDocumentDelete  :ArangoDocumentRelated  :ArangoDocumentGraph",
        "Comment",
      },
    },
    virt_text_pos = "right_align",
  })

  if doc.show ~= false then
    vim.cmd("buffer " .. buf)
  end
  document_actions(config, buf)
  if doc.show ~= false then
    vim.cmd("normal! gg")
  end
end

open_new_document = function(config, collection, opts)
  local key = generate_uuid()
  local payload = draft_document_payload(collection, key)
  M.open_document(
    config,
    vim.tbl_extend("force", {
      database = config.database,
      id = payload._id,
      key = key,
      collection = collection,
      document = payload,
      preview = draft_document_preview(collection, key),
      is_new = true,
    }, opts or {})
  )
end

open_duplicate_document = function(config, collection, document, opts)
  local payload, key, target_collection = duplicated_document_payload(collection, document)
  M.open_document(
    config,
    vim.tbl_extend("force", {
      database = config.database,
      id = payload._id,
      key = key,
      collection = target_collection,
      document = payload,
      preview = utils.json_pretty(payload),
      is_new = true,
    }, opts or {})
  )
end

local function item_text(item)
  return string.format("%s  %s", item.key or "?", item.field_value_text or "")
end

local function update_picker_title(picker, meta)
  picker.title = title(meta.database, meta.collection, meta.field, meta)
  picker:update_titles()
end

local function update_collection_picker_title(picker, config, search, allow_database_back)
  picker.title = collection_picker_title(config.database, search or "", allow_database_back)
  picker:update_titles()
end

--- Open the collections picker for the selected database.
browse_collections = function(config, opts, prev_picker)
  local snacks = get_snacks()
  if not snacks then
    return
  end

  opts = opts or {}
  local meta = {
    search = opts.search or "",
    collection_count = 0,
    collections = {},
    overview = {
      name = config.database,
      endpoint = string.format("%s:%s", tostring(config.host), tostring(config.port)),
    },
    collection_lookup = {},
    overview_loaded = false,
    overview_loading = false,
    overview_waiters = {},
  }
  local preview_request
  local overview_request

  local function current_search(current)
    if current and current.input and current.input.filter and type(current.input.filter.search) == "string" then
      return current.input.filter.search
    end
    return meta.search or ""
  end

  local function clear_collection_overview()
    if preview_request and preview_request.cancel then
      preview_request.cancel()
    end
    if overview_request and overview_request.cancel then
      overview_request.cancel()
    end
    preview_request = nil
    overview_request = nil
    meta.collections = {}
    meta.collection_lookup = {}
    meta.overview = {
      name = config.database,
      endpoint = string.format("%s:%s", tostring(config.host), tostring(config.port)),
    }
    meta.overview_loaded = false
    meta.overview_loading = false
    meta.overview_waiters = {}
  end

  local function collection_items(collections, search)
    meta.collection_count = #collections
    meta.overview.collection_count = #collections
    meta.collections = collections
    local previous_lookup = meta.collection_lookup
    meta.collection_lookup = {}
    for _, item in ipairs(collections) do
      meta.collection_lookup[item.name] = vim.tbl_extend("force", {}, item, previous_lookup[item.name] or {})
    end

    local query = string.lower(vim.trim(search or ""))
    meta.search = search or ""

    local items = {}
    for _, detail in ipairs(collections) do
      local collection = detail.name
      if query == "" or string.lower(collection):find(query, 1, true) ~= nil then
        items[#items + 1] = {
          text = collection,
          item = {
            name = collection,
            database = config.database,
          },
          preview = {
            text = collection_preview_text(config, collection, meta),
            ft = "text",
            loc = false,
          },
        }
      end
    end
    return items
  end

  local function ensure_database_overview(render)
    if meta.overview_loaded then
      return
    end

    meta.overview_waiters[#meta.overview_waiters + 1] = render
    if meta.overview_loading then
      return
    end

    meta.overview_loading = true
    overview_request = client.database_overview_async(config, {
      collections = meta.collections,
      include_figures = true,
    }, function(err, overview)
      meta.overview_loading = false
      overview_request = nil
      if not err and overview then
        meta.overview = overview
        meta.collection_count = overview.collection_count or meta.collection_count
        for _, details in ipairs(overview.collections or {}) do
          meta.collection_lookup[details.name] =
            vim.tbl_extend("force", meta.collection_lookup[details.name] or { name = details.name }, details)
        end
        meta.overview_loaded = true
      end

      local waiters = meta.overview_waiters
      meta.overview_waiters = {}
      for _, waiter in ipairs(waiters) do
        waiter()
      end
    end)
  end

  local function preview_collection(ctx)
    local collection = ctx.item and ctx.item.item and ctx.item.item.name
    if not collection then
      return
    end
    if preview_request and preview_request.cancel then
      preview_request.cancel()
    end

    local function render()
      if not ctx.preview.item or not ctx.preview.item.item or ctx.preview.item.item.name ~= collection then
        return
      end
      ctx.preview:reset()
      ctx.preview:set_lines(vim.split(collection_preview_text(config, collection, meta), "\n", { plain = true }))
    end
    render()
    ensure_database_overview(render)

    preview_request = client.collection_metrics_async(config, collection, function(err, metrics)
      if err or not ctx.preview.item or not ctx.preview.item.item or ctx.preview.item.item.name ~= collection then
        return
      end
      meta.collection_lookup[collection] =
        vim.tbl_extend("force", meta.collection_lookup[collection] or { name = collection }, metrics or {})
      render()
    end)
  end

  local function selected_collection(current, item)
    local selected = picker_current_item(current, item)
    if not selected or not selected.item then
      return nil
    end
    return selected.item.name
  end

  local function open_collection(current, item)
    local collection = selected_collection(current, item)
    if not collection then
      return
    end

    push_history(collections_route(config, opts, current_search(current)))
    browse_collection(config, collection, opts.document_field or "_key", opts.document_search or "", {}, current)
  end

  local function create_document(current, item)
    local collection = selected_collection(current, item)
    if not collection then
      vim.notify("Select a collection first", vim.log.levels.INFO)
      return
    end

    push_history(collections_route(config, opts, current_search(current)))
    push_history(collection_route(config, collection, opts.document_field or "_key", opts.document_search or ""))
    close_picker_then(current, function()
      open_new_document(config, collection)
    end)
  end

  local function open_action_menu(current, item)
    local collection = selected_collection(current, item)
    local choices = {}
    local keymaps = picker_options()

    if collection then
      choices[#choices + 1] = { label = "Open collection (Enter)", action = "arango_open_collection" }
      choices[#choices + 1] = {
        label = action_label("Duplicate collection", keymaps.duplicate_collection),
        action = "arango_duplicate_collection",
      }
      choices[#choices + 1] =
        { label = action_label("Rename collection", keymaps.rename), action = "arango_rename_collection" }
      choices[#choices + 1] =
        { label = action_label("Truncate collection", keymaps.truncate), action = "arango_truncate_collection" }
      choices[#choices + 1] = { label = "Manage indexes", action = "arango_manage_indexes" }
      choices[#choices + 1] = {
        label = "Edit properties and schema",
        action = "arango_edit_collection_properties",
      }
    end
    choices[#choices + 1] = {
      label = action_label("Create collection", keymaps.create_collection),
      action = "arango_create_collection",
    }
    if opts.allow_database_back then
      choices[#choices + 1] = { label = action_label("Choose database", keymaps.back), action = "arango_pick_database" }
    end

    vim.schedule(function()
      vim.ui.select(
        choices,
        picker_select_options({
          prompt = string.format("Collection actions (%s)", config.database),
          format_item = function(choice)
            return choice.label
          end,
        }),
        function(choice)
          if not choice or not current or current.closed then
            return
          end
          execute_picker_action(current, choice.action)
        end
      )
    end)
  end

  local function pick_database(current)
    if not opts.allow_database_back then
      vim.notify("Database picker is not available in this view", vim.log.levels.INFO)
      return
    end

    close_picker_then(current, function()
      M.open({
        field = opts.document_field or "_key",
        search = opts.document_search or "",
      })
    end)
  end

  local picker
  local keymaps = picker_options()
  picker = snacks.picker({
    title = collection_picker_title(config.database, meta.search, opts.allow_database_back == true),
    search = meta.search,
    find = false,
    live = true,
    supports_live = true,
    show_empty = true,
    auto_close = false,
    focus = "input",
    layout = picker_layout,
    finder = function(_, ctx)
      local search = ctx.filter.search or ""
      return function(cb)
        local err, collections = await_picker_request(ctx, function(done)
          return client.list_collection_details_async(config, done)
        end)
        if err then
          vim.schedule(function()
            arango.notify_error(err, "ArangoDB")
          end)
          return
        end
        for _, item in ipairs(collection_items(collections or {}, search)) do
          cb(item)
        end
      end
    end,
    format = "text",
    preview = preview_collection,
    confirm = function(current, item)
      open_collection(current, item)
    end,
    on_show = function(current)
      state.picker = current
      restore_picker_backdrop(current)
      watch_picker_backdrop(current)
      update_collection_picker_title(current, config, current_search(current), opts.allow_database_back == true)
      if prev_picker and not prev_picker.closed then
        prev_picker:close()
      end
    end,
    on_change = function(current)
      state.picker = current
      update_collection_picker_title(current, config, current_search(current), opts.allow_database_back == true)
    end,
    on_close = function()
      cancel_picker_request(picker)
      if preview_request and preview_request.cancel then
        preview_request.cancel()
      end
      if overview_request and overview_request.cancel then
        overview_request.cancel()
      end
      if state.picker == picker then
        state.picker = nil
      end
    end,
    actions = {
      arango_open_collection = function(current, item)
        open_collection(current, item)
      end,
      arango_create_document = function(current, item)
        create_document(current, item)
      end,
      arango_create_collection = function(current)
        create_collection_with_prompt(config, current, function(result, collection)
          clear_collection_overview()
          set_picker_search(current, result.name or collection)
          refresh_picker(current)
          vim.notify(string.format("Collection %s created", result.name or collection), vim.log.levels.INFO)
        end)
      end,
      arango_duplicate_collection = function(current, item)
        local collection = selected_collection(current, item)
        if not collection then
          vim.notify("Select a collection first", vim.log.levels.INFO)
          return
        end

        duplicate_collection_with_prompt(config, collection, function(result, new_name)
          clear_collection_overview()
          set_picker_search(current, new_name)
          refresh_picker(current)
          vim.notify(
            string.format(
              "Collection %s duplicated to %s (%d documents, %d indexes)",
              collection,
              new_name,
              result.copied_count or 0,
              result.copied_indexes or 0
            ),
            vim.log.levels.INFO
          )
        end, current)
      end,
      arango_rename_collection = function(current, item)
        local collection = selected_collection(current, item)
        if not collection then
          vim.notify("Select a collection first", vim.log.levels.INFO)
          return
        end

        rename_collection_with_prompt(config, collection, function(_, new_name)
          clear_collection_overview()
          set_picker_search(current, new_name)
          refresh_picker(current)
          vim.notify(string.format("Collection renamed to %s", new_name), vim.log.levels.INFO)
        end, current)
      end,
      arango_truncate_collection = function(current, item)
        local collection = selected_collection(current, item)
        if not collection then
          vim.notify("Select a collection first", vim.log.levels.INFO)
          return
        end

        truncate_collection_with_prompt(config, collection, function()
          clear_collection_overview()
          refresh_picker(current)
          vim.notify(string.format("Collection %s truncated", collection), vim.log.levels.INFO)
        end, current)
      end,
      arango_manage_indexes = function(current, item)
        local collection = selected_collection(current, item)
        if not collection then
          vim.notify("Select a collection first", vim.log.levels.INFO)
          return
        end
        local route = collections_route(config, opts, current_search(current))
        close_picker_then(current, function()
          collection_admin.manage_indexes(config, collection, function()
            clear_collection_overview()
            refresh_picker(current)
          end, function()
            open_route(route)
          end)
        end)
      end,
      arango_edit_collection_properties = function(current, item)
        local collection = selected_collection(current, item)
        if not collection then
          vim.notify("Select a collection first", vim.log.levels.INFO)
          return
        end
        close_picker_then(current, function()
          collection_admin.edit_properties(config, collection, function()
            clear_collection_overview()
            refresh_picker(current)
          end)
        end)
      end,
      arango_pick_database = function(current)
        pick_database(current)
      end,
      arango_action_menu = function(current, item)
        open_action_menu(current, item)
      end,
    },
    win = {
      input = {
        keys = merge_keymaps(
          picker_key(keymaps.execute, "arango_action_menu", { "n", "i" }, "Actions"),
          picker_key(keymaps.create, "arango_create_document", { "n" }, "Create Document"),
          picker_key(keymaps.create_collection, "arango_create_collection", { "n" }, "Create Collection"),
          picker_key(keymaps.duplicate_collection, "arango_duplicate_collection", { "n" }, "Duplicate Collection"),
          picker_key(keymaps.rename, "arango_rename_collection", { "n" }, "Rename Collection"),
          picker_key(keymaps.truncate, "arango_truncate_collection", { "n" }, "Truncate Collection"),
          picker_key(keymaps.back, "arango_pick_database", { "n", "i" }, "Choose Database", opts.allow_database_back)
        ),
      },
      list = {
        keys = merge_keymaps(
          picker_key(keymaps.execute, "arango_action_menu", { "n" }, "Actions"),
          picker_key(keymaps.create, "arango_create_document", { "n" }, "Create Document"),
          picker_key(keymaps.create_collection, "arango_create_collection", { "n" }, "Create Collection"),
          picker_key(keymaps.duplicate_collection, "arango_duplicate_collection", { "n" }, "Duplicate Collection"),
          picker_key(keymaps.rename, "arango_rename_collection", { "n" }, "Rename Collection"),
          picker_key(keymaps.truncate, "arango_truncate_collection", { "n" }, "Truncate Collection"),
          picker_key(keymaps.back, "arango_pick_database", { "n" }, "Choose Database", opts.allow_database_back)
        ),
      },
    },
  })

  picker.opts.search = meta.search
  picker.input.filter.search = meta.search
  picker:find({ refresh = true })

  return picker
end

--- Open the document picker for a collection or related-document route.
browse_collection = function(config, collection, field, initial_search, opts, prev_picker)
  local snacks = get_snacks()
  if not snacks then
    return
  end

  opts = opts or {}
  local page_size = plugin_options().page_size
  local initial_offset = math.max(tonumber(opts.offset) or 0, 0)

  local meta = {
    database = config.database,
    collection = collection,
    field = field,
    offset = initial_offset,
    limit = page_size,
    search = initial_search or "",
    items = {},
    total_count = nil,
    has_more = false,
    page_index = math.floor(initial_offset / page_size) + 1,
    pages = {},
  }

  local route_kind = opts.kind or (type(field) == "table" and "related" or "collection")
  local picker_title = opts.title

  local function current_route()
    return {
      kind = route_kind,
      config = config,
      collection = collection,
      field = meta.field,
      search = meta.search,
      offset = meta.offset,
      values = opts.values,
      prompt = opts.prompt,
      title = picker_title,
    }
  end

  local function open_picker_document(current, item)
    local selected = picker_current_item(current, item)
    if not selected or not selected.item then
      return
    end

    picker_request(current, "ArangoDB", function(done)
      return client.get_document_async(config, selected.item.id, done)
    end, function(payload)
      push_history(current_route())
      close_picker_then(current, function()
        M.open_document(config, vim.tbl_extend("force", payload, { database = config.database }))
      end)
    end)
  end

  local function open_action_menu(current, item)
    local selected = picker_current_item(current, item)
    local choices = {}
    local keymaps = picker_options()

    if selected and selected.item then
      choices[#choices + 1] = { label = "Open document (Enter)", action = "arango_open_document" }
      choices[#choices + 1] =
        { label = action_label("Duplicate document", keymaps.duplicate), action = "arango_duplicate_document" }
      choices[#choices + 1] = { label = action_label("Open related", keymaps.related), action = "arango_open_related" }
      choices[#choices + 1] = { label = "Explore named graph", action = "arango_explore_graph" }
      choices[#choices + 1] =
        { label = action_label("Delete document", keymaps.delete), action = "arango_delete_document" }
    end
    choices[#choices + 1] =
      { label = action_label("Create document", keymaps.create), action = "arango_create_document" }
    choices[#choices + 1] =
      { label = action_label("Truncate collection", keymaps.truncate), action = "arango_truncate_collection" }
    choices[#choices + 1] = { label = "Manage indexes", action = "arango_manage_indexes" }

    if route_kind ~= "related" then
      choices[#choices + 1] =
        { label = action_label("Change filter field", keymaps.change_field), action = "arango_change_field" }
    end
    if meta.search ~= "" then
      choices[#choices + 1] = { label = action_label("Reset search", keymaps.reset), action = "arango_reset_search" }
    end
    if meta.offset > 0 then
      choices[#choices + 1] = { label = action_label("Previous page", keymaps.prev_page), action = "arango_prev_page" }
    end
    if meta.has_more then
      choices[#choices + 1] = { label = action_label("Next page", keymaps.next_page), action = "arango_next_page" }
    end
    if #state.history > 0 then
      choices[#choices + 1] = { label = action_label("Go back", keymaps.back), action = "arango_go_back" }
    end

    vim.schedule(function()
      vim.ui.select(
        choices,
        picker_select_options({
          prompt = string.format("Actions (%s/%s)", config.database, collection),
          format_item = function(choice)
            return choice.label
          end,
        }),
        function(choice)
          if not choice or not current or current.closed then
            return
          end
          execute_picker_action(current, choice.action)
        end
      )
    end)
  end

  local function close_active_cursor()
    local last_page = meta.pages[#meta.pages]
    if last_page and last_page.cursor_id and last_page.has_more then
      client.close_cursor_async(config, last_page.cursor_id)
    end
  end

  local function reset_pages(search)
    close_active_cursor()
    meta.search = search or ""
    meta.page_index = 1
    meta.offset = 0
    meta.pages = {}
    meta.items = {}
    meta.total_count = nil
    meta.has_more = false
  end

  local function invalidate_pages(page_index)
    close_active_cursor()
    meta.pages = {}
    meta.page_index = math.max(tonumber(page_index) or 1, 1)
    meta.offset = (meta.page_index - 1) * meta.limit
    meta.items = {}
    meta.total_count = nil
    meta.has_more = false
  end

  local function format_page(data, index)
    meta.offset = (index - 1) * meta.limit
    meta.total_count = data.total_count or meta.total_count
    meta.has_more = data.has_more or false
    meta.items = data.items or {}
    local items = {}
    for index, entry in ipairs(meta.items) do
      items[#items + 1] = {
        idx = index,
        text = item_text(entry),
        item = vim.tbl_extend("force", entry, { database = config.database }),
        preview = { text = entry.preview, ft = "json", loc = false },
      }
    end
    return items
  end

  local function request_cursor_page(ctx, search, cursor_id)
    return await_picker_request(ctx, function(done)
      if route_kind == "related" then
        return client.browse_related_collection_async(
          config,
          collection,
          field,
          opts.values,
          search,
          meta.limit,
          cursor_id,
          done
        )
      end
      return client.browse_collection_async(config, collection, field, search, meta.limit, cursor_id, done)
    end)
  end

  local function load_page(ctx, search)
    if search ~= meta.search then
      reset_pages(search)
    end

    while #meta.pages < meta.page_index do
      local previous = meta.pages[#meta.pages]
      if previous and not previous.has_more then
        meta.page_index = math.max(#meta.pages, 1)
        break
      end
      local err, data = request_cursor_page(ctx, search, previous and previous.cursor_id or nil)
      if err then
        return err
      end
      meta.pages[#meta.pages + 1] = data
    end

    local page = meta.pages[meta.page_index]
    if not page then
      return nil, {}
    end
    return nil, format_page(page, meta.page_index)
  end

  local picker
  local keymaps = picker_options()
  picker = snacks.picker({
    title = picker_title or title(meta.database, meta.collection, meta.field, meta),
    search = meta.search,
    find = false,
    live = true,
    supports_live = true,
    show_empty = true,
    auto_close = false,
    focus = "input",
    layout = picker_layout,
    finder = function(_, ctx)
      local search = ctx.filter.search or ""
      return function(cb)
        local err, items = load_page(ctx, search)
        if err then
          vim.schedule(function()
            arango.notify_error(err, "ArangoDB")
          end)
          return
        end
        for _, item in ipairs(items) do
          cb(item)
        end
      end
    end,
    format = "text",
    preview = "preview",
    confirm = function(current, item)
      open_picker_document(current, item)
    end,
    on_show = function(current)
      state.picker = current
      restore_picker_backdrop(current)
      watch_picker_backdrop(current)
      if picker_title then
        current.title = picker_title
        current:update_titles()
      else
        update_picker_title(current, meta)
      end
      if prev_picker and not prev_picker.closed then
        prev_picker:close()
      end
    end,
    on_change = function(current)
      state.picker = current
      if picker_title then
        current.title = picker_title
        current:update_titles()
      else
        update_picker_title(current, meta)
      end
    end,
    on_close = function()
      cancel_picker_request(picker)
      close_active_cursor()
      if state.picker == picker then
        state.picker = nil
      end
    end,
    actions = {
      arango_open_document = function(current, item)
        open_picker_document(current, item)
      end,
      arango_duplicate_document = function(current, item)
        local selected = picker_current_item(current, item)
        if not selected or not selected.item or not selected.item.id then
          vim.notify("Select a document first", vim.log.levels.INFO)
          return
        end

        picker_request(current, "ArangoDB", function(done)
          return client.get_document_async(config, selected.item.id, done)
        end, function(payload)
          push_history(current_route())
          close_picker_then(current, function()
            open_duplicate_document(config, collection, payload.document or payload)
          end)
        end)
      end,
      arango_create_document = function(current)
        push_history(current_route())
        close_picker_then(current, function()
          open_new_document(config, collection)
        end)
      end,
      arango_truncate_collection = function(current)
        truncate_collection_with_prompt(config, collection, function()
          reset_pages(meta.search)
          refresh_picker(current)
          vim.notify(string.format("Collection %s truncated", collection), vim.log.levels.INFO)
        end, current)
      end,
      arango_manage_indexes = function(current)
        local route = current_route()
        close_picker_then(current, function()
          collection_admin.manage_indexes(config, collection, function()
            refresh_picker(current)
          end, function()
            open_route(route)
          end)
        end)
      end,
      arango_next_page = function(current)
        if not meta.has_more then
          vim.notify("Already on last page", vim.log.levels.INFO)
          return
        end
        meta.page_index = meta.page_index + 1
        current:find()
      end,
      arango_prev_page = function(current)
        if meta.offset == 0 then
          vim.notify("Already on first page", vim.log.levels.INFO)
          return
        end
        meta.page_index = math.max(1, meta.page_index - 1)
        current:find()
      end,
      arango_change_field = function(current)
        if route_kind == "related" then
          vim.notify("Filter field is fixed for related navigation", vim.log.levels.INFO)
          return
        end
        choose_field(config, collection, current, function(new_field)
          meta.field = new_field
          field = new_field
          reset_pages(meta.search)
          current:find({ refresh = true })
        end)
      end,
      arango_reset_search = function(current)
        reset_pages("")
        current.input:set(nil, "")
        current:find({ refresh = true })
      end,
      arango_open_related = function(current, item)
        local selected = picker_current_item(current, item)
        if not selected or not selected.item then
          return
        end

        local payload = try_decode_preview(selected.item)
        picker_request(current, "ArangoDB Relations", function(done)
          return document_relations_async(config, payload, done)
        end, function(relations)
          open_related_selector(config, relations, function(choice)
            jump_to_related(config, choice, current, current_route())
          end)
        end)
      end,
      arango_explore_graph = function(current, item)
        local selected = picker_current_item(current, item)
        if not selected or not selected.item then
          return
        end
        close_picker_then(current, function()
          require("arangodb.graph").open({ config = config, start = selected.item.id })
        end)
      end,
      arango_delete_document = function(current, item)
        local selected = picker_current_item(current, item)
        if not selected or not selected.item or not selected.item.id then
          return
        end

        local document_id = selected.item.id
        if
          not ensure_unmodified_document_buffers({
            database = config.database,
            id = document_id,
            include_drafts = false,
          }, "deleting this document")
        then
          return
        end

        if not confirm_delete_document(config, document_id) then
          return
        end

        picker_request(current, "ArangoDB Delete", function(done)
          return client.delete_document_async(config, document_id, done)
        end, function()
          close_document_buffers({ database = config.database, id = document_id, include_drafts = false })
          local target_page = meta.page_index > 1 and #meta.items == 1 and (meta.page_index - 1) or meta.page_index
          invalidate_pages(target_page)
          current:find({ refresh = true })
          vim.notify("Document deleted", vim.log.levels.INFO)
        end)
      end,
      arango_action_menu = function(current, item)
        open_action_menu(current, item)
      end,
      arango_go_back = function(current)
        go_back(current)
      end,
    },
    win = {
      input = {
        keys = merge_keymaps(
          picker_key(keymaps.execute, "arango_action_menu", { "n", "i" }, "Actions"),
          picker_key(keymaps.create, "arango_create_document", { "n" }, "Create Document"),
          picker_key(keymaps.duplicate, "arango_duplicate_document", { "n" }, "Duplicate Document"),
          picker_key(keymaps.prev_page, "arango_prev_page", { "n", "i" }, "Previous Page"),
          picker_key(keymaps.next_page, "arango_next_page", { "n", "i" }, "Next Page"),
          picker_key(
            keymaps.change_field,
            "arango_change_field",
            { "n", "i" },
            "Change Filter Field",
            route_kind ~= "related"
          ),
          picker_key(keymaps.reset, "arango_reset_search", { "n", "i" }, "Reset Search"),
          picker_key(keymaps.related, "arango_open_related", { "n", "i" }, "Open Related"),
          picker_key(keymaps.delete, "arango_delete_document", { "n" }, "Delete Document"),
          picker_key(keymaps.truncate, "arango_truncate_collection", { "n" }, "Truncate Collection"),
          picker_key(keymaps.back, "arango_go_back", { "n", "i" }, "Go Back")
        ),
      },
      list = {
        keys = merge_keymaps(
          picker_key(keymaps.execute, "arango_action_menu", { "n" }, "Actions"),
          picker_key(keymaps.create, "arango_create_document", { "n" }, "Create Document"),
          picker_key(keymaps.duplicate, "arango_duplicate_document", { "n" }, "Duplicate Document"),
          picker_key(keymaps.prev_page, "arango_prev_page", { "n" }, "Previous Page"),
          picker_key(keymaps.next_page, "arango_next_page", { "n" }, "Next Page"),
          picker_key(
            keymaps.change_field,
            "arango_change_field",
            { "n" },
            "Change Filter Field",
            route_kind ~= "related"
          ),
          picker_key(keymaps.reset, "arango_reset_search", { "n" }, "Reset Search"),
          picker_key(keymaps.related, "arango_open_related", { "n" }, "Open Related"),
          picker_key(keymaps.delete, "arango_delete_document", { "n" }, "Delete Document"),
          picker_key(keymaps.truncate, "arango_truncate_collection", { "n" }, "Truncate Collection"),
          picker_key(keymaps.back, "arango_go_back", { "n" }, "Go Back")
        ),
      },
    },
  })

  picker.opts.search = meta.search
  picker.input.filter.search = meta.search
  picker:find({ refresh = true })

  return picker
end

--- Open the browser entry point or restore a serialized route.
function M.open(opts)
  opts = opts or {}
  if opts.kind == "document" or opts.kind == "collection" or opts.kind == "collections" or opts.kind == "related" then
    open_route(opts)
    return
  end
  if opts.database and opts.pick_database == nil then
    opts.pick_database = false
  end

  local db_item = opts.database
      and (arango.find_database(opts.database) or {
        name = opts.database,
        url = arango.arango_url(opts.database),
      })
    or arango.default_database()
  if not db_item then
    arango.notify_error("No ArangoDB database configured")
    return
  end

  local resolved, config = pcall(arango.resolve_connection, db_item)
  if not resolved then
    arango.notify_error(config)
    return
  end
  if not config then
    arango.notify_error("Invalid ArangoDB connection URL for " .. tostring(db_item.name or "selected connection"))
    return
  end

  local function with_collection_picker(allow_database_back)
    if opts.reset_history ~= false then
      clear_history()
    end
    browse_collections(config, {
      document_field = opts.field or "_key",
      document_search = opts.search or "",
      allow_database_back = allow_database_back == true,
    })
  end

  if opts.pick_database == false then
    with_collection_picker(false)
    return
  end

  choose_database(function(choice)
    local ok, chosen = pcall(arango.resolve_connection, choice)
    if not ok then
      arango.notify_error(chosen)
      return
    end
    if not chosen then
      arango.notify_error("Invalid ArangoDB connection URL for " .. tostring(choice.name or "selected connection"))
      return
    end
    config = chosen
    with_collection_picker(true)
  end)
end

--- Re-show the active picker, or open the browser again if it was closed.
function M.resume()
  if state.picker and not state.picker.closed then
    state.picker:show()
    restore_picker_backdrop(state.picker)
    watch_picker_backdrop(state.picker)
    state.picker:focus("input", { show = true })
    return
  end
  M.open()
end

return M
