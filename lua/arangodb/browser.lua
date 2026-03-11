local M = {}

local arango = require("arangodb.core")
local client = require("arangodb.client")

local function is_list(value)
  if vim.islist then
    return vim.islist(value)
  end
  if type(value) ~= "table" then
    return false
  end

  local count = 0
  for key, _ in pairs(value) do
    if type(key) ~= "number" or key < 1 or key % 1 ~= 0 then
      return false
    end
    count = count + 1
  end

  for index = 1, count do
    if value[index] == nil then
      return false
    end
  end

  return true
end

local state = {
  picker = nil,
}

local ns = vim.api.nvim_create_namespace("arangodb.nvim")

local function plugin_options()
  return require("arangodb.config").get()
end

local function get_snacks()
  local ok, snacks = pcall(require, "snacks")
  if ok then
    return snacks
  end

  arango.notify_error("`folke/snacks.nvim` is required to use the ArangoDB browser")
  return nil
end

local function parse_extra(extra)
  local options = {}
  local index = 1

  while type(extra) == "table" and index <= #extra do
    local key = extra[index]
    if type(key) == "string" and vim.startswith(key, "--") then
      local name = key:sub(3):gsub("-", "_")
      local value = extra[index + 1]
      if value == nil then
        options[name] = true
        index = index + 1
      else
        options[name] = value
        index = index + 2
      end
    else
      options[#options + 1] = key
      index = index + 1
    end
  end

  return options
end

local function run_json(config, subcommand, extra)
  local options = parse_extra(extra)

  if subcommand == "browse" then
    return client.browse_collection(
      config,
      options.collection,
      options.field,
      options.search,
      options.offset,
      options.limit
    )
  end
  if subcommand == "get" then
    return client.get_document(config, options.id)
  end
  if subcommand == "delete" then
    return client.delete_document(config, options.id)
  end
  if subcommand == "rename-collection" then
    return client.rename_collection(config, options.collection, options.name)
  end
  if subcommand == "truncate-collection" then
    return client.truncate_collection(config, options.collection)
  end
  if subcommand == "search-related" then
    return client.search_related(config, options.field, options.value, options.limit)
  end

  error("Unsupported ArangoDB command: " .. tostring(subcommand))
end

local function run_lines(config, subcommand, extra)
  local options = parse_extra(extra)

  if subcommand == "collections" then
    return client.list_collections(config)
  end
  if subcommand == "databases" then
    return client.list_databases(config)
  end
  if subcommand == "fields" then
    return client.list_fields(config, options.collection, options.sample_size)
  end

  error("Unsupported ArangoDB command: " .. tostring(subcommand))
end

local function try_call(title, fn, ...)
  local ok, result = pcall(fn, ...)
  if ok then
    return result
  end
  arango.notify_error(result, title or "ArangoDB")
end

local function try_json(config, title, subcommand, extra)
  return try_call(title, run_json, config, subcommand, extra)
end

local function try_lines(config, title, subcommand, extra)
  return try_call(title, run_lines, config, subcommand, extra)
end

local function close_picker(picker)
  if not picker or picker.closed then
    return
  end
  picker:close()
end

local function refresh_picker(picker, opts)
  picker = picker or state.picker
  if not picker or picker.closed then
    return
  end
  picker:find(opts or { refresh = true })
end

local function execute_picker_action(picker, action)
  if not picker or picker.closed or not picker.list or not picker.list.win then
    return
  end
  picker.list.win:execute(action)
end

local function restore_picker_input_focus(picker)
  if not picker or picker.closed then
    return
  end

  vim.schedule(function()
    if not picker or picker.closed or not picker.input or not picker.input.win then
      return
    end

    local input_win = picker.input.win.win
    if not input_win or not vim.api.nvim_win_is_valid(input_win) then
      return
    end

    picker:focus("input", { show = true })
    if vim.api.nvim_get_current_win() == input_win and vim.fn.mode():sub(1, 1) ~= "i" then
      vim.cmd("startinsert!")
    end
  end)
end

local function title(database, collection, field, meta)
  local first = #meta.items > 0 and (meta.offset + 1) or 0
  local last = meta.offset + #meta.items
  local hint = "  [^X actions  ^P/^N pages]"
  if meta.total_count ~= nil and (meta.search == nil or meta.search == "") then
    return string.format(
      "Arango %s/%s - %s (%d-%d/%d)%s%s",
      database,
      collection,
      field,
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
    field,
    first,
    last,
    meta.has_more and "+" or "",
    hint
  )
end

local function prompt_select(items, opts, callback)
  vim.ui.select(items, opts, function(choice)
    if choice then
      callback(choice)
    end
  end)
end

local function prompt_input(opts, callback)
  vim.ui.input(opts, function(value)
    if value ~= nil then
      callback(value)
    end
  end)
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
    if vim.api.nvim_buf_is_valid(buf) and vim.b[buf].arangodb_document_id then
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

local function confirm_delete_document(document_id)
  return vim.fn.confirm(
    string.format("Delete document %s?", document_id),
    "&Delete\n&Cancel",
    2
  ) == 1
end

local function confirm_collection_name(action, collection, callback)
  prompt_input({
    prompt = string.format("Type %s to %s: ", collection, action),
  }, function(value)
    if vim.trim(value) ~= collection then
      vim.notify("Confirmation does not match collection name", vim.log.levels.WARN, { title = "ArangoDB" })
      return
    end
    callback()
  end)
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

local function choose_collection(config, callback)
  local collections = try_lines(config, "ArangoDB", "collections")
  if not collections then
    return
  end
  if #collections == 0 then
    vim.notify("No collections found in " .. config.database, vim.log.levels.WARN)
    return
  end

  prompt_select(collections, {
    prompt = string.format("Collection (%s)", config.database),
  }, callback)
end

local function choose_field(config, collection, picker, callback)
  local fields = try_lines(config, "ArangoDB", "fields", {
    "--collection",
    collection,
    "--sample-size",
    plugin_options().field_sample_size,
  })
  if not fields then
    return
  end

  if #fields == 0 then
    fields = { "_key" }
  end

  vim.ui.select(fields, {
    prompt = string.format("Filter field (%s/%s)", config.database, collection),
  }, function(choice)
    if choice then
      callback(choice)
    end
    restore_picker_input_focus(picker)
  end)
end

local function document_buffer_name(doc)
  return string.format("arangodb://%s/%s", doc.database, doc.id)
end

local function set_buffer_json(buf, text)
  local win = vim.fn.bufwinid(buf)
  local view = win ~= -1 and vim.fn.winsaveview() or nil
  vim.bo[buf].modifiable = true
  vim.api.nvim_buf_set_lines(buf, 0, -1, false, vim.split(text, "\n", { plain = true }))
  vim.bo[buf].modified = false
  if view then
    pcall(vim.fn.winrestview, view)
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

local function refresh_collection_document_buffers(config, old_collection, new_collection)
  for _, buf in ipairs(arangodb_document_buffers({
    database = config.database,
    collection = old_collection,
  })) do
    local document_id = vim.b[buf].arangodb_document_id
    local key = type(document_id) == "string" and document_id:match("^[^/]+/(.+)$") or nil
    if key then
      local payload = try_json(config, "ArangoDB Rename Collection", "get", { "--id", new_collection .. "/" .. key })
      if payload then
        M.open_document(config, vim.tbl_extend("force", payload, {
          database = config.database,
          buf = buf,
          show = false,
        }))
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

local function related_values(document)
  local values = {}
  local seen = {}

  local function add(label, field, value)
    if value == nil or value == "" then
      return
    end
    local key = field .. "\0" .. tostring(value)
    if seen[key] then
      return
    end
    seen[key] = true
    values[#values + 1] = {
      label = label,
      field = field,
      value = tostring(value),
    }
  end

  local function walk(prefix, value)
    if type(value) == "table" then
      if is_list(value) then
        for _, item in ipairs(value) do
          if type(item) ~= "table" then
            add(string.format("%s = %s", prefix, tostring(item)), prefix, item)
          end
        end
      else
        for key, nested in pairs(value) do
          local path = prefix ~= "" and (prefix .. "." .. key) or key
          walk(path, nested)
        end
      end
      return
    end

    if type(value) ~= "string" and type(value) ~= "number" and type(value) ~= "boolean" then
      return
    end

    local text = tostring(value)
    if prefix == "_id" or prefix:sub(-3) == "_id" or prefix:sub(-4) == "_key" or prefix:sub(-4) == "_ids" then
      add(string.format("%s = %s", prefix, text), prefix, text)
      return
    end
    if text:find("/", 1, true) then
      add(string.format("%s = %s", prefix, text), prefix, text)
    end
  end

  walk("", document)
  return values
end

local function jump_to_related(config, relation)
  if relation.value:find("/", 1, true) then
    local payload = try_json(config, "ArangoDB", "get", { "--id", relation.value })
    if not payload then
      return
    end
    M.open_document(config, payload)
    return
  end

  local result = try_json(config, "ArangoDB", "search-related", {
    "--field",
    relation.field,
    "--value",
    relation.value,
    "--limit",
    20,
  })
  if not result then
    return
  end

  local matches = result.matches or {}
  if #matches == 0 then
    vim.notify("No related document found", vim.log.levels.INFO)
    return
  end

  if #matches == 1 then
    M.open_document(config, vim.tbl_extend("force", matches[1], { database = config.database }))
    return
  end

  prompt_select(matches, {
    prompt = string.format("Related documents for %s", relation.field),
    format_item = function(item)
      return string.format("%s  %s", item.id or "?", item.preview:gsub("\n.*", ""))
    end,
  }, function(choice)
    M.open_document(config, vim.tbl_extend("force", choice, { database = config.database }))
  end)
end

local function document_actions(config, buf)
  if vim.b[buf].arangodb_actions_initialized then
    return
  end

  local function save_document()
    local ok, payload = pcall(get_current_document_payload, buf)
    if not ok then
      arango.notify_error(payload, "ArangoDB Save")
      return
    end

    local result = try_call("ArangoDB Save", client.save_document, config, payload)
    if not result then
      return
    end

    M.open_document(config, vim.tbl_extend("force", result, { database = config.database, buf = buf }))
    vim.notify("Document saved", vim.log.levels.INFO)
  end

  local function open_related_picker()
    local ok, payload = pcall(get_current_document_payload, buf)
    if not ok then
      arango.notify_error(payload, "ArangoDB Relations")
      return
    end

    local relations = related_values(payload)
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
      jump_to_related(config, choice)
    end)
  end

  local function delete_document()
    local document_id = vim.b[buf].arangodb_document_id
    if not document_id then
      arango.notify_error("Missing current document id", "ArangoDB Delete")
      return
    end

    if not confirm_delete_document(document_id) then
      return
    end

    if not ensure_unmodified_document_buffers({ id = document_id }, "deleting this document") then
      return
    end

    local result = try_json(config, "ArangoDB Delete", "delete", { "--id", document_id })
    if not result then
      return
    end

    close_document_buffers({ id = document_id })
    refresh_picker()
    vim.notify("Document deleted", vim.log.levels.INFO)
  end

  local keymaps = plugin_options().document_keymaps or {}
  if keymaps.save then
    vim.keymap.set("n", keymaps.save, save_document, { buffer = buf, desc = "Save Arango document" })
  end
  if keymaps.delete then
    vim.keymap.set("n", keymaps.delete, delete_document, { buffer = buf, desc = "Delete Arango document" })
  end
  if keymaps.related then
    vim.keymap.set("n", keymaps.related, open_related_picker, { buffer = buf, desc = "Open related Arango document" })
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
  vim.api.nvim_buf_create_user_command(buf, "ArangoDocumentRelated", open_related_picker, {
    desc = "Open related Arango document",
  })

  vim.b[buf].arangodb_actions_initialized = true
end

function M.open_document(config, doc)
  local buf
  local target = doc.buf
  if target and vim.api.nvim_buf_is_valid(target) then
    buf = target
  else
    buf = vim.fn.bufadd(document_buffer_name(doc))
  end

  local preview = doc.preview
  if not preview and type(doc.document) == "table" then
    preview = vim.json.encode(doc.document)
  end
  preview = preview or "{}"

  vim.fn.bufload(buf)
  pcall(vim.api.nvim_buf_set_name, buf, document_buffer_name(doc))
  set_buffer_json(buf, preview)

  vim.bo[buf].filetype = "json"
  vim.bo[buf].buftype = ""
  vim.bo[buf].buflisted = true
  vim.bo[buf].bufhidden = "hide"
  vim.bo[buf].swapfile = false
  vim.bo[buf].modifiable = true
  vim.bo[buf].modified = false

  vim.b[buf].arangodb_config = config
  vim.b[buf].arangodb_document = doc.document
  vim.b[buf].arangodb_document_id = doc.id
  vim.b[buf].arangodb_document_collection = doc.collection
  vim.b[buf].arangodb_database = doc.database or config.database

  vim.api.nvim_buf_clear_namespace(buf, ns, 0, -1)
  vim.api.nvim_buf_set_extmark(buf, ns, 0, 0, {
    virt_text = {
      { string.format(" ArangoDB %s/%s ", vim.b[buf].arangodb_database, doc.id), "Title" },
      { "  :ArangoDocumentSave  :ArangoDocumentDelete  :ArangoDocumentRelated", "Comment" },
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

local function item_text(item)
  return string.format("%s  %s", item.key or "?", item.field_value_text or "")
end

local function update_picker_title(picker, meta)
  picker.title = title(meta.database, meta.collection, meta.field, meta)
  picker:update_titles()
end

local function browse_collection(config, collection, field)
  local snacks = get_snacks()
  if not snacks then
    return
  end

  local meta = {
    database = config.database,
    collection = collection,
    field = field,
    offset = 0,
    limit = plugin_options().page_size,
    search = "",
    items = {},
    total_count = nil,
    has_more = false,
  }

  local function open_picker_document(current, item)
    local selected = picker_current_item(current, item)
    if not selected or not selected.item then
      return
    end

    local payload = try_json(config, "ArangoDB", "get", { "--id", selected.item.id })
    if not payload then
      return
    end

    close_picker(current)
    M.open_document(config, vim.tbl_extend("force", payload, { database = config.database }))
  end

  local function open_action_menu(current, item)
    local selected = picker_current_item(current, item)
    local choices = {}

    if selected and selected.item then
      choices[#choices + 1] = { label = "Open document (Enter)", action = "arango_open_document" }
      choices[#choices + 1] = { label = "Open related (Ctrl-o)", action = "arango_open_related" }
      choices[#choices + 1] = { label = "Delete document (Ctrl-d)", action = "arango_delete_document" }
    end

    choices[#choices + 1] = { label = "Change filter field (Ctrl-f)", action = "arango_change_field" }
    if meta.search ~= "" then
      choices[#choices + 1] = { label = "Reset search (Ctrl-u)", action = "arango_reset_search" }
    end
    if meta.offset > 0 then
      choices[#choices + 1] = { label = "Previous page (Ctrl-p)", action = "arango_prev_page" }
    end
    if meta.has_more then
      choices[#choices + 1] = { label = "Next page (Ctrl-n)", action = "arango_next_page" }
    end
    choices[#choices + 1] = { label = "Rename collection (Ctrl-r)", action = "arango_rename_collection" }
    choices[#choices + 1] = { label = "Truncate collection (Ctrl-t)", action = "arango_truncate_collection" }

    vim.schedule(function()
      vim.ui.select(choices, {
        prompt = string.format("Actions (%s/%s)", config.database, collection),
        format_item = function(choice)
          return choice.label
        end,
      }, function(choice)
        if not choice or not current or current.closed then
          return
        end
        execute_picker_action(current, choice.action)
      end)
    end)
  end

  local function load_page(search, offset)
    local data = run_json(config, "browse", {
      "--collection",
      collection,
      "--field",
      field,
      "--search",
      search or "",
      "--offset",
      offset,
      "--limit",
      meta.limit,
    })

    meta.search = data.search or ""
    meta.offset = data.offset or 0
    meta.limit = data.limit or meta.limit
    meta.total_count = data.total_count
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

  local picker
  picker = snacks.picker({
    title = title(meta.database, meta.collection, meta.field, meta),
    live = true,
    supports_live = true,
    show_empty = true,
    auto_close = false,
    focus = "input",
    layout = {
      preset = "vertical",
      preview = true,
    },
    finder = function(_, ctx)
      local search = ctx.filter.search or ""
      local offset = search == meta.search and meta.offset or 0
      local ok, items = pcall(load_page, search, offset)
      if not ok then
        vim.schedule(function()
          arango.notify_error(items, "ArangoDB")
        end)
        return {}
      end
      return items
    end,
    format = "text",
    preview = "preview",
    confirm = function(current, item)
      open_picker_document(current, item)
    end,
    on_show = function(current)
      state.picker = current
      update_picker_title(current, meta)
    end,
    on_change = function(current)
      state.picker = current
      update_picker_title(current, meta)
    end,
    on_close = function()
      if state.picker == picker then
        state.picker = nil
      end
    end,
    actions = {
      arango_open_document = function(current, item)
        open_picker_document(current, item)
      end,
      arango_next_page = function(current)
        if not meta.has_more then
          vim.notify("Already on last page", vim.log.levels.INFO)
          return
        end
        meta.offset = meta.offset + meta.limit
        current:find()
      end,
      arango_prev_page = function(current)
        if meta.offset == 0 then
          vim.notify("Already on first page", vim.log.levels.INFO)
          return
        end
        meta.offset = math.max(0, meta.offset - meta.limit)
        current:find()
      end,
      arango_change_field = function(current)
        choose_field(config, collection, current, function(new_field)
          meta.field = new_field
          field = new_field
          meta.offset = 0
          current:find({ refresh = true })
        end)
      end,
      arango_reset_search = function(current)
        meta.offset = 0
        current.input:set(nil, "")
        current:find({ refresh = true })
      end,
      arango_open_related = function(current, item)
        local selected = picker_current_item(current, item)
        if not selected or not selected.item then
          return
        end

        local relations = related_values(try_decode_preview(selected.item))
        if #relations == 0 then
          vim.notify("No related values found for this document", vim.log.levels.INFO)
          return
        end

        prompt_select(relations, {
          prompt = "Related values",
          format_item = function(relation)
            return relation.label
          end,
        }, function(choice)
          jump_to_related(config, choice)
        end)
      end,
      arango_delete_document = function(current, item)
        local selected = picker_current_item(current, item)
        if not selected or not selected.item or not selected.item.id then
          return
        end

        local document_id = selected.item.id
        if not confirm_delete_document(document_id) then
          return
        end

        if not ensure_unmodified_document_buffers({ id = document_id }, "deleting this document") then
          return
        end

        local result = try_json(config, "ArangoDB Delete", "delete", { "--id", document_id })
        if not result then
          return
        end

        close_document_buffers({ id = document_id })
        if meta.offset > 0 and #meta.items == 1 then
          meta.offset = math.max(0, meta.offset - meta.limit)
        end
        current:find({ refresh = true })
        vim.notify("Document deleted", vim.log.levels.INFO)
      end,
      arango_rename_collection = function(current)
        if not ensure_unmodified_document_buffers({
          database = config.database,
          collection = collection,
        }, "renaming this collection") then
          return
        end

        prompt_input({
          prompt = string.format("Rename collection %s to: ", collection),
          default = collection,
        }, function(value)
          local new_name = vim.trim(value)
          if new_name == "" or new_name == collection then
            return
          end

          local previous = collection
          local result = try_json(config, "ArangoDB Rename Collection", "rename-collection", {
            "--collection",
            previous,
            "--name",
            new_name,
          })
          if not result then
            return
          end

          collection = result.name or new_name
          meta.collection = collection
          meta.offset = 0
          refresh_collection_document_buffers(config, previous, collection)
          refresh_picker(current)
          vim.notify(string.format("Collection renamed to %s", collection), vim.log.levels.INFO)
        end)
      end,
      arango_truncate_collection = function(current)
        if not ensure_unmodified_document_buffers({
          database = config.database,
          collection = collection,
        }, "truncating this collection") then
          return
        end

        confirm_collection_name("truncate this collection", collection, function()
          local result = try_json(config, "ArangoDB Truncate Collection", "truncate-collection", {
            "--collection",
            collection,
          })
          if not result then
            return
          end

          close_document_buffers({
            database = config.database,
            collection = collection,
          })
          meta.offset = 0
          refresh_picker(current)
          vim.notify(string.format("Collection %s truncated", collection), vim.log.levels.INFO)
        end)
      end,
      arango_action_menu = function(current, item)
        open_action_menu(current, item)
      end,
    },
    win = {
      input = {
        keys = {
          ["<c-x>"] = { "arango_action_menu", mode = { "n", "i" }, desc = "Actions" },
          ["<c-p>"] = { "arango_prev_page", mode = { "n", "i" }, desc = "Previous Page" },
          ["<c-n>"] = { "arango_next_page", mode = { "n", "i" }, desc = "Next Page" },
          ["<c-f>"] = { "arango_change_field", mode = { "n", "i" }, desc = "Change Filter Field" },
          ["<c-u>"] = { "arango_reset_search", mode = { "n", "i" }, desc = "Reset Search" },
          ["<c-o>"] = { "arango_open_related", mode = { "n", "i" }, desc = "Open Related" },
          ["<c-d>"] = { "arango_delete_document", mode = { "n", "i" }, desc = "Delete Document" },
          ["<c-r>"] = { "arango_rename_collection", mode = { "n", "i" }, desc = "Rename Collection" },
          ["<c-t>"] = { "arango_truncate_collection", mode = { "n", "i" }, desc = "Truncate Collection" },
        },
      },
      list = {
        keys = {
          ["<c-x>"] = { "arango_action_menu", mode = { "n" }, desc = "Actions" },
          ["<c-p>"] = { "arango_prev_page", mode = { "n" }, desc = "Previous Page" },
          ["<c-n>"] = { "arango_next_page", mode = { "n" }, desc = "Next Page" },
          ["<c-f>"] = { "arango_change_field", mode = { "n" }, desc = "Change Filter Field" },
          ["<c-u>"] = { "arango_reset_search", mode = { "n" }, desc = "Reset Search" },
          ["<c-o>"] = { "arango_open_related", mode = { "n" }, desc = "Open Related" },
          ["<c-d>"] = { "arango_delete_document", mode = { "n" }, desc = "Delete Document" },
          ["<c-r>"] = { "arango_rename_collection", mode = { "n" }, desc = "Rename Collection" },
          ["<c-t>"] = { "arango_truncate_collection", mode = { "n" }, desc = "Truncate Collection" },
        },
      },
    },
  })

  picker.input.filter.search = meta.search
  return picker
end

function M.open(opts)
  opts = opts or {}
  if opts.database and opts.pick_database == nil then
    opts.pick_database = false
  end

  local db_item = opts.database and (arango.find_database(opts.database) or {
    name = opts.database,
    url = arango.arango_url(opts.database),
  })
    or arango.default_database()
  if not db_item then
    arango.notify_error("No ArangoDB database configured")
    return
  end

  local config = arango.parse_connection(db_item.url)
  if not config then
    arango.notify_error("Invalid ArangoDB connection URL: " .. db_item.url)
    return
  end

  local function with_collection()
    choose_collection(config, function(collection)
      browse_collection(config, collection, opts.field or "_key")
    end)
  end

  if opts.pick_database == false then
    with_collection()
    return
  end

  choose_database(function(choice)
    local chosen = arango.parse_connection(choice.url)
    if not chosen then
      arango.notify_error("Invalid ArangoDB connection URL: " .. choice.url)
      return
    end
    config = chosen
    with_collection()
  end)
end

function M.resume()
  if state.picker and not state.picker.closed then
    state.picker:show()
    state.picker:focus("input", { show = true })
    return
  end
  M.open()
end

return M
