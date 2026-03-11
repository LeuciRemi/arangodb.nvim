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
  history = {},
}

local ns = vim.api.nvim_create_namespace("arangodb.nvim")
local browse_collection
local go_back

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
    local value = options.value
    local field = options.field
    if type(value) == "string" and value:match("^%s*%[") then
      local ok, decoded = pcall(vim.json.decode, value)
      if ok then
        value = decoded
      end
    end
    if type(field) == "string" and field:match("^%s*%[") then
      local ok, decoded = pcall(vim.json.decode, field)
      if ok then
        field = decoded
      end
    end
    return client.search_related(config, field, value, options.limit, options.collection)
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

local function push_history(route)
  if type(route) ~= "table" then
    return
  end
  state.history[#state.history + 1] = vim.deepcopy(route)
end

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

local function set_picker_search(picker, search)
  if not picker or picker.closed or not picker.input then
    return
  end

  if picker.input.set then
    picker.input:set(nil, search or "")
    return
  end

  if picker.input.filter then
    picker.input.filter.search = search or ""
  end
end

local function field_label(field)
  if type(field) == "table" then
    return table.concat(field, ", ")
  end
  return field or "_key"
end

local function title(database, collection, field, meta)
  local first = #meta.items > 0 and (meta.offset + 1) or 0
  local last = meta.offset + #meta.items
  local hint = "  [^X actions  ^P/^N pages"
  if #state.history > 0 then
    hint = hint .. "  ^B back"
  end
  hint = hint .. "]"
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
  return path:match("([^.]+)$") or path
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

  if type(relation.values) == "table" and is_list(relation.values) then
    for _, value in ipairs(relation.values) do
      add(value)
    end
  else
    add(relation.value)
  end

  return values
end

local function related_values(document, collections)
  local values = {}
  local entries = {}
  local resolved_collections = {
    lookup = collection_lookup(collections),
  }

  local function source_depth(source)
    local _, count = tostring(source or ""):gsub("%.", "")
    return count
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

    local key = display
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

    local source = scope ~= "" and (scope .. "." .. field_name) or field_name
    local inferred_collection = resolve_collection_name(info.base, resolved_collections)

    if info.multiple then
      if type(value) == "table" and is_list(value) then
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
    if scope == "" or type(node) ~= "table" or is_list(node) then
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
    if type(ids) == "table" and is_list(ids) then
      for _, item in ipairs(ids) do
        if add_field_relation(ids_source, item, scope_collection) then
          added = true
        end
      end
    end

    local keys = node._keys
    if type(keys) == "table" and is_list(keys) then
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

    if is_list(value) then
      if scope_collection then
        for _, item in ipairs(value) do
          if type(item) == "table" and not is_list(item) then
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
        local path = scope ~= "" and (scope .. "." .. key) or key
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

local function reverse_related_values(config, document, collections)
  local values = {}

  if type(document) ~= "table" then
    return values
  end

  local document_id, document_collection, document_key = parse_related_id(document._id)
  if not document_collection or not document_key then
    return values
  end

  local candidate_field_names = reverse_relation_fields(document_collection)
  if #candidate_field_names == 0 then
    return values
  end

  local candidate_lookup = {}
  for _, field_name in ipairs(candidate_field_names) do
    candidate_lookup[field_name] = true
  end

  local relation_values = { document_key }
  if document_id then
    relation_values[#relation_values + 1] = document_id
  end

  for _, collection_name in ipairs(collections or {}) do
    if collection_name ~= document_collection then
      local fields = try_lines(config, "ArangoDB", "fields", {
        "--collection",
        collection_name,
        "--sample-size",
        plugin_options().field_sample_size,
      }) or {}

      local matched_fields = {}
      for _, field_path in ipairs(fields) do
        local tail = path_tail(field_path)
        if tail and candidate_lookup[tail] then
          matched_fields[#matched_fields + 1] = field_path
        end
      end

      if #matched_fields > 0 then
        local result = try_call("ArangoDB", client.search_related, config, matched_fields, relation_values, 2, collection_name)
        if result and result.matches and #result.matches > 0 then
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
      end
    end
  end

  return values
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
  if relation.collection and relation.value then
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

local function open_route(route)
  if not route or type(route) ~= "table" then
    return
  end

  if route.kind == "document" then
    local payload = try_json(route.config, "ArangoDB", "get", { "--id", route.id })
    if payload then
      M.open_document(route.config, payload)
    end
    return
  end

  if route.kind == "related" then
    browse_collection(route.config, route.collection, route.field, route.search or "", {
      kind = "related",
      title = string.format("Arango %s/%s - %s", route.config.database, route.collection, related_route_title(route)),
      offset = route.offset,
      values = route.values,
      prompt = route.prompt,
    })
    return
  end

  browse_collection(route.config, route.collection, route.field or "_key", route.search or "", {
    kind = route.kind or "collection",
    title = route.title,
    offset = route.offset,
  })
end

local function related_browse_payload(opts)
  return try_call("ArangoDB", client.browse_related_collection, opts.config, opts.collection, opts.field, opts.values, opts.search, opts.offset, opts.limit)
end

go_back = function(current)
  local route = pop_history()
  if not route then
    vim.notify("No previous ArangoDB view", vim.log.levels.INFO)
    return
  end

  if current and not current.closed then
    close_picker(current)
  end

  open_route(route)
end

function M.back()
  go_back(nil)
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

  if current and not current.closed then
    close_picker(current)
  end

  open_route(route)
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

    local collections = try_lines(config, "ArangoDB", "collections") or {}
    local relations = related_values(payload, collections)
    vim.list_extend(relations, reverse_related_values(config, payload, collections))
    open_related_selector(config, relations, function(choice)
      push_history({
        kind = "document",
        config = config,
        id = vim.b[buf].arangodb_document_id,
      })
      jump_to_related(config, choice, nil, nil)
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
  vim.api.nvim_buf_create_user_command(buf, "ArangoBack", function()
    go_back(nil)
  end, {
    desc = "Return to previous ArangoDB view",
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

browse_collection = function(config, collection, field, initial_search, opts)
  local snacks = get_snacks()
  if not snacks then
    return
  end

  opts = opts or {}

  local meta = {
    database = config.database,
    collection = collection,
    field = field,
    offset = math.max(tonumber(opts.offset) or 0, 0),
    limit = plugin_options().page_size,
    search = initial_search or "",
    items = {},
    total_count = nil,
    has_more = false,
  }

  local route_kind = opts.kind or (type(field) == "table" and "related" or "collection")
  local picker_title = opts.title

  local function open_picker_document(current, item)
    local selected = picker_current_item(current, item)
    if not selected or not selected.item then
      return
    end

    local payload = try_json(config, "ArangoDB", "get", { "--id", selected.item.id })
    if not payload then
      return
    end

    push_history({
      kind = route_kind,
      config = config,
      collection = collection,
      field = meta.field,
      search = meta.search,
      offset = meta.offset,
    })
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

    if route_kind ~= "related" then
      choices[#choices + 1] = { label = "Change filter field (Ctrl-f)", action = "arango_change_field" }
    end
    if meta.search ~= "" then
      choices[#choices + 1] = { label = "Reset search (Ctrl-u)", action = "arango_reset_search" }
    end
    if meta.offset > 0 then
      choices[#choices + 1] = { label = "Previous page (Ctrl-p)", action = "arango_prev_page" }
    end
    if meta.has_more then
      choices[#choices + 1] = { label = "Next page (Ctrl-n)", action = "arango_next_page" }
    end
    if route_kind ~= "related" then
      choices[#choices + 1] = { label = "Rename collection (Ctrl-r)", action = "arango_rename_collection" }
      choices[#choices + 1] = { label = "Truncate collection (Ctrl-t)", action = "arango_truncate_collection" }
    end
    if #state.history > 0 then
      choices[#choices + 1] = { label = "Go back (Ctrl-b)", action = "arango_go_back" }
    end

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
    local data
    if route_kind == "related" then
      data = related_browse_payload({
        config = config,
        collection = collection,
        field = field,
        values = opts.values,
        search = search,
        offset = offset,
        limit = meta.limit,
      })
    else
      data = run_json(config, "browse", {
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
    end

    if not data then
      error("Failed to load ArangoDB data")
    end

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
    title = picker_title or title(meta.database, meta.collection, meta.field, meta),
    search = meta.search,
    find = false,
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
      if picker_title then
        current.title = picker_title
        current:update_titles()
      else
        update_picker_title(current, meta)
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
        if route_kind == "related" then
          vim.notify("Filter field is fixed for related navigation", vim.log.levels.INFO)
          return
        end
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

        local collections = try_lines(config, "ArangoDB", "collections") or {}
        local payload = try_decode_preview(selected.item)
        local relations = related_values(payload, collections)
        vim.list_extend(relations, reverse_related_values(config, payload, collections))
        open_related_selector(config, relations, function(choice)
          jump_to_related(config, choice, current, {
            kind = route_kind,
            config = config,
            collection = collection,
            field = meta.field,
            search = meta.search,
            offset = meta.offset,
          })
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
      arango_go_back = function(current)
        go_back(current)
      end,
    },
    win = {
      input = {
        keys = {
          ["<c-x>"] = { "arango_action_menu", mode = { "n", "i" }, desc = "Actions" },
          ["<c-p>"] = { "arango_prev_page", mode = { "n", "i" }, desc = "Previous Page" },
          ["<c-n>"] = { "arango_next_page", mode = { "n", "i" }, desc = "Next Page" },
          ["<c-f>"] = route_kind ~= "related" and { "arango_change_field", mode = { "n", "i" }, desc = "Change Filter Field" } or nil,
          ["<c-u>"] = { "arango_reset_search", mode = { "n", "i" }, desc = "Reset Search" },
          ["<c-o>"] = { "arango_open_related", mode = { "n", "i" }, desc = "Open Related" },
          ["<c-d>"] = { "arango_delete_document", mode = { "n", "i" }, desc = "Delete Document" },
          ["<c-r>"] = route_kind ~= "related" and { "arango_rename_collection", mode = { "n", "i" }, desc = "Rename Collection" } or nil,
          ["<c-t>"] = route_kind ~= "related" and { "arango_truncate_collection", mode = { "n", "i" }, desc = "Truncate Collection" } or nil,
          ["<c-b>"] = { "arango_go_back", mode = { "n", "i" }, desc = "Go Back" },
        },
      },
      list = {
        keys = {
          ["<c-x>"] = { "arango_action_menu", mode = { "n" }, desc = "Actions" },
          ["<c-p>"] = { "arango_prev_page", mode = { "n" }, desc = "Previous Page" },
          ["<c-n>"] = { "arango_next_page", mode = { "n" }, desc = "Next Page" },
          ["<c-f>"] = route_kind ~= "related" and { "arango_change_field", mode = { "n" }, desc = "Change Filter Field" } or nil,
          ["<c-u>"] = { "arango_reset_search", mode = { "n" }, desc = "Reset Search" },
          ["<c-o>"] = { "arango_open_related", mode = { "n" }, desc = "Open Related" },
          ["<c-d>"] = { "arango_delete_document", mode = { "n" }, desc = "Delete Document" },
          ["<c-r>"] = route_kind ~= "related" and { "arango_rename_collection", mode = { "n" }, desc = "Rename Collection" } or nil,
          ["<c-t>"] = route_kind ~= "related" and { "arango_truncate_collection", mode = { "n" }, desc = "Truncate Collection" } or nil,
          ["<c-b>"] = { "arango_go_back", mode = { "n" }, desc = "Go Back" },
        },
      },
    },
  })

  picker.opts.search = meta.search
  picker.input.filter.search = meta.search
  picker:find({ refresh = true })

  return picker
end

function M.open(opts)
  opts = opts or {}
  if opts.kind == "document" or opts.kind == "collection" or opts.kind == "related" then
    open_route(opts)
    return
  end
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
      if opts.reset_history ~= false then
        clear_history()
      end
      browse_collection(config, collection, opts.field or "_key", opts.search)
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
