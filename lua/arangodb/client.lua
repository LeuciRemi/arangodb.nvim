--- ArangoDB HTTP client helpers used by the picker and document buffers.
local M = {}

local core = require("arangodb.core")
local http = require("arangodb.http")

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

local function timeout()
  return require("arangodb.config").get().http_timeout or 30000
end

local function database_root(config)
  return "/_db/" .. core.url_encode(config.database)
end

local function trim_message(value)
  if type(value) ~= "string" then
    return value
  end
  return vim.trim(value)
end

--- Decode a JSON response and turn ArangoDB API failures into Lua errors.
local function decode_json_response(response)
  local body = response.body or ""
  local decoded = nil

  if body ~= "" then
    local ok, value = pcall(vim.json.decode, body)
    if ok then
      decoded = value
    end
  end

  if response.status >= 400 then
    if type(decoded) == "table" then
      local message = decoded.errorMessage or decoded.message
      if type(message) == "string" and message ~= "" then
        error(trim_message(message))
      end
    end

    if body ~= "" then
      error(trim_message(body))
    end

    error("ArangoDB request failed with HTTP " .. response.status)
  end

  if decoded == nil then
    if body == "" then
      return {}
    end
    error("Invalid JSON response from ArangoDB")
  end

  return decoded
end

--- Send an authenticated request through the transport configured for the database.
local function request(config, method, path, payload)
  local options = require("arangodb.config").get()
  local body = payload ~= nil and vim.json.encode(payload) or nil
  local response = http.request({
    method = method,
    scheme = config.scheme,
    host = config.host,
    port = config.port,
    path = path,
    body = body,
    user = config.user,
    password = config.password,
    timeout = timeout(),
    tls_verify = options.tls_verify,
    tls_ca_file = options.tls_ca_file,
  })

  return decode_json_response(response)
end

local function server_request(config, method, path, payload)
  return request(config, method, path, payload)
end

local function database_request(config, method, path, payload)
  return request(config, method, database_root(config) .. path, payload)
end

local function split_document_id(document_id)
  local collection, key = tostring(document_id):match("^([^/]+)/(.+)$")
  if not collection or not key then
    error("Invalid document id: " .. tostring(document_id))
  end
  return collection, key
end

local function document_path(collection, key)
  return string.format("/_api/document/%s/%s", core.url_encode(collection), core.url_encode(key))
end

local function collection_path(collection)
  return string.format("/_api/collection/%s", core.url_encode(collection))
end

local function collection_type_label(collection_type)
  if collection_type == 2 then
    return "document"
  end
  if collection_type == 3 then
    return "edge"
  end
  return tostring(collection_type or "unknown")
end

local function collection_status_label(status)
  local labels = {
    [1] = "newborn",
    [2] = "unloaded",
    [3] = "loaded",
    [4] = "loading",
    [5] = "deleted",
    [6] = "corrupted",
  }
  return labels[status] or tostring(status or "unknown")
end

local function collection_size_bytes(figures)
  if type(figures) ~= "table" then
    return nil
  end

  local document_size = type(figures.documentsSize) == "number" and figures.documentsSize or nil
  local index_size = type(figures.indexSize) == "number" and figures.indexSize
    or type(figures.indexesSize) == "number" and figures.indexesSize
    or type(figures.indexes) == "table" and type(figures.indexes.size) == "number" and figures.indexes.size
    or nil

  if document_size or index_size then
    return (document_size or 0) + (index_size or 0)
  end

  local size = 0
  local found = false
  local function add(value)
    if type(value) == "number" then
      size = size + value
      found = true
    end
  end

  if type(figures.alive) == "table" then
    add(figures.alive.size)
  end
  if type(figures.dead) == "table" then
    add(figures.dead.size)
  end
  if type(figures.indexes) == "table" and is_list(figures.indexes) then
    for _, index in ipairs(figures.indexes) do
      if type(index) == "table" then
        add(index.size)
      end
    end
  end

  return found and size or nil
end

local function json_pretty(value, indent, depth)
  indent = indent or 2
  depth = depth or 0

  if value == vim.NIL then
    return "null"
  end

  if type(value) ~= "table" then
    return vim.json.encode(value)
  end

  local current_indent = string.rep(" ", depth * indent)
  local next_indent = string.rep(" ", (depth + 1) * indent)

  if is_list(value) then
    if vim.tbl_isempty(value) then
      return "[]"
    end

    local items = {}
    for _, item in ipairs(value) do
      items[#items + 1] = next_indent .. json_pretty(item, indent, depth + 1)
    end

    return string.format("[\n%s\n%s]", table.concat(items, ",\n"), current_indent)
  end

  local keys = {}
  for key, _ in pairs(value) do
    keys[#keys + 1] = key
  end
  table.sort(keys, function(left, right)
    return tostring(left) < tostring(right)
  end)

  if #keys == 0 then
    return "{}"
  end

  local items = {}
  for _, key in ipairs(keys) do
    items[#items + 1] = string.format(
      "%s%s: %s",
      next_indent,
      vim.json.encode(tostring(key)),
      json_pretty(value[key], indent, depth + 1)
    )
  end

  return string.format("{\n%s\n%s}", table.concat(items, ",\n"), current_indent)
end

local function wrap_document(document, database)
  local document_id = document._id
  return {
    database = database,
    id = document_id,
    key = document._key,
    collection = type(document_id) == "string" and document_id:match("^([^/]+)/") or nil,
    document = document,
    preview = json_pretty(document),
  }
end

local function get_document_raw(config, collection, key)
  return database_request(config, "GET", document_path(collection, key))
end

local function get_collection_count(config, collection)
  local data = database_request(config, "GET", collection_path(collection) .. "/count")
  return data.count or 0
end

--- Execute an AQL query and transparently read every cursor page.
local function run_aql(config, query, bind_vars, batch_size)
  local payload = {
    query = query,
    batchSize = batch_size or 1000,
    count = true,
  }
  if bind_vars and not vim.tbl_isempty(bind_vars) then
    payload.bindVars = bind_vars
  end

  local data = database_request(config, "POST", "/_api/cursor", payload)
  local result = {}
  vim.list_extend(result, data.result or {})

  local extra = data.extra
  local cursor_id = data.id
  while data.hasMore and cursor_id do
    data = database_request(config, "PUT", "/_api/cursor/" .. core.url_encode(cursor_id))
    vim.list_extend(result, data.result or {})
    if extra == nil then
      extra = data.extra
    end
    cursor_id = data.id or cursor_id
  end

  return {
    result = result,
    count = #result,
    extra = extra,
  }
end

--- Collect dotted field paths from sampled documents for the filter picker.
local function collect_field_paths(value, prefix, result, depth, max_depth)
  prefix = prefix or ""
  result = result or {}
  depth = depth or 0
  max_depth = max_depth or 4

  if depth >= max_depth or type(value) ~= "table" or is_list(value) then
    return result
  end

  for key, nested in pairs(value) do
    local path = prefix ~= "" and (prefix .. "." .. key) or key
    result[path] = true
    if type(nested) == "table" and not is_list(nested) then
      collect_field_paths(nested, path, result, depth + 1, max_depth)
    end
  end

  return result
end

local function extract_value(document, field_path)
  if not field_path or field_path == "*" then
    return document
  end

  local value = document
  for part in field_path:gmatch("[^.]+") do
    if type(value) ~= "table" then
      return nil
    end
    value = value[part]
  end

  return value
end

--- Convert a dotted field path into a safe AQL expression.
local function field_expression(field_path)
  if not field_path or field_path == "*" then
    return "doc"
  end

  local expression = "doc"
  for part in field_path:gmatch("[^.]+") do
    if part:find("[.%[%]]") then
      error("Invalid field path segment: " .. part)
    end
    expression = expression .. "[" .. vim.json.encode(part) .. "]"
  end

  return expression
end

local function related_field_paths(field)
  if type(field) == "table" then
    if vim.tbl_isempty(field) then
      error("Missing field")
    end
    return field
  end

  if not field or field == "" then
    error("Missing field")
  end

  return { field }
end

local function related_filter_clause(fields)
  local clauses = {}

  for _, field_name in ipairs(related_field_paths(fields)) do
    local expression = field_expression(field_name)
    clauses[#clauses + 1] = string.format(
      "((IS_ARRAY(%s) AND LENGTH(FOR entry IN %s FILTER POSITION(@values, TO_STRING(entry)) LIMIT 1 RETURN 1) > 0) OR POSITION(@values, TO_STRING(%s)))",
      expression,
      expression,
      expression
    )
  end

  return table.concat(clauses, " OR ")
end

local function related_search_values(value)
  local values = {}
  local seen = {}

  local function add(item)
    if item == nil or item == "" then
      return
    end

    local text = tostring(item)
    if seen[text] then
      return
    end

    seen[text] = true
    values[#values + 1] = text
  end

  if type(value) == "table" and is_list(value) then
    for _, item in ipairs(value) do
      add(item)
    end
  else
    add(value)
  end

  if #values == 0 then
    error("Missing value")
  end

  return values
end

local function truncate_text(value, max_length)
  max_length = max_length or 120

  local text
  if type(value) == "string" then
    text = value
  elseif value == vim.NIL then
    text = "null"
  elseif value == nil then
    text = "null"
  else
    text = vim.json.encode(value)
  end

  text = text:gsub("\n", " "):gsub("\r", " ")
  text = vim.trim(text)
  if #text <= max_length then
    return text
  end
  return text:sub(1, max_length - 1) .. "..."
end

local function related_field_value_text(document, fields)
  local items = {}

  for _, field_name in ipairs(related_field_paths(fields)) do
    local value = extract_value(document, field_name)
    if value ~= nil then
      items[#items + 1] = string.format("%s=%s", field_name, truncate_text(value, 48))
    end
  end

  return table.concat(items, " | ")
end

--- List the databases visible to the authenticated user.
function M.list_databases(config)
  local data = server_request(config, "GET", "/_db/_system/_api/database/user")
  local databases = data.result or {}
  table.sort(databases)
  return databases
end

--- Return collection metadata enriched with labels used by the picker preview.
function M.list_collection_details(config)
  local data = database_request(config, "GET", "/_api/collection")
  local collections = {}

  for _, item in ipairs(data.result or {}) do
    if not item.isSystem then
      collections[#collections + 1] = {
        name = item.name,
        id = item.id,
        global_id = item.globallyUniqueId,
        type = collection_type_label(item.type),
        status = collection_status_label(item.status),
        wait_for_sync = item.waitForSync == true,
        cache_enabled = item.cacheEnabled == true,
        collection = item,
      }
    end
  end

  table.sort(collections, function(left, right)
    return left.name < right.name
  end)
  return collections
end

--- Return collection names sorted for the browser picker.
function M.list_collections(config)
  local collections = M.list_collection_details(config)
  local result = {}

  for _, item in ipairs(collections) do
    result[#result + 1] = item.name
  end

  return result
end

--- Gather high-level database metrics for the collections overview preview.
function M.database_overview(config)
  local overview = {
    name = config.database,
    endpoint = string.format("%s:%s", tostring(config.host), tostring(config.port)),
    collections = M.list_collection_details(config),
  }

  overview.collection_count = #overview.collections

  local ok, current = pcall(database_request, config, "GET", "/_api/database/current")
  if ok and type(current) == "table" and type(current.result) == "table" then
    local info = current.result
    overview.id = info.id
    overview.path = info.path
    overview.is_system = info.isSystem == true
    overview.sharding = info.sharding
    overview.replication_factor = info.replicationFactor
    overview.write_concern = info.writeConcern
  else
    overview.info_error = trim_message(current)
  end

  local total_documents = 0
  local has_total_documents = false
  local total_size = 0
  local has_total_size = false

  for _, item in ipairs(overview.collections) do
    local ok_figures, figures_data = pcall(database_request, config, "GET", collection_path(item.name) .. "/figures")
    if ok_figures and type(figures_data) == "table" then
      if type(figures_data.count) == "number" then
        item.count = figures_data.count
        total_documents = total_documents + figures_data.count
        has_total_documents = true
      end

      local figures = figures_data.figures
      local size = collection_size_bytes(figures)
      if type(size) == "number" then
        item.size = size
        total_size = total_size + size
        has_total_size = true
      end

      item.engine = type(figures) == "table" and figures.engine or figures_data.engine
    else
      item.figures_error = trim_message(figures_data)
    end
  end

  if has_total_documents then
    overview.total_documents = total_documents
  end
  if has_total_size then
    overview.total_size = total_size
  end

  return overview
end

--- Sample collection documents to extract candidate field paths for filtering.
function M.list_fields(config, collection, sample_size)
  local query = "FOR doc IN @@collection LIMIT @sample RETURN doc"
  local data = run_aql(config, query, {
    ["@collection"] = collection,
    sample = math.max(tonumber(sample_size) or 100, 1),
  })
  local fields = {
    _id = true,
    _key = true,
    _rev = true,
  }

  for _, document in ipairs(data.result or {}) do
    collect_field_paths(document, "", fields)
  end

  local result = {}
  for field, _ in pairs(fields) do
    result[#result + 1] = field
  end

  table.sort(result)
  return result
end

--- Fetch a single document and format it for the editor buffer.
function M.get_document(config, document_id)
  local collection, key = split_document_id(document_id)
  local document = get_document_raw(config, collection, key)
  return wrap_document(document, config.database)
end

--- Replace an existing document and return the refreshed document payload.
function M.save_document(config, document)
  if type(document) ~= "table" then
    error("Document payload must be a JSON object")
  end

  local document_id = document._id
  if not document_id then
    error("Document payload must contain _id")
  end

  local collection, key = split_document_id(document_id)
  local saved = database_request(config, "PUT", document_path(collection, key), document)
  local current = get_document_raw(config, collection, key)

  return {
    database = config.database,
    id = document_id,
    key = key,
    collection = collection,
    meta = saved,
    document = current,
    preview = json_pretty(current),
  }
end

--- Delete a document by id and return the ArangoDB response metadata.
function M.delete_document(config, document_id)
  local collection, key = split_document_id(document_id)
  local deleted = database_request(config, "DELETE", document_path(collection, key))

  return {
    database = config.database,
    id = document_id,
    key = key,
    collection = collection,
    meta = deleted,
  }
end

--- Normalize a draft document before creating it in ArangoDB.
local function sanitize_new_document(collection, document)
  if type(document) ~= "table" then
    error("Document payload must be a JSON object")
  end

  collection = type(collection) == "string" and vim.trim(collection) or ""
  if collection == "" then
    local document_id = type(document._id) == "string" and document._id or nil
    collection = document_id and document_id:match("^([^/]+)/") or ""
  end
  if collection == "" then
    error("Missing collection name")
  end

  local key = type(document._key) == "string" and vim.trim(document._key) or ""
  if key == "" then
    error("Document payload must contain _key")
  end

  local payload = vim.deepcopy(document)
  payload._id = nil
  if payload._rev == vim.NIL or payload._rev == "" then
    payload._rev = nil
  end

  return collection, key, payload
end

local function collection_type_code(collection_type)
  local normalized = type(collection_type) == "string" and vim.trim(collection_type):lower() or "document"
  if normalized == "" or normalized == "document" then
    return "document", 2
  end
  if normalized == "edge" then
    return "edge", 3
  end

  error("Collection type must be 'document' or 'edge'")
end

--- Insert a new document and return the created payload formatted for editing.
function M.create_document(config, collection, document)
  local target_collection, key, payload = sanitize_new_document(collection, document)
  local created = database_request(config, "POST", "/_api/document/" .. core.url_encode(target_collection), payload)
  local current = get_document_raw(config, target_collection, created._key or key)

  return {
    database = config.database,
    id = current._id,
    key = current._key,
    collection = target_collection,
    meta = created,
    document = current,
    preview = json_pretty(current),
  }
end

--- Create a collection and normalize the type label for the UI.
function M.create_collection(config, collection, collection_type)
  collection = vim.trim(collection or "")
  if collection == "" then
    error("Missing collection name")
  end

  local normalized_type, type_code = collection_type_code(collection_type)
  local created = database_request(config, "POST", "/_api/collection", {
    name = collection,
    type = type_code,
  })

  return {
    database = config.database,
    name = created.name or collection,
    type = normalized_type,
    collection = created,
  }
end

--- Rename a collection and return the updated name.
function M.rename_collection(config, collection, new_name)
  new_name = vim.trim(new_name or "")
  if new_name == "" then
    error("Missing new collection name")
  end
  if new_name == collection then
    error("The new collection name must be different")
  end

  local renamed = database_request(config, "PUT", collection_path(collection) .. "/rename", {
    name = new_name,
  })

  return {
    database = config.database,
    old_name = collection,
    name = renamed.name or new_name,
    collection = renamed,
  }
end

--- Remove every document from a collection without deleting the collection itself.
function M.truncate_collection(config, collection)
  local truncated = database_request(config, "PUT", collection_path(collection) .. "/truncate?compact=false")

  return {
    database = config.database,
    name = truncated.name or collection,
    collection = truncated,
  }
end

--- Create a new collection and copy every document from the source collection.
function M.duplicate_collection(config, source, target)
  source = vim.trim(source or "")
  target = vim.trim(target or "")
  if source == "" then
    error("Missing source collection name")
  end
  if target == "" then
    error("Missing target collection name")
  end
  if source == target then
    error("The target collection name must be different")
  end

  local source_collection = database_request(config, "GET", collection_path(source))
  local collection_type = collection_type_label(source_collection.type)
  if collection_type ~= "document" and collection_type ~= "edge" then
    error("Unsupported collection type: " .. tostring(source_collection.type))
  end

  local created = M.create_collection(config, target, collection_type)
  local target_name = created.name or target

  run_aql(config, table.concat({
    "FOR doc IN @@source",
    "INSERT UNSET(doc, \"_id\", \"_rev\") INTO @@target",
  }, "\n"), {
    ["@source"] = source,
    ["@target"] = target_name,
  })

  return {
    database = config.database,
    source = source,
    name = target_name,
    type = collection_type,
    copied_count = get_collection_count(config, target_name),
    collection = created.collection,
  }
end

--- Search documents that refer to one or more related ids or keys.
function M.search_related(config, field, value, limit, collection)
  local values = related_search_values(value)
  local matches = {}
  local seen = {}
  local batch_size = math.max(tonumber(limit) or 20, 1)
  local collections = collection and { collection } or M.list_collections(config)
  local fields = related_field_paths(field)
  local filter_clause = related_filter_clause(fields)

  for _, collection_name in ipairs(collections) do
    local query = table.concat({
      "FOR doc IN @@collection",
      "FILTER " .. filter_clause,
      "SORT doc._key",
      "LIMIT @limit",
      "RETURN doc",
    }, "\n")

    local result = run_aql(config, query, {
      ["@collection"] = collection_name,
      values = values,
      limit = batch_size,
    }, batch_size)

    for _, document in ipairs(result.result or {}) do
      local document_id = document._id
      if document_id and not seen[document_id] then
        seen[document_id] = true
        matches[#matches + 1] = wrap_document(document, config.database)
      end
    end
  end

  return {
    matches = matches,
  }
end

--- Browse related documents with pagination support for the live picker.
function M.browse_related_collection(config, collection, field, value, search, offset, limit)
  local values = related_search_values(value)
  local fields = related_field_paths(field)
  local filter_clause = related_filter_clause(fields)

  search = search or ""
  offset = math.max(tonumber(offset) or 0, 0)
  limit = math.max(tonumber(limit) or 50, 1)

  local bind_vars = {
    ["@collection"] = collection,
    values = values,
    offset = offset,
    limit = limit + 1,
  }

  local query_lines = {
    "FOR doc IN @@collection",
    "FILTER " .. filter_clause,
  }

  if search ~= "" then
    bind_vars.search = search:lower()
    query_lines[#query_lines + 1] = "FILTER CONTAINS(LOWER(doc._id), @search) OR CONTAINS(LOWER(doc._key), @search)"
  end

  vim.list_extend(query_lines, {
    "SORT doc._key",
    "LIMIT @offset, @limit",
    "RETURN doc",
  })

  local data = run_aql(config, table.concat(query_lines, "\n"), bind_vars)
  local documents = vim.deepcopy(data.result or {})
  local has_more = #documents > limit

  while #documents > limit do
    documents[#documents] = nil
  end

  local items = {}
  for _, document in ipairs(documents) do
    items[#items + 1] = {
      key = document._key,
      id = document._id,
      field = fields,
      field_value = nil,
      field_value_text = related_field_value_text(document, fields),
      preview = json_pretty(document),
    }
  end

  return {
    database = config.database,
    collection = collection,
    field = fields,
    search = search,
    offset = offset,
    limit = limit,
    total_count = nil,
    has_more = has_more,
    items = items,
  }
end

--- Browse a collection page and return picker-ready preview items.
function M.browse_collection(config, collection, field, search, offset, limit)
  field = field or "_key"
  search = search or ""
  offset = math.max(tonumber(offset) or 0, 0)
  limit = math.max(tonumber(limit) or 50, 1)

  local expression = field_expression(field)
  local bind_vars = {
    ["@collection"] = collection,
    offset = offset,
    limit = limit + 1,
  }
  local filters = {}

  if search ~= "" then
    bind_vars.search = search:lower()
    filters[#filters + 1] = string.format("FILTER CONTAINS(LOWER(TO_STRING(%s)), @search)", expression)
  end

  local query_lines = {
    "FOR doc IN @@collection",
  }
  vim.list_extend(query_lines, filters)
  vim.list_extend(query_lines, {
    "SORT doc._key",
    "LIMIT @offset, @limit",
    "RETURN doc",
  })

  local data = run_aql(config, table.concat(query_lines, "\n"), bind_vars)
  local documents = vim.deepcopy(data.result or {})
  local has_more = #documents > limit

  while #documents > limit do
    documents[#documents] = nil
  end

  local items = {}
  for _, document in ipairs(documents) do
    local value = extract_value(document, field)
    items[#items + 1] = {
      key = document._key,
      id = document._id,
      field = field,
      field_value = value,
      field_value_text = truncate_text(value),
      preview = json_pretty(document),
    }
  end

  return {
    database = config.database,
    collection = collection,
    field = field,
    search = search,
    offset = offset,
    limit = limit,
    total_count = get_collection_count(config, collection),
    has_more = has_more,
    items = items,
  }
end

return M
