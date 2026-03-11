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

local function request(config, method, path, payload)
  local body = payload ~= nil and vim.json.encode(payload) or nil
  local response = http.request({
    method = method,
    host = config.host,
    port = config.port,
    path = path,
    body = body,
    user = config.user,
    password = config.password,
    timeout = timeout(),
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

function M.list_databases(config)
  local data = server_request(config, "GET", "/_db/_system/_api/database/user")
  local databases = data.result or {}
  table.sort(databases)
  return databases
end

function M.list_collections(config)
  local data = database_request(config, "GET", "/_api/collection")
  local collections = {}

  for _, item in ipairs(data.result or {}) do
    if not item.isSystem then
      collections[#collections + 1] = item.name
    end
  end

  table.sort(collections)
  return collections
end

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

function M.get_document(config, document_id)
  local collection, key = split_document_id(document_id)
  local document = get_document_raw(config, collection, key)
  return wrap_document(document, config.database)
end

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

function M.truncate_collection(config, collection)
  local truncated = database_request(config, "PUT", collection_path(collection) .. "/truncate?compact=false")

  return {
    database = config.database,
    name = truncated.name or collection,
    collection = truncated,
  }
end

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
