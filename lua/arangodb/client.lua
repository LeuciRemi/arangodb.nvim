--- ArangoDB HTTP client helpers used by the picker and document buffers.
local M = {}

local async = require("arangodb.async")
local cache = require("arangodb.cache")
local core = require("arangodb.core")
local errors = require("arangodb.errors")
local transport = require("arangodb.client.transport")
local utils = require("arangodb.utils")

local function plugin_options()
  return require("arangodb.config").get()
end

local function cache_ttl()
  return plugin_options().cache_ttl or 0
end

local function trim_message(value)
  if type(value) ~= "string" then
    return value
  end
  return vim.trim(value)
end

-- Keep credentials out of cache keys without calling Vimscript functions. Client
-- requests may start from a Snacks/libuv fast event, where vim.fn.sha256() is not
-- allowed. Two independent 32-bit rolling hashes are sufficient for this
-- process-local cache namespace and remain stable in every Neovim context.
local function credential_fingerprint(value)
  value = tostring(value)
  local first = 5381
  local second = 2166136261
  for index = 1, #value do
    local byte = value:byte(index)
    first = (first * 33 + byte) % 4294967296
    second = (second * 65599 + byte) % 4294967296
  end
  return string.format("%08x%08x", first, second)
end

local function connection_cache_prefix(config)
  local password_fingerprint = config.password and credential_fingerprint(config.password) or ""
  return table.concat({
    tostring(config.scheme),
    tostring(config.host),
    tostring(config.port),
    tostring(config.database),
    tostring(config.user),
    password_fingerprint,
  }, "\0") .. "\0"
end

local function cache_key(config, resource)
  return connection_cache_prefix(config) .. resource
end

local function invalidate_cache(config)
  cache.invalidate(connection_cache_prefix(config), true)
end

local server_request = transport.server_request
local database_request = transport.database_request
local database_request_async = transport.database_request_async

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

local function index_path(index_id)
  local collection, identifier = tostring(index_id):match("^([^/]+)/(.+)$")
  if not collection or not identifier then
    error("Invalid index id: " .. tostring(index_id))
  end
  return string.format("/_api/index/%s/%s", core.url_encode(collection), core.url_encode(identifier))
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
  if type(figures.indexes) == "table" and utils.is_list(figures.indexes) then
    for _, index in ipairs(figures.indexes) do
      if type(index) == "table" then
        add(index.size)
      end
    end
  end

  return found and size or nil
end

local function wrap_document(document, database)
  local document_id = document._id
  return {
    database = database,
    id = document_id,
    key = document._key,
    collection = type(document_id) == "string" and document_id:match("^([^/]+)/") or nil,
    document = document,
    preview = utils.json_pretty(document),
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
    batchSize = batch_size or plugin_options().aql_batch_size or 1000,
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
  local seen_cursors = {}
  while data.hasMore do
    if not cursor_id or cursor_id == "" then
      error("ArangoDB returned an incomplete cursor response")
    end
    if seen_cursors[cursor_id] then
      error("ArangoDB returned a repeated cursor id")
    end
    seen_cursors[cursor_id] = true

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

--- Collect escaped dotted field paths from sampled documents for the filter picker.
local function collect_field_paths(value, prefix, result, depth, max_depth)
  prefix = prefix or ""
  result = result or {}
  depth = depth or 0
  max_depth = max_depth or plugin_options().max_field_depth or 4

  if depth >= max_depth or type(value) ~= "table" or utils.is_list(value) then
    return result
  end

  for key, nested in pairs(value) do
    local segment = utils.escape_field_segment(key)
    local path = prefix ~= "" and (prefix .. "." .. segment) or segment
    result[path] = true
    if type(nested) == "table" and not utils.is_list(nested) then
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
  for _, part in ipairs(utils.field_path_segments(field_path)) do
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
  for _, part in ipairs(utils.field_path_segments(field_path)) do
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

  if type(value) == "table" and utils.is_list(value) then
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
  max_length = max_length or plugin_options().truncate_length or 120

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
  local key = cache_key(config, "collections")
  local cached = cache.get(key)
  if cached then
    return cached
  end

  local data = database_request(config, "GET", "/_api/collection")
  local collections = {}

  for _, item in ipairs(data.result or {}) do
    if plugin_options().show_system_collections or not item.isSystem then
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
  return cache.set(key, collections, cache_ttl())
end

--- Return collection details without blocking the editor.
function M.list_collection_details_async(config, callback)
  local key = cache_key(config, "collections")
  local cached = cache.get(key)
  if cached then
    vim.schedule(function()
      callback(nil, cached)
    end)
    return { cancel = function() end }
  end

  return database_request_async(config, "GET", "/_api/collection", nil, function(err, data)
    if err then
      callback(err)
      return
    end
    local collections = {}
    for _, item in ipairs(data.result or {}) do
      if plugin_options().show_system_collections or not item.isSystem then
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
    callback(nil, cache.set(key, collections, cache_ttl()))
  end)
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

--- Return collection names without blocking the editor.
function M.list_collections_async(config, callback)
  return M.list_collection_details_async(config, function(err, collections)
    if err then
      callback(err)
      return
    end
    local result = {}
    for _, item in ipairs(collections) do
      result[#result + 1] = item.name
    end
    callback(nil, result)
  end)
end

--- List named graph definitions without blocking Neovim.
function M.list_graphs_async(config, callback)
  return database_request_async(config, "GET", "/_api/gharial", nil, function(err, data)
    if err then
      callback(err)
      return
    end
    callback(nil, data.graphs or {})
  end)
end

--- Create a named graph definition. Primarily useful for automation and tests.
function M.create_named_graph(config, definition)
  if type(definition) ~= "table" or type(definition.name) ~= "string" then
    error("Named graph definition must contain a name", 0)
  end
  return database_request(config, "POST", "/_api/gharial", definition).graph
end

--- Delete a named graph while retaining its collections by default.
function M.delete_named_graph(config, graph, drop_collections)
  return database_request(
    config,
    "DELETE",
    "/_api/gharial/" .. core.url_encode(graph) .. "?dropCollections=" .. tostring(drop_collections == true)
  )
end

--- Traverse a bounded named graph neighborhood with AQL.
function M.traverse_graph_async(config, graph, start_vertex, opts, callback)
  opts = opts or {}
  local direction = tostring(opts.direction or "ANY"):upper()
  if direction ~= "ANY" and direction ~= "OUTBOUND" and direction ~= "INBOUND" then
    error("Graph direction must be ANY, OUTBOUND, or INBOUND", 0)
  end
  local depth = math.min(math.max(tonumber(opts.depth) or 2, 1), 10)
  depth = math.floor(depth)
  local limit = math.max(math.floor(tonumber(opts.limit) or 100), 1)
  local query = table.concat({
    string.format("FOR vertex, edge, path IN 0..%d %s @start GRAPH %s", depth, direction, vim.json.encode(graph)),
    'OPTIONS { order: "bfs", uniqueVertices: "global" }',
    "LIMIT @limit",
    "RETURN {",
    "  vertex: vertex,",
    "  edge: edge,",
    "  depth: LENGTH(path.edges),",
    "  vertexIds: path.vertices[*]._id",
    "}",
  }, "\n")
  return M.execute_aql_async(config, query, { start = start_vertex, limit = limit }, {
    batch_size = limit,
    cursor_ttl = 60,
  }, callback)
end

--- Gather high-level database metrics for the collections overview preview.
function M.database_overview(config, opts)
  opts = opts or {}
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

  if opts.include_figures ~= true then
    return overview
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

--- Gather database metadata and optional collection figures without blocking the editor.
function M.database_overview_async(config, opts, callback)
  if type(opts) == "function" then
    callback = opts
    opts = {}
  end
  opts = opts or {}

  local cancelled = false
  local requests = {}

  local function track(request_handle)
    if request_handle then
      requests[#requests + 1] = request_handle
    end
    return request_handle
  end

  local function cancel()
    if cancelled then
      return
    end
    cancelled = true
    for _, request_handle in ipairs(requests) do
      if request_handle.cancel then
        request_handle.cancel()
      end
    end
  end

  local function load_overview(collections)
    if cancelled then
      return
    end

    local overview = {
      name = config.database,
      endpoint = string.format("%s:%s", tostring(config.host), tostring(config.port)),
      collections = vim.deepcopy(collections or {}),
    }
    overview.collection_count = #overview.collections

    local include_figures = opts.include_figures == true
    local pending = 1 + (include_figures and #overview.collections or 0)
    local total_documents = 0
    local total_size = 0
    local has_all_document_counts = true
    local has_all_sizes = true

    local function complete_one()
      if cancelled then
        return
      end
      pending = pending - 1
      if pending > 0 then
        return
      end

      if include_figures and has_all_document_counts then
        overview.total_documents = total_documents
      end
      if include_figures and has_all_sizes then
        overview.total_size = total_size
      end
      callback(nil, overview)
    end

    track(database_request_async(config, "GET", "/_api/database/current", nil, function(err, current)
      if cancelled then
        return
      end
      if err then
        overview.info_error = trim_message(err)
      elseif type(current) == "table" and type(current.result) == "table" then
        local info = current.result
        overview.id = info.id
        overview.path = info.path
        overview.is_system = info.isSystem == true
        overview.sharding = info.sharding
        overview.replication_factor = info.replicationFactor
        overview.write_concern = info.writeConcern
      end
      complete_one()
    end))

    if not include_figures then
      return
    end

    local next_collection = 1
    local active_metrics = 0
    local max_concurrent_metrics = 4

    local function load_next_metrics()
      if cancelled then
        return
      end
      while active_metrics < max_concurrent_metrics and next_collection <= #overview.collections do
        local item = overview.collections[next_collection]
        next_collection = next_collection + 1
        active_metrics = active_metrics + 1

        track(M.collection_metrics_async(config, item.name, function(err, metrics)
          if cancelled then
            return
          end
          active_metrics = active_metrics - 1
          if err then
            item.figures_error = trim_message(err)
            has_all_document_counts = false
            has_all_sizes = false
          else
            item.count = metrics.count
            item.size = metrics.size
            item.engine = metrics.engine

            if type(metrics.count) == "number" then
              total_documents = total_documents + metrics.count
            else
              has_all_document_counts = false
            end
            if type(metrics.size) == "number" then
              total_size = total_size + metrics.size
            else
              has_all_sizes = false
            end
          end
          complete_one()
          load_next_metrics()
        end))
      end
    end

    load_next_metrics()
  end

  if opts.collections ~= nil then
    load_overview(opts.collections)
  else
    track(M.list_collection_details_async(config, function(err, collections)
      if cancelled then
        return
      end
      if err then
        callback(err)
        return
      end
      load_overview(collections)
    end))
  end

  return { cancel = cancel }
end

--- Fetch the expensive figures for one collection on demand.
function M.collection_metrics(config, collection)
  local key = cache_key(config, "metrics:" .. collection)
  local cached = cache.get(key)
  if cached then
    return cached
  end
  local figures_data = database_request(config, "GET", collection_path(collection) .. "/figures")
  local figures = figures_data.figures
  return cache.set(key, {
    count = figures_data.count,
    size = collection_size_bytes(figures),
    engine = type(figures) == "table" and figures.engine or figures_data.engine,
  }, cache_ttl())
end

--- Fetch collection figures asynchronously for lazy picker previews.
function M.collection_metrics_async(config, collection, callback)
  local key = cache_key(config, "metrics:" .. collection)
  local cached = cache.get(key)
  if cached then
    vim.schedule(function()
      callback(nil, cached)
    end)
    return { cancel = function() end }
  end
  return database_request_async(config, "GET", collection_path(collection) .. "/figures", nil, function(err, data)
    if err then
      callback(err)
      return
    end
    local figures = data.figures
    callback(
      nil,
      cache.set(key, {
        count = data.count,
        size = collection_size_bytes(figures),
        engine = type(figures) == "table" and figures.engine or data.engine,
      }, cache_ttl())
    )
  end)
end

--- Sample collection documents to extract candidate field paths for filtering.
function M.list_fields(config, collection, sample_size)
  local sample = math.max(tonumber(sample_size) or 100, 1)
  local key = cache_key(config, string.format("fields:%s:%d", collection, sample))
  local cached = cache.get(key)
  if cached then
    return cached
  end
  local query = "FOR doc IN @@collection LIMIT @sample RETURN doc"
  local data = run_aql(config, query, {
    ["@collection"] = collection,
    sample = sample,
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
  return cache.set(key, result, cache_ttl())
end

--- Sample candidate field paths without blocking the editor.
function M.list_fields_async(config, collection, sample_size, callback)
  local sample = math.max(tonumber(sample_size) or 100, 1)
  local key = cache_key(config, string.format("fields:%s:%d", collection, sample))
  local cached = cache.get(key)
  if cached then
    return async.resolved(callback, cached)
  end

  return database_request_async(config, "POST", "/_api/cursor", {
    query = "FOR doc IN @@collection LIMIT @sample RETURN doc",
    bindVars = { ["@collection"] = collection, sample = sample },
    batchSize = sample,
  }, function(err, data)
    if err then
      callback(err)
      return
    end
    if data.hasMore == true and data.id then
      M.close_cursor_async(config, data.id)
    end
    local fields = { _id = true, _key = true, _rev = true }
    for _, document in ipairs(data.result or {}) do
      collect_field_paths(document, "", fields)
    end
    local result = {}
    for field in pairs(fields) do
      result[#result + 1] = field
    end
    table.sort(result)
    callback(nil, cache.set(key, result, cache_ttl()))
  end)
end

--- Fetch a single document and format it for the editor buffer.
function M.get_document(config, document_id)
  local collection, key = split_document_id(document_id)
  local document = get_document_raw(config, collection, key)
  return wrap_document(document, config.database)
end

--- Fetch a single document without blocking Neovim.
function M.get_document_async(config, document_id, callback)
  local collection, key = split_document_id(document_id)
  return database_request_async(config, "GET", document_path(collection, key), nil, function(err, document)
    if err then
      callback(err)
      return
    end
    callback(nil, wrap_document(document, config.database))
  end)
end

--- Replace an existing document and return the refreshed document payload.
function M.save_document(config, document_id, document, opts)
  if type(document) ~= "table" then
    error("Document payload must be a JSON object")
  end

  if type(document_id) ~= "string" or document_id == "" then
    error("Missing document id")
  end

  local collection, key = split_document_id(document_id)
  if document._id ~= nil and document._id ~= document_id then
    error("Document _id cannot be changed")
  end
  if document._key ~= nil and document._key ~= key then
    error("Document _key cannot be changed")
  end

  opts = opts or {}
  local revision = opts.force ~= true and type(document._rev) == "string" and document._rev or nil
  local headers = revision and { ["If-Match"] = vim.json.encode(revision) } or nil
  local saved = database_request(config, "PUT", document_path(collection, key), document, { headers = headers })
  local current = get_document_raw(config, collection, key)
  invalidate_cache(config)

  return {
    database = config.database,
    id = document_id,
    key = key,
    collection = collection,
    meta = saved,
    document = current,
    preview = utils.json_pretty(current),
  }
end

--- Replace and refresh an existing document without blocking Neovim.
function M.save_document_async(config, document_id, document, opts, callback)
  if type(opts) == "function" then
    callback = opts
    opts = {}
  end
  if type(document) ~= "table" then
    error("Document payload must be a JSON object")
  end
  if type(document_id) ~= "string" or document_id == "" then
    error("Missing document id")
  end

  local collection, key = split_document_id(document_id)
  if document._id ~= nil and document._id ~= document_id then
    error("Document _id cannot be changed")
  end
  if document._key ~= nil and document._key ~= key then
    error("Document _key cannot be changed")
  end

  opts = opts or {}
  local revision = opts.force ~= true and type(document._rev) == "string" and document._rev or nil
  local headers = revision and { ["If-Match"] = vim.json.encode(revision) } or nil
  local task, step = async.sequence(callback)
  step(function(done)
    return database_request_async(config, "PUT", document_path(collection, key), document, done, { headers = headers })
  end, function(saved, next_step, finish)
    next_step(function(done)
      return database_request_async(config, "GET", document_path(collection, key), nil, done)
    end, function(current)
      invalidate_cache(config)
      finish(nil, {
        database = config.database,
        id = document_id,
        key = key,
        collection = collection,
        meta = saved,
        document = current,
        preview = utils.json_pretty(current),
      })
    end)
  end)
  return task
end

local function cursor_page_async(config, query, bind_vars, limit, cursor_id, callback)
  if cursor_id then
    return database_request_async(config, "PUT", "/_api/cursor/" .. core.url_encode(cursor_id), nil, callback)
  end

  local payload = {
    query = query,
    bindVars = bind_vars,
    batchSize = limit,
    count = true,
  }
  return database_request_async(config, "POST", "/_api/cursor", payload, callback)
end

local function browse_page(data, config, collection, field, search, limit, related)
  local items = {}
  for _, document in ipairs(data.result or {}) do
    local fields = related and related_field_paths(field) or field
    local value = related and nil or extract_value(document, field)
    items[#items + 1] = {
      key = document._key,
      id = document._id,
      field = fields,
      field_value = value,
      field_value_text = related and related_field_value_text(document, fields) or truncate_text(value),
      preview = utils.json_pretty(document),
    }
  end
  return {
    database = config.database,
    collection = collection,
    field = field,
    search = search,
    limit = limit,
    total_count = data.count,
    has_more = data.hasMore == true,
    cursor_id = data.id,
    items = items,
  }
end

--- Browse one cursor-backed collection page without blocking Neovim.
function M.browse_collection_async(config, collection, field, search, limit, cursor_id, callback)
  field = field or "_key"
  search = search or ""
  limit = math.max(tonumber(limit) or 50, 1)

  local bind_vars = { ["@collection"] = collection }
  local query_lines = { "FOR doc IN @@collection" }
  if search ~= "" then
    bind_vars.search = search:lower()
    query_lines[#query_lines + 1] =
      string.format("FILTER CONTAINS(LOWER(TO_STRING(%s)), @search)", field_expression(field))
  end
  query_lines[#query_lines + 1] = "SORT " .. (plugin_options().default_sort or "doc._key ASC")
  query_lines[#query_lines + 1] = "RETURN doc"

  return cursor_page_async(config, table.concat(query_lines, "\n"), bind_vars, limit, cursor_id, function(err, data)
    if err then
      callback(err)
      return
    end
    local ok, page = pcall(browse_page, data, config, collection, field, search, limit, false)
    if ok then
      callback(nil, page)
    else
      callback(page)
    end
  end)
end

--- Browse one cursor-backed related-document page without blocking Neovim.
function M.browse_related_collection_async(config, collection, field, value, search, limit, cursor_id, callback)
  local values = related_search_values(value)
  local fields = related_field_paths(field)
  search = search or ""
  limit = math.max(tonumber(limit) or 50, 1)

  local bind_vars = {
    ["@collection"] = collection,
    values = values,
  }
  local query_lines = {
    "FOR doc IN @@collection",
    "FILTER " .. related_filter_clause(fields),
  }
  if search ~= "" then
    bind_vars.search = search:lower()
    query_lines[#query_lines + 1] = "FILTER CONTAINS(LOWER(doc._id), @search) OR CONTAINS(LOWER(doc._key), @search)"
  end
  query_lines[#query_lines + 1] = "SORT " .. (plugin_options().default_sort or "doc._key ASC")
  query_lines[#query_lines + 1] = "RETURN doc"

  return cursor_page_async(config, table.concat(query_lines, "\n"), bind_vars, limit, cursor_id, function(err, data)
    if err then
      callback(err)
      return
    end
    local ok, page = pcall(browse_page, data, config, collection, fields, search, limit, true)
    if ok then
      callback(nil, page)
    else
      callback(page)
    end
  end)
end

--- Best-effort cleanup for a cursor abandoned by a picker.
function M.close_cursor_async(config, cursor_id)
  if not cursor_id or cursor_id == "" then
    return
  end
  database_request_async(config, "DELETE", "/_api/cursor/" .. core.url_encode(cursor_id), nil, function() end)
end

local function optional_bind_vars(payload, bind_vars)
  if type(bind_vars) == "table" and not vim.tbl_isempty(bind_vars) then
    payload.bindVars = bind_vars
  end
  return payload
end

--- Validate an AQL query without executing it.
function M.validate_aql_async(config, query, callback)
  return database_request_async(config, "POST", "/_api/query", { query = query }, callback)
end

--- Return the optimized execution plan for an AQL query without executing it.
function M.explain_aql_async(config, query, bind_vars, callback)
  local payload = optional_bind_vars({ query = query }, bind_vars)
  return database_request_async(config, "POST", "/_api/explain", payload, callback)
end

--- Execute an AQL query and return its first cursor page without blocking Neovim.
function M.execute_aql_async(config, query, bind_vars, opts, callback)
  opts = opts or {}
  local payload = optional_bind_vars({
    query = query,
    batchSize = math.max(tonumber(opts.batch_size) or 100, 1),
    count = true,
    ttl = math.max(tonumber(opts.cursor_ttl) or 300, 1),
  }, bind_vars)
  local query_options = {}
  if opts.profile == true then
    query_options.profile = 2
  end
  if type(opts.max_runtime) == "number" then
    query_options.maxRuntime = opts.max_runtime
  end
  if not vim.tbl_isempty(query_options) then
    payload.options = query_options
  end

  return database_request_async(config, "POST", "/_api/cursor", payload, function(err, data)
    if not err and opts.modification == true then
      invalidate_cache(config)
    end
    callback(err, data)
  end)
end

--- Read the next page from an AQL cursor.
function M.next_aql_page_async(config, cursor_id, callback)
  if type(cursor_id) ~= "string" or cursor_id == "" then
    vim.schedule(function()
      callback(errors.new({ kind = "protocol", message = "Missing ArangoDB AQL cursor id" }))
    end)
    return { cancel = function() end }
  end
  return database_request_async(config, "POST", "/_api/cursor/" .. core.url_encode(cursor_id), nil, callback)
end

--- Delete a document by id and return the ArangoDB response metadata.
function M.delete_document(config, document_id)
  local collection, key = split_document_id(document_id)
  local deleted = database_request(config, "DELETE", document_path(collection, key))
  invalidate_cache(config)

  return {
    database = config.database,
    id = document_id,
    key = key,
    collection = collection,
    meta = deleted,
  }
end

--- Delete a document without blocking Neovim.
function M.delete_document_async(config, document_id, callback)
  local collection, key = split_document_id(document_id)
  return database_request_async(config, "DELETE", document_path(collection, key), nil, function(err, deleted)
    if err then
      callback(err)
      return
    end
    invalidate_cache(config)
    callback(nil, {
      database = config.database,
      id = document_id,
      key = key,
      collection = collection,
      meta = deleted,
    })
  end)
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
  payload._rev = nil

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

local collection_create_properties = {
  "cacheEnabled",
  "computedValues",
  "distributeShardsLike",
  "keyOptions",
  "numberOfShards",
  "replicationFactor",
  "schema",
  "shardKeys",
  "shardingStrategy",
  "waitForSync",
  "writeConcern",
}

local index_definition_properties = {
  "cacheEnabled",
  "deduplicate",
  "estimates",
  "expireAfter",
  "fields",
  "geoJson",
  "inBackground",
  "name",
  "sparse",
  "storedValues",
  "type",
  "unique",
}

local function copy_properties(source, names)
  local result = {}
  for _, name in ipairs(names) do
    if source[name] ~= nil and source[name] ~= vim.NIL then
      result[name] = vim.deepcopy(source[name])
    end
  end
  return result
end

local function collection_create_payload(collection, type_code, properties)
  return vim.tbl_extend(
    "force",
    { name = collection, type = type_code },
    copy_properties(properties or {}, collection_create_properties)
  )
end

local function copyable_indexes(indexes)
  local result = {}
  for _, index in ipairs(indexes or {}) do
    if type(index) == "table" and index.type ~= "primary" and index.type ~= "edge" then
      result[#result + 1] = copy_properties(index, index_definition_properties)
    end
  end
  return result
end

--- Return all mutable and immutable properties reported for a collection.
function M.collection_properties(config, collection)
  return database_request(config, "GET", collection_path(collection) .. "/properties")
end

--- Return collection properties without blocking Neovim.
function M.collection_properties_async(config, collection, callback)
  return database_request_async(config, "GET", collection_path(collection) .. "/properties", nil, callback)
end

--- Update the mutable subset of collection properties.
function M.update_collection_properties(config, collection, properties)
  if type(properties) ~= "table" then
    error("Collection properties must be a JSON object")
  end
  local result = database_request(config, "PUT", collection_path(collection) .. "/properties", properties)
  invalidate_cache(config)
  return result
end

--- Update collection properties without blocking Neovim.
function M.update_collection_properties_async(config, collection, properties, callback)
  if type(properties) ~= "table" then
    error("Collection properties must be a JSON object")
  end
  return database_request_async(
    config,
    "PUT",
    collection_path(collection) .. "/properties",
    properties,
    function(err, result)
      if not err then
        invalidate_cache(config)
      end
      callback(err, result)
    end
  )
end

--- List all indexes for a collection.
function M.list_indexes(config, collection)
  local data = database_request(config, "GET", "/_api/index?collection=" .. core.url_encode(collection))
  return data.indexes or {}
end

--- List indexes without blocking Neovim.
function M.list_indexes_async(config, collection, callback)
  return database_request_async(
    config,
    "GET",
    "/_api/index?collection=" .. core.url_encode(collection),
    nil,
    function(err, data)
      callback(err, not err and (data.indexes or {}) or nil)
    end
  )
end

--- Create an index from an ArangoDB index definition.
function M.create_index(config, collection, definition)
  if type(definition) ~= "table" then
    error("Index definition must be a JSON object")
  end
  local result = database_request(
    config,
    "POST",
    "/_api/index?collection=" .. core.url_encode(collection),
    copy_properties(definition, index_definition_properties)
  )
  invalidate_cache(config)
  return result
end

--- Create an index without blocking Neovim.
function M.create_index_async(config, collection, definition, callback)
  if type(definition) ~= "table" then
    error("Index definition must be a JSON object")
  end
  return database_request_async(
    config,
    "POST",
    "/_api/index?collection=" .. core.url_encode(collection),
    copy_properties(definition, index_definition_properties),
    function(err, result)
      if not err then
        invalidate_cache(config)
      end
      callback(err, result)
    end
  )
end

--- Delete a non-system index.
function M.delete_index_async(config, index_id, callback)
  return database_request_async(config, "DELETE", index_path(index_id), nil, function(err, result)
    if not err then
      invalidate_cache(config)
    end
    callback(err, result)
  end)
end

--- Insert a new document and return the created payload formatted for editing.
function M.create_document(config, collection, document)
  local target_collection, key, payload = sanitize_new_document(collection, document)
  local created = database_request(config, "POST", "/_api/document/" .. core.url_encode(target_collection), payload)
  local current = get_document_raw(config, target_collection, created._key or key)
  invalidate_cache(config)

  return {
    database = config.database,
    id = current._id,
    key = current._key,
    collection = target_collection,
    meta = created,
    document = current,
    preview = utils.json_pretty(current),
  }
end

--- Insert and refresh a document without blocking Neovim.
function M.create_document_async(config, collection, document, callback)
  local target_collection, key, payload = sanitize_new_document(collection, document)
  local task, step = async.sequence(callback)
  step(function(done)
    return database_request_async(
      config,
      "POST",
      "/_api/document/" .. core.url_encode(target_collection),
      payload,
      done
    )
  end, function(created, next_step, finish)
    next_step(function(done)
      return database_request_async(config, "GET", document_path(target_collection, created._key or key), nil, done)
    end, function(current)
      invalidate_cache(config)
      finish(nil, {
        database = config.database,
        id = current._id,
        key = current._key,
        collection = target_collection,
        meta = created,
        document = current,
        preview = utils.json_pretty(current),
      })
    end)
  end)
  return task
end

--- Create a collection and normalize the type label for the UI.
function M.create_collection(config, collection, collection_type, properties)
  collection = vim.trim(collection or "")
  if collection == "" then
    error("Missing collection name")
  end

  local normalized_type, type_code = collection_type_code(collection_type)
  local created =
    database_request(config, "POST", "/_api/collection", collection_create_payload(collection, type_code, properties))
  invalidate_cache(config)

  return {
    database = config.database,
    name = created.name or collection,
    type = normalized_type,
    collection = created,
  }
end

--- Create a collection without blocking Neovim.
function M.create_collection_async(config, collection, collection_type, properties, callback)
  if type(properties) == "function" then
    callback = properties
    properties = nil
  end
  collection = vim.trim(collection or "")
  if collection == "" then
    error("Missing collection name")
  end
  local normalized_type, type_code = collection_type_code(collection_type)
  return database_request_async(
    config,
    "POST",
    "/_api/collection",
    collection_create_payload(collection, type_code, properties),
    function(err, created)
      if err then
        callback(err)
        return
      end
      invalidate_cache(config)
      callback(nil, {
        database = config.database,
        name = created.name or collection,
        type = normalized_type,
        collection = created,
      })
    end
  )
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
  invalidate_cache(config)

  return {
    database = config.database,
    old_name = collection,
    name = renamed.name or new_name,
    collection = renamed,
  }
end

--- Rename a collection without blocking Neovim.
function M.rename_collection_async(config, collection, new_name, callback)
  new_name = vim.trim(new_name or "")
  if new_name == "" then
    error("Missing new collection name")
  end
  if new_name == collection then
    error("The new collection name must be different")
  end
  return database_request_async(
    config,
    "PUT",
    collection_path(collection) .. "/rename",
    { name = new_name },
    function(err, renamed)
      if err then
        callback(err)
        return
      end
      invalidate_cache(config)
      callback(nil, {
        database = config.database,
        old_name = collection,
        name = renamed.name or new_name,
        collection = renamed,
      })
    end
  )
end

--- Remove every document from a collection without deleting the collection itself.
function M.truncate_collection(config, collection)
  local truncated = database_request(config, "PUT", collection_path(collection) .. "/truncate?compact=false")
  invalidate_cache(config)

  return {
    database = config.database,
    name = truncated.name or collection,
    collection = truncated,
  }
end

--- Truncate a collection without blocking Neovim.
function M.truncate_collection_async(config, collection, callback)
  return database_request_async(
    config,
    "PUT",
    collection_path(collection) .. "/truncate?compact=false",
    nil,
    function(err, truncated)
      if err then
        callback(err)
        return
      end
      invalidate_cache(config)
      callback(nil, {
        database = config.database,
        name = truncated.name or collection,
        collection = truncated,
      })
    end
  )
end

--- Delete a collection. This internal helper is also used by integration cleanup.
function M.delete_collection(config, collection)
  local deleted = database_request(config, "DELETE", collection_path(collection))
  invalidate_cache(config)

  return {
    database = config.database,
    name = collection,
    collection = deleted,
  }
end

--- Delete a collection without blocking Neovim.
function M.delete_collection_async(config, collection, callback)
  return database_request_async(config, "DELETE", collection_path(collection), nil, function(err, deleted)
    if err then
      callback(err)
      return
    end
    invalidate_cache(config)
    callback(nil, {
      database = config.database,
      name = collection,
      collection = deleted,
    })
  end)
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

  local properties = M.collection_properties(config, source)
  local indexes = copyable_indexes(M.list_indexes(config, source))
  local created = M.create_collection(config, target, collection_type, properties)
  local target_name = created.name or target

  local copied, copy_error = pcall(function()
    for _, definition in ipairs(indexes) do
      M.create_index(config, target_name, definition)
    end
    run_aql(
      config,
      table.concat({
        "FOR doc IN @@source",
        'INSERT UNSET(doc, "_id", "_rev") INTO @@target',
      }, "\n"),
      {
        ["@source"] = source,
        ["@target"] = target_name,
      }
    )
  end)
  if not copied then
    local cleaned, cleanup_error = pcall(M.delete_collection, config, target_name)
    if not cleaned then
      error(string.format("%s (cleanup also failed: %s)", tostring(copy_error), tostring(cleanup_error)), 0)
    end
    error(copy_error, 0)
  end

  return {
    database = config.database,
    source = source,
    name = target_name,
    type = collection_type,
    copied_count = get_collection_count(config, target_name),
    copied_indexes = #indexes,
    collection = created.collection,
  }
end

--- Duplicate documents, properties, and non-system indexes without blocking Neovim.
function M.duplicate_collection_async(config, source, target, callback)
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

  local active
  local cancelled = false
  local cleaning = false
  local finished = false
  local target_name
  local task = {}

  local function complete(err, value, report_cancelled)
    if finished or (cancelled and not report_cancelled) then
      return
    end
    finished = true
    active = nil
    callback(err, value)
  end

  local function cleanup(original_error, cancellation)
    if cleaning or finished then
      return
    end
    if not target_name then
      if not cancellation then
        complete(original_error)
      end
      return
    end

    cleaning = true
    local cleanup_completed = false
    local cleanup_ok, cleanup_handle = pcall(M.delete_collection_async, config, target_name, function(cleanup_error)
      cleanup_completed = true
      active = nil
      cleaning = false
      if cleanup_error then
        complete(
          string.format("%s (cleanup also failed: %s)", tostring(original_error), tostring(cleanup_error)),
          nil,
          cancellation or cancelled
        )
      elseif cancellation then
        finished = true
      else
        complete(original_error)
      end
    end)
    if not cleanup_ok then
      cleaning = false
      complete(
        string.format("%s (cleanup also failed: %s)", tostring(original_error), tostring(cleanup_handle)),
        nil,
        cancellation
      )
    elseif not cleanup_completed then
      active = cleanup_handle
    end
  end

  function task.cancel()
    if cancelled or finished then
      return
    end
    cancelled = true
    if cleaning then
      return
    end
    if active and active.cancel then
      active.cancel()
    end
    active = nil
    if target_name then
      cleanup("Collection duplication cancelled", true)
    end
  end

  local function request_step(starter, on_success)
    if cancelled or cleaning or finished then
      return
    end
    local ok, handle = pcall(starter, function(err, value)
      active = nil
      if cancelled or finished then
        return
      end
      if err then
        cleanup(err)
        return
      end
      local next_ok, next_err = pcall(on_success, value)
      if not next_ok then
        cleanup(next_err)
      end
    end)
    if not ok then
      cleanup(handle)
    else
      active = handle
    end
  end

  local source_collection
  local properties
  local indexes
  request_step(function(done)
    return database_request_async(config, "GET", collection_path(source), nil, done)
  end, function(value)
    source_collection = value
    request_step(function(done)
      return M.collection_properties_async(config, source, done)
    end, function(value_properties)
      properties = value_properties
      request_step(function(done)
        return M.list_indexes_async(config, source, done)
      end, function(value_indexes)
        indexes = copyable_indexes(value_indexes)
        local collection_type = collection_type_label(source_collection.type)
        if collection_type ~= "document" and collection_type ~= "edge" then
          error("Unsupported collection type: " .. tostring(source_collection.type))
        end
        request_step(function(done)
          return M.create_collection_async(config, target, collection_type, properties, done)
        end, function(created)
          target_name = created.name or target
          local index_number = 0
          local function create_next_index()
            index_number = index_number + 1
            local definition = indexes[index_number]
            if definition then
              request_step(function(done)
                return M.create_index_async(config, target_name, definition, done)
              end, create_next_index)
              return
            end
            request_step(function(done)
              return M.execute_aql_async(
                config,
                table.concat({
                  "FOR doc IN @@source",
                  'INSERT UNSET(doc, "_id", "_rev") INTO @@target',
                }, "\n"),
                {
                  ["@source"] = source,
                  ["@target"] = target_name,
                },
                {
                  batch_size = plugin_options().aql_batch_size or 1000,
                  modification = true,
                },
                done
              )
            end, function(copy_result)
              if copy_result.hasMore == true and copy_result.id then
                M.close_cursor_async(config, copy_result.id)
              end
              request_step(function(done)
                return database_request_async(config, "GET", collection_path(target_name) .. "/count", nil, done)
              end, function(count)
                complete(nil, {
                  database = config.database,
                  source = source,
                  name = target_name,
                  type = collection_type,
                  copied_count = count.count or 0,
                  copied_indexes = #indexes,
                  collection = created.collection,
                })
              end)
            end)
          end
          create_next_index()
        end)
      end)
    end)
  end)
  return task
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
      "SORT " .. (plugin_options().default_sort or "doc._key ASC"),
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

--- Search reverse relations across one or more collections without blocking Neovim.
function M.search_related_async(config, field, value, limit, collection, callback)
  local values = related_search_values(value)
  local fields = related_field_paths(field)
  local filter_clause = related_filter_clause(fields)
  local batch_size = math.max(tonumber(limit) or 20, 1)
  local task, step, finish = async.sequence(callback)
  local matches = {}
  local seen = {}

  local function search_collections(collections)
    local index = 0
    local function search_next()
      index = index + 1
      local collection_name = collections[index]
      if not collection_name then
        finish(nil, { matches = matches })
        return
      end

      local query = table.concat({
        "FOR doc IN @@collection",
        "FILTER " .. filter_clause,
        "SORT " .. (plugin_options().default_sort or "doc._key ASC"),
        "LIMIT @limit",
        "RETURN doc",
      }, "\n")
      step(function(done)
        return database_request_async(config, "POST", "/_api/cursor", {
          query = query,
          bindVars = {
            ["@collection"] = collection_name,
            values = values,
            limit = batch_size,
          },
          batchSize = batch_size,
        }, done)
      end, function(data)
        if data.hasMore == true and data.id then
          M.close_cursor_async(config, data.id)
        end
        for _, document in ipairs(data.result or {}) do
          if document._id and not seen[document._id] then
            seen[document._id] = true
            matches[#matches + 1] = wrap_document(document, config.database)
          end
        end
        search_next()
      end)
    end
    search_next()
  end

  if collection then
    search_collections({ collection })
  else
    step(function(done)
      return M.list_collections_async(config, done)
    end, search_collections)
  end
  return task
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
    "SORT " .. (plugin_options().default_sort or "doc._key ASC"),
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
      preview = utils.json_pretty(document),
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
    "SORT " .. (plugin_options().default_sort or "doc._key ASC"),
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
      preview = utils.json_pretty(document),
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
