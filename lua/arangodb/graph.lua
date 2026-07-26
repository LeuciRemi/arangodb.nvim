--- Bounded, interactive named-graph explorer.
local M = {}

local ui = require("arangodb.browser.ui")
local client = require("arangodb.client")
local core = require("arangodb.core")

local sessions = {}

local function graph_options()
  return require("arangodb.config").get().graph or {}
end

local function graph_keymaps()
  return require("arangodb.config").get().graph_keymaps or {}
end

local function notify_error(err)
  core.notify_error(err, "ArangoDB Graph")
end

local function node_label(vertex)
  for _, field in ipairs({ "name", "title", "label", "_key", "_id" }) do
    if vertex[field] ~= nil then
      return tostring(vertex[field])
    end
  end
  return "?"
end

local function set_lines(buf, lines)
  vim.bo[buf].modifiable = true
  vim.bo[buf].readonly = false
  vim.api.nvim_buf_set_lines(buf, 0, -1, false, lines)
  vim.bo[buf].modified = false
  vim.bo[buf].modifiable = false
  vim.bo[buf].readonly = true
end

local function selected_vertex(session)
  local line = vim.api.nvim_win_get_cursor(0)[1]
  return session.line_vertices[line]
end

local function open_document(session)
  local vertex = selected_vertex(session)
  if not vertex or type(vertex._id) ~= "string" then
    vim.notify("Select a graph vertex first", vim.log.levels.INFO)
    return
  end
  require("arangodb.browser").open({ kind = "document", config = session.config, id = vertex._id })
end

local explore

local function prompt_depth(session)
  vim.ui.input({ prompt = "Traversal depth (1-10): ", default = tostring(session.depth) }, function(value)
    local depth = tonumber(value)
    if not depth or depth < 1 or depth > 10 or depth % 1 ~= 0 then
      if value ~= nil then
        notify_error("Graph depth must be an integer between 1 and 10")
      end
      return
    end
    session.depth = depth
    explore(session)
  end)
end

local function prompt_root(session, default)
  vim.ui.input({ prompt = "Start vertex id (collection/key): ", default = default or session.start }, function(value)
    value = type(value) == "string" and vim.trim(value) or ""
    if value == "" then
      return
    end
    if not value:match("^[^/]+/.+$") then
      notify_error("Start vertex must be a document id such as users/alice")
      return
    end
    session.start = value
    explore(session)
  end)
end

local function render(session, data)
  local lines = {
    string.format("# ArangoDB graph: %s", session.graph.name),
    "",
    string.format("Database: %s", session.config.database),
    string.format("Start: %s", session.start),
    string.format("Direction: %s", session.direction),
    string.format("Depth: %d", session.depth),
    string.format("Limit: %d", session.limit),
    "",
    "Keys: " .. table.concat(
      vim.tbl_filter(function(value)
        return value ~= nil
      end, {
        graph_keymaps().open and (graph_keymaps().open .. " open document") or nil,
        graph_keymaps().start and (graph_keymaps().start .. " traverse from vertex") or nil,
        graph_keymaps().refresh and (graph_keymaps().refresh .. " refresh") or nil,
        graph_keymaps().depth and (graph_keymaps().depth .. " depth") or nil,
        graph_keymaps().direction and (graph_keymaps().direction .. " direction") or nil,
      }),
      " · "
    ),
    "",
    "## Vertices",
  }
  session.line_vertices = {}
  local seen_edges = {}
  local edges = {}
  for _, item in ipairs(data.result or {}) do
    local vertex = item.vertex
    if type(vertex) == "table" and type(vertex._id) == "string" then
      local prefix = string.rep("  ", tonumber(item.depth) or 0)
      lines[#lines + 1] = string.format("%s- [%d] %s — %s", prefix, item.depth or 0, vertex._id, node_label(vertex))
      session.line_vertices[#lines] = vertex
    end
    local edge = item.edge
    if type(edge) == "table" and type(edge._id) == "string" and not seen_edges[edge._id] then
      seen_edges[edge._id] = true
      edges[#edges + 1] = edge
    end
  end
  lines[#lines + 1] = ""
  lines[#lines + 1] = "## Edges"
  for _, edge in ipairs(edges) do
    lines[#lines + 1] = string.format("- %s → %s  (%s)", tostring(edge._from), tostring(edge._to), edge._id)
  end
  if #(data.result or {}) == 0 then
    lines[#lines + 1] = ""
    lines[#lines + 1] = "No reachable vertices found."
  end
  set_lines(session.buf, lines)
end

explore = function(session)
  if session.request and session.request.cancel then
    session.request.cancel()
  end
  session.generation = session.generation + 1
  local generation = session.generation
  local completed = false
  local ok, handle = pcall(client.traverse_graph_async, session.config, session.graph.name, session.start, {
    direction = session.direction,
    depth = session.depth,
    limit = session.limit,
  }, function(err, data)
    completed = true
    if generation ~= session.generation or not vim.api.nvim_buf_is_valid(session.buf) then
      return
    end
    session.request = nil
    if err then
      notify_error(err)
      return
    end
    if data.hasMore == true and data.id then
      client.close_cursor_async(session.config, data.id)
    end
    render(session, data)
  end)
  if not ok then
    notify_error(handle)
  elseif not completed then
    session.request = handle
  end
end

local function create_session(config, graph, start, opts)
  local graph_opts = graph_options()
  local buf = vim.api.nvim_create_buf(true, true)
  local session = {
    buf = buf,
    config = vim.deepcopy(config),
    graph = graph,
    start = start,
    depth = math.min(math.max(tonumber(opts.depth or graph_opts.depth) or 2, 1), 10),
    direction = (opts.direction or graph_opts.direction or "ANY"):upper(),
    limit = math.max(math.floor(tonumber(opts.limit or graph_opts.max_nodes) or 100), 1),
    generation = 0,
    line_vertices = {},
  }
  sessions[buf] = session
  pcall(vim.api.nvim_buf_set_name, buf, string.format("arangodb-graph://%s/%s", config.database, graph.name))
  vim.bo[buf].buftype = "nofile"
  vim.bo[buf].bufhidden = "wipe"
  vim.bo[buf].swapfile = false
  vim.bo[buf].filetype = "markdown"
  vim.bo[buf].modifiable = false
  vim.bo[buf].readonly = true
  vim.b[buf].arangodb_graph = true
  vim.b[buf].arangodb_database = config.database
  vim.api.nvim_set_current_buf(buf)

  local maps = graph_keymaps()
  local function map(lhs, callback, desc)
    if type(lhs) == "string" and lhs ~= "" then
      vim.keymap.set("n", lhs, callback, { buffer = buf, desc = desc })
    end
  end
  map(maps.open, function()
    open_document(session)
  end, "Open Graph Vertex")
  map(maps.start, function()
    local vertex = selected_vertex(session)
    if vertex then
      session.start = vertex._id
      explore(session)
    end
  end, "Traverse From Graph Vertex")
  map(maps.refresh, function()
    explore(session)
  end, "Refresh Graph")
  map(maps.depth, function()
    prompt_depth(session)
  end, "Change Graph Depth")
  map(maps.direction, function()
    local next_direction = { ANY = "OUTBOUND", OUTBOUND = "INBOUND", INBOUND = "ANY" }
    session.direction = next_direction[session.direction] or "ANY"
    explore(session)
  end, "Cycle Graph Direction")
  vim.api.nvim_buf_create_user_command(buf, "ArangoGraphRefresh", function()
    explore(session)
  end, { desc = "Refresh the graph neighborhood" })
  vim.api.nvim_buf_create_user_command(buf, "ArangoGraphRoot", function()
    prompt_root(session)
  end, { desc = "Choose another graph start vertex" })
  vim.api.nvim_buf_create_user_command(buf, "ArangoGraphDepth", function()
    prompt_depth(session)
  end, { desc = "Change graph traversal depth" })
  vim.api.nvim_create_autocmd("BufWipeout", {
    buffer = buf,
    once = true,
    callback = function()
      session.generation = session.generation + 1
      if session.request and session.request.cancel then
        session.request.cancel()
      end
      sessions[buf] = nil
    end,
  })
  explore(session)
  return session
end

local function choose_start(config, graph, opts)
  if type(opts.start) == "string" and opts.start ~= "" then
    return create_session(config, graph, opts.start, opts)
  end
  vim.ui.input({ prompt = string.format("Start vertex for %s (collection/key): ", graph.name) }, function(start)
    start = type(start) == "string" and vim.trim(start) or ""
    if start ~= "" then
      create_session(config, graph, start, opts)
    end
  end)
end

local function choose_graph(config, opts)
  client.list_graphs_async(config, function(err, graphs)
    if err then
      notify_error(err)
      return
    end
    if #graphs == 0 then
      vim.notify("No named graphs in " .. config.database, vim.log.levels.INFO)
      return
    end
    if type(opts.graph) == "string" then
      for _, graph in ipairs(graphs) do
        if graph.name == opts.graph then
          choose_start(config, graph, opts)
          return
        end
      end
      notify_error("Named graph not found: " .. opts.graph)
      return
    end
    vim.ui.select(
      graphs,
      ui.select_options({
        prompt = "Named graphs (" .. config.database .. ")",
        format_item = function(graph)
          return string.format("%s  (%d edge definitions)", graph.name, #(graph.edgeDefinitions or {}))
        end,
      }),
      function(graph)
        if graph then
          choose_start(config, graph, opts)
        end
      end
    )
  end)
end

local function open_item(item, opts)
  local ok, config = pcall(core.resolve_connection, item)
  if not ok then
    notify_error(config)
  elseif config then
    choose_graph(config, opts)
  else
    notify_error("Invalid ArangoDB connection URL for " .. tostring(item.name or "selected connection"))
  end
end

--- Select a database and named graph, then explore a bounded neighborhood.
function M.open(opts)
  opts = opts or {}
  if type(opts.config) == "table" then
    return choose_graph(opts.config, opts)
  end
  if type(opts.database) == "string" and opts.database ~= "" then
    return open_item(core.find_database(opts.database) or {
      name = opts.database,
      url = core.arango_url(opts.database),
    }, opts)
  end
  local items = core.available_databases()
  if #items == 0 then
    notify_error("No ArangoDB database configured")
    return
  end
  vim.ui.select(
    items,
    ui.select_options({
      prompt = "Arango database for graph exploration",
      format_item = function(item)
        return item.name
      end,
    }),
    function(item)
      if item then
        open_item(item, opts)
      end
    end
  )
end

--- Return an explorer session for tests and integrations.
function M.session(buf)
  return sessions[buf]
end

return M
