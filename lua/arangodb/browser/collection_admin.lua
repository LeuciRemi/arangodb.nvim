--- Collection properties, schema, and index management UI.
local M = {}

local client = require("arangodb.client")
local editor = require("arangodb.browser.json_editor")
local ui = require("arangodb.browser.ui")

local mutable_properties = {
  "cacheEnabled",
  "computedValues",
  "replicationFactor",
  "schema",
  "waitForSync",
  "writeConcern",
}

local function select_properties(properties)
  local result = {}
  for _, name in ipairs(mutable_properties) do
    if properties[name] ~= nil then
      result[name] = vim.deepcopy(properties[name])
    end
  end
  return result
end

local function request(title, starter, callback)
  local ok, handle = pcall(starter, function(err, value)
    if err then
      require("arangodb.core").notify_error(err, title)
    elseif callback then
      callback(value)
    end
  end)
  if not ok then
    require("arangodb.core").notify_error(handle, title)
    return nil
  end
  return handle
end

--- Open the mutable collection properties and JSON Schema editor.
function M.edit_properties(config, collection, on_change)
  return request("ArangoDB Collection Properties", function(done)
    return client.collection_properties_async(config, collection, done)
  end, function(properties)
    editor.open({
      config = config,
      name = string.format("arangodb-collection-properties://%s/%s", config.database, collection),
      title = "ArangoDB Collection Properties",
      value = select_properties(properties),
      on_save = function(value, done)
        return client.update_collection_properties_async(config, collection, select_properties(value), done)
      end,
      normalize = function(saved)
        return select_properties(saved)
      end,
      success_message = string.format("Collection %s properties updated", collection),
      on_success = on_change,
    })
  end)
end

local function index_label(index)
  local fields = type(index.fields) == "table" and table.concat(index.fields, ", ") or ""
  local flags = {}
  if index.unique == true then
    flags[#flags + 1] = "unique"
  end
  if index.sparse == true then
    flags[#flags + 1] = "sparse"
  end
  local suffix = #flags > 0 and (" [" .. table.concat(flags, ", ") .. "]") or ""
  return string.format("%s  %s  %s%s", index.type or "?", index.name or index.id or "?", fields, suffix)
end

local function create_index(config, collection, on_change)
  editor.open({
    config = config,
    name = string.format("arangodb-index-draft://%s/%s", config.database, collection),
    title = "ArangoDB Create Index",
    value = {
      type = "persistent",
      fields = {},
      unique = false,
      sparse = false,
      inBackground = true,
    },
    on_save = function(value, done)
      return client.create_index_async(config, collection, value, done)
    end,
    normalize = function(saved, value)
      return saved or value
    end,
    success_message = string.format("Index created on %s", collection),
    on_success = on_change,
  })
end

local function delete_index(config, collection, index, on_change, on_back)
  if index.type == "primary" or index.type == "edge" then
    vim.notify("ArangoDB system indexes cannot be deleted", vim.log.levels.WARN)
    M.manage_indexes(config, collection, on_change, on_back)
    return
  end
  if
    vim.fn.confirm(
      string.format("Delete index %s from %s/%s?", index.name or index.id, config.database, collection),
      "&Delete\n&Cancel",
      2
    ) ~= 1
  then
    M.manage_indexes(config, collection, on_change, on_back)
    return
  end
  request("ArangoDB Delete Index", function(done)
    return client.delete_index_async(config, index.id, done)
  end, function()
    vim.notify("Index deleted", vim.log.levels.INFO)
    if on_change then
      on_change()
    end
    M.manage_indexes(config, collection, on_change, on_back)
  end)
end

local function inspect_index_json(config, collection, index)
  editor.open({
    config = config,
    name = string.format("arangodb-index://%s/%s/%s", config.database, collection, index.id or index.name),
    title = "ArangoDB Index",
    value = index,
    readonly = true,
  })
end

local function inspect_index(config, collection, index, on_change, on_back)
  vim.ui.select(
    { "Inspect JSON", "Delete index" },
    ui.select_options({
      prompt = index_label(index),
    }),
    function(action)
      if action == "Inspect JSON" then
        inspect_index_json(config, collection, index)
        return
      end
      if action == "Delete index" then
        delete_index(config, collection, index, on_change, on_back)
      end
    end
  )
end

--- Browse indexes and create, inspect, or delete them.
function M.manage_indexes(config, collection, on_change, on_back)
  return request("ArangoDB Indexes", function(done)
    return client.list_indexes_async(config, collection, done)
  end, function(indexes)
    local items = { { create = true } }
    local keymaps = require("arangodb.config").get().picker_keymaps or {}
    local execute = keymaps.execute
    local input_execute = ui.picker_key(execute, "arango_action_menu", { "n", "i" }, "Actions")
    local list_execute = ui.picker_key(execute, "arango_action_menu", { "n" }, "Actions")
    vim.list_extend(items, indexes)

    local function open_action_menu(picker)
      local current = picker:current()
      local index = current and current.item
      local choices = {}
      if index and not index.create then
        choices[#choices + 1] = { label = "Inspect JSON", action = "inspect" }
        choices[#choices + 1] = { label = "Delete index", action = "delete" }
      end
      choices[#choices + 1] = { label = "Create index", action = "create" }

      picker.opts.on_close = nil
      picker:close()
      vim.schedule(function()
        vim.ui.select(
          choices,
          ui.select_options({
            prompt = string.format("Index actions (%s/%s)", config.database, collection),
            format_item = function(choice)
              return choice.label
            end,
          }),
          function(choice)
            if not choice then
              M.manage_indexes(config, collection, on_change, on_back)
              return
            end
            vim.schedule(function()
              if choice.action == "create" then
                create_index(config, collection, on_change)
              elseif choice.action == "inspect" and index then
                inspect_index_json(config, collection, index)
              elseif choice.action == "delete" and index then
                delete_index(config, collection, index, on_change, on_back)
              end
            end)
          end
        )
      end)
    end

    vim.ui.select(
      items,
      ui.select_options({
        prompt = string.format("Indexes (%s/%s)", config.database, collection),
        format_item = function(item)
          return item.create and "+ Create index" or index_label(item)
        end,
        snacks = {
          auto_close = false,
          actions = {
            arango_action_menu = open_action_menu,
          },
          win = {
            input = { keys = input_execute },
            list = { keys = list_execute },
          },
        },
      }),
      function(choice)
        if not choice then
          if on_back then
            on_back()
          end
          return
        end
        if choice.create then
          create_index(config, collection, on_change)
        else
          inspect_index(config, collection, choice, on_change, on_back)
        end
      end
    )
  end)
end

return M
