local h = require("tests.helpers")

local config = {
  scheme = "http",
  host = "localhost",
  port = 8529,
  database = "completion-test",
}

local function scratch_buffer(lines)
  local buf = vim.api.nvim_create_buf(false, true)
  vim.api.nvim_buf_set_lines(buf, 0, -1, false, lines)
  vim.api.nvim_set_current_buf(buf)
  return buf
end

local function find(items, word)
  for _, item in ipairs(items) do
    if item.word == word then
      return item
    end
  end
end

local function has_kind(items, kind)
  for _, item in ipairs(items) do
    if item.kind == kind then
      return true
    end
  end
  return false
end

return {
  h.test("AQL completion offers keywords and functions by case-insensitive prefix", function()
    local buf = scratch_buffer({ "FILTER" })
    package.loaded["arangodb.aql_completion"] = nil
    local completion = require("arangodb.aql_completion")

    local keyword = find(completion.omnifunc(0, "fil"), "FILTER")
    h.eq("k", keyword and keyword.kind)
    local function_name = find(completion.omnifunc(0, "Len"), "LENGTH")
    h.eq("f", function_name and function_name.kind)

    vim.api.nvim_buf_set_lines(buf, 0, -1, false, { "RETURN us" })
    vim.api.nvim_win_set_cursor(0, { 1, 8 })
    h.eq(7, completion.omnifunc(1, ""))

    vim.b[buf].arangodb_aql_bind_vars = true
    h.eq(0, #completion.omnifunc(0, ""))
    pcall(vim.api.nvim_buf_delete, buf, { force = true })
  end),

  h.test("AQL completion loads session collections asynchronously", function()
    local original_client = package.loaded["arangodb.client"]
    local original_aql = package.loaded["arangodb.aql"]
    local fetches = {}
    package.loaded["arangodb.client"] = {
      list_collections_async = function(_, done)
        fetches[#fetches + 1] = done
        return { cancel = function() end }
      end,
    }
    package.loaded["arangodb.aql"] = nil
    package.loaded["arangodb.aql_completion"] = nil

    local ok, err = xpcall(function()
      require("arangodb.aql").open({ config = config, connection = "completion", query = "RETURN 1" })
      local completion = require("arangodb.aql_completion")

      h.eq(false, has_kind(completion.omnifunc(0, ""), "c"))
      h.eq(1, #fetches)
      completion.omnifunc(0, "")
      h.eq(1, #fetches)

      fetches[1]("connection refused")
      local retried = completion.omnifunc(0, "")
      h.eq(2, #fetches)
      h.eq(false, has_kind(retried, "c"))

      fetches[2](nil, { "users", "items" })
      local items = completion.omnifunc(0, "us")
      h.eq("c", (find(items, "users") or {}).kind)

      -- Loaded names are revalidated: a dropped collection disappears once the refresh lands.
      fetches[3](nil, { "items" })
      h.eq(nil, find(completion.omnifunc(0, "us"), "users"))
      h.eq("k", (find(completion.omnifunc(0, ""), "FILTER") or {}).kind)

      for _, buf in ipairs(vim.api.nvim_list_bufs()) do
        if vim.api.nvim_buf_is_valid(buf) and (vim.b[buf].arangodb_aql or vim.b[buf].arangodb_aql_query_buf) then
          pcall(vim.api.nvim_buf_delete, buf, { force = true })
        end
      end
      pcall(vim.cmd, "tabonly!")
      pcall(vim.cmd, "only")
    end, debug.traceback)

    package.loaded["arangodb.aql"] = original_aql
    package.loaded["arangodb.client"] = original_client
    if not ok then
      error(err, 0)
    end
  end),
}
