local h = require("tests.helpers")

local config = {
  scheme = "http",
  host = "localhost",
  port = 8529,
  database = "test",
}

local function async(value, done)
  local cancelled = false
  vim.schedule(function()
    if not cancelled then
      done(nil, vim.deepcopy(value))
    end
  end)
  return {
    cancel = function()
      cancelled = true
    end,
  }
end

local function listed_unnamed_buffers()
  local count = 0
  for _, buf in ipairs(vim.api.nvim_list_bufs()) do
    if vim.api.nvim_buf_is_valid(buf) and vim.bo[buf].buflisted and vim.api.nvim_buf_get_name(buf) == "" then
      count = count + 1
    end
  end
  return count
end

local function with_aql(client, callback)
  local original_client = package.loaded["arangodb.client"]
  local original_aql = package.loaded["arangodb.aql"]
  require("arangodb.config").setup({ aql = { history = { enabled = false } } })
  package.loaded["arangodb.client"] = client
  package.loaded["arangodb.aql"] = nil

  local ok, err = xpcall(function()
    callback(require("arangodb.aql"))
  end, debug.traceback)

  for _, buf in ipairs(vim.api.nvim_list_bufs()) do
    if vim.api.nvim_buf_is_valid(buf) and (vim.b[buf].arangodb_aql or vim.b[buf].arangodb_aql_query_buf) then
      pcall(vim.api.nvim_buf_delete, buf, { force = true })
    end
  end
  pcall(vim.cmd, "tabonly!")
  pcall(vim.cmd, "only")
  package.loaded["arangodb.aql"] = original_aql
  package.loaded["arangodb.client"] = original_client
  require("arangodb.config").setup()
  if not ok then
    error(err, 0)
  end
end

local function base_client(overrides)
  return vim.tbl_extend("force", {
    explain_aql_async = function(_, _, _, done)
      return async({ plan = { isModificationQuery = false, collections = {} } }, done)
    end,
    execute_aql_async = function(_, _, _, _, done)
      return async({ result = { 1, 2 }, count = 2, hasMore = false }, done)
    end,
    validate_aql_async = function(_, _, done)
      return async({ parsed = true, bindVars = {} }, done)
    end,
    next_aql_page_async = function(_, _, done)
      return async({ result = {}, hasMore = false }, done)
    end,
    close_cursor_async = function() end,
  }, overrides or {})
end

local function with_saved_query_picker(entry, callback)
  local history = require("arangodb.aql_history")
  local library = require("arangodb.aql_library")
  local original_history_load = history.load
  local original_library_load = library.load
  local original_snacks = package.loaded["snacks"]
  local original_select = vim.ui.select
  local original_confirm = vim.fn.confirm
  local confirmation = { answer = 2, prompts = {} }
  history.load = function()
    return { entry }
  end
  library.load = history.load
  package.loaded["snacks"] = {
    picker = function(opts)
      local selected
      opts.finder()(function(item)
        selected = item
      end)
      opts.confirm({ close = function() end }, selected)
    end,
  }
  vim.ui.select = function(items, _, done)
    done(items[1])
  end
  vim.fn.confirm = function(message, _, default)
    confirmation.prompts[#confirmation.prompts + 1] = message
    h.eq(2, default)
    return confirmation.answer
  end

  local ok, err = xpcall(function()
    callback(confirmation)
  end, debug.traceback)
  history.load = original_history_load
  library.load = original_library_load
  package.loaded["snacks"] = original_snacks
  vim.ui.select = original_select
  vim.fn.confirm = original_confirm
  if not ok then
    error(err, 0)
  end
end

local function with_query_file(callback)
  local path = vim.fn.tempname() .. ".aql"
  vim.fn.writefile({ "RETURN 1" }, path)
  local ok, err = xpcall(function()
    vim.cmd("edit " .. vim.fn.fnameescape(path))
    callback(vim.api.nvim_get_current_buf(), path)
  end, debug.traceback)
  vim.fn.delete(path)
  if not ok then
    error(err, 0)
  end
end

return {
  h.test("AQL editor opens bind variables and keeps the query focused", function()
    with_aql(base_client(), function(aql)
      local session = aql.open({
        config = config,
        query = "RETURN 1",
      })

      h.eq(session.query_buf, vim.api.nvim_get_current_buf())
      h.eq(1, #vim.fn.win_findbuf(session.query_buf))
      h.eq(1, #vim.fn.win_findbuf(session.bind_buf))
      h.eq("json", vim.bo[session.bind_buf].filetype)
      h.eq(false, vim.bo[session.bind_buf].buflisted)
      h.eq("{}", table.concat(vim.api.nvim_buf_get_lines(session.bind_buf, 0, -1, false), "\n"))
    end)
  end),

  h.test("additional AQL editors open in isolated tabpages", function()
    with_aql(base_client(), function(aql)
      local first = aql.open({
        config = config,
        query = "RETURN @value",
        bind_vars = { value = 1 },
      })
      local unnamed_before = listed_unnamed_buffers()
      local second = aql.open({
        config = config,
        query = "RETURN @value",
        bind_vars = { value = 2 },
      })

      h.eq(false, first.tabpage == second.tabpage)
      h.eq(false, first.bind_buf == second.bind_buf)
      h.eq(unnamed_before, listed_unnamed_buffers())
      h.eq(2, #vim.api.nvim_tabpage_list_wins(first.tabpage))
      h.eq(2, #vim.api.nvim_tabpage_list_wins(second.tabpage))
      h.eq(second.query_buf, vim.api.nvim_get_current_buf())

      local first_buffers = {}
      for _, win in ipairs(vim.api.nvim_tabpage_list_wins(first.tabpage)) do
        first_buffers[vim.api.nvim_win_get_buf(win)] = true
      end
      h.eq(true, first_buffers[first.query_buf])
      h.eq(true, first_buffers[first.bind_buf])
      h.eq(nil, first_buffers[second.query_buf])
      h.eq(nil, first_buffers[second.bind_buf])

      local second_query_win = vim.fn.win_findbuf(second.query_buf)[1]
      vim.api.nvim_set_current_win(second_query_win)
      vim.api.nvim_win_set_buf(second_query_win, first.query_buf)
      local active_buffers = {}
      for _, win in ipairs(vim.api.nvim_tabpage_list_wins(second.tabpage)) do
        active_buffers[vim.api.nvim_win_get_buf(win)] = true
      end
      h.eq(true, active_buffers[first.query_buf])
      h.eq(true, active_buffers[first.bind_buf])
      h.eq(nil, active_buffers[second.bind_buf])
      h.matches('"value": 1', table.concat(vim.api.nvim_buf_get_lines(first.bind_buf, 0, -1, false), "\n"))

      vim.api.nvim_win_set_buf(second_query_win, second.query_buf)
      active_buffers = {}
      for _, win in ipairs(vim.api.nvim_tabpage_list_wins(second.tabpage)) do
        active_buffers[vim.api.nvim_win_get_buf(win)] = true
      end
      h.eq(true, active_buffers[second.query_buf])
      h.eq(true, active_buffers[second.bind_buf])
      h.eq(nil, active_buffers[first.bind_buf])

      local second_bind_win = vim.fn.win_findbuf(second.bind_buf)[1]
      vim.api.nvim_set_current_win(second_bind_win)
      vim.api.nvim_win_set_buf(second_bind_win, first.query_buf)
      active_buffers = {}
      for _, win in ipairs(vim.api.nvim_tabpage_list_wins(second.tabpage)) do
        active_buffers[vim.api.nvim_win_get_buf(win)] = true
      end
      h.eq(2, #vim.api.nvim_tabpage_list_wins(second.tabpage))
      h.eq(true, active_buffers[first.query_buf])
      h.eq(true, active_buffers[first.bind_buf])
      h.eq(nil, active_buffers[second.query_buf])
      h.eq(nil, active_buffers[second.bind_buf])
    end)
  end),

  h.test("AQL editor executes with JSON bind variables and renders results", function()
    local captured
    with_aql(
      base_client({
        execute_aql_async = function(_, query, bind_vars, opts, done)
          captured = { query = query, bind_vars = bind_vars, opts = opts }
          return async({ result = { { value = 42 } }, count = 1, hasMore = false }, done)
        end,
      }),
      function(aql)
        local session = aql.open({
          config = config,
          connection = "local",
          query = "RETURN @value",
          bind_vars = { value = 42 },
        })
        h.eq("aql", vim.bo[session.query_buf].filetype)
        vim.api.nvim_buf_call(session.query_buf, function()
          vim.cmd("ArangoAqlBindVars")
        end)
        h.eq("json", vim.bo[session.bind_buf].filetype)
        vim.api.nvim_buf_call(session.bind_buf, function()
          h.eq(2, vim.fn.exists(":ArangoAqlExecute"))
          h.eq(2, vim.fn.exists(":ArangoAqlValidate"))
          h.eq(2, vim.fn.exists(":ArangoAqlExplain"))
          h.eq(2, vim.fn.exists(":ArangoAqlProfile"))
          h.eq(2, vim.fn.exists(":ArangoAqlHistory"))
          h.eq(2, vim.fn.exists(":ArangoAqlCancel"))
          vim.cmd("ArangoAqlExecute")
        end)
        assert(vim.wait(1000, function()
          return captured ~= nil and session.result_buf ~= nil
        end))
        h.eq("RETURN @value", captured.query)
        h.eq(42, captured.bind_vars.value)
        h.eq(100, captured.opts.batch_size)
        h.eq(false, vim.bo[session.result_buf].modifiable)
        h.matches('"value": 42', table.concat(vim.api.nvim_buf_get_lines(session.result_buf, 0, -1, false), "\n"))
      end
    )
  end),

  h.test("AQL writes require confirmation before execution", function()
    local executions = 0
    local confirmed = false
    local original_confirm = vim.fn.confirm
    vim.fn.confirm = function()
      confirmed = true
      return 2
    end
    local ok, err = xpcall(function()
      with_aql(
        base_client({
          explain_aql_async = function(_, _, _, done)
            return async({
              plan = {
                isModificationQuery = true,
                collections = { { name = "items", type = "write" } },
              },
            }, done)
          end,
          execute_aql_async = function(_, _, _, _, done)
            executions = executions + 1
            return async({ result = {}, hasMore = false }, done)
          end,
        }),
        function(aql)
          local session = aql.open({ config = config, query = "REMOVE 'a' IN items" })
          vim.api.nvim_buf_call(session.query_buf, function()
            vim.cmd("ArangoAqlExecute")
          end)
          assert(vim.wait(1000, function()
            return confirmed and session.request == nil
          end))
          h.eq(0, executions)
          h.eq(nil, session.result_buf)
        end
      )
    end, debug.traceback)
    vim.fn.confirm = original_confirm
    if not ok then
      error(err, 0)
    end
  end),

  h.test("AQL result pages are fetched once and previous pages are cached", function()
    local next_calls = 0
    with_aql(
      base_client({
        execute_aql_async = function(_, _, _, _, done)
          return async({ result = { 1 }, count = 2, hasMore = true, id = "cursor-1" }, done)
        end,
        next_aql_page_async = function(_, cursor_id, done)
          next_calls = next_calls + 1
          h.eq("cursor-1", cursor_id)
          return async({ result = { 2 }, hasMore = false }, done)
        end,
      }),
      function(aql)
        local session = aql.open({ config = config, query = "FOR value IN 1..2 RETURN value" })
        vim.api.nvim_buf_call(session.query_buf, function()
          vim.cmd("ArangoAqlExecute")
        end)
        assert(vim.wait(1000, function()
          return session.result_buf ~= nil
        end))
        vim.api.nvim_buf_call(session.result_buf, function()
          vim.cmd("ArangoAqlNextPage")
        end)
        assert(vim.wait(1000, function()
          return session.page_index == 2
        end))
        vim.api.nvim_buf_call(session.result_buf, function()
          vim.cmd("ArangoAqlPrevPage")
          vim.cmd("ArangoAqlNextPage")
        end)
        h.eq(1, next_calls)
        h.eq(2, session.page_index)
      end
    )
  end),

  h.test("closing an AQL result releases its active cursor", function()
    local closed_cursor
    with_aql(
      base_client({
        execute_aql_async = function(_, _, _, _, done)
          return async({ result = { 1 }, count = 2, hasMore = true, id = "cursor-close" }, done)
        end,
        close_cursor_async = function(_, cursor_id)
          closed_cursor = cursor_id
        end,
      }),
      function(aql)
        local session = aql.open({ config = config, query = "FOR value IN 1..2 RETURN value" })
        vim.api.nvim_buf_call(session.query_buf, function()
          vim.cmd("ArangoAqlExecute")
        end)
        assert(vim.wait(1000, function()
          return session.result_buf ~= nil
        end))
        vim.api.nvim_buf_delete(session.result_buf, { force = true })
        h.eq("cursor-close", closed_cursor)
        h.eq(nil, session.cursor_id)
      end
    )
  end),

  h.test("AQL pagination ignores repeated next-page requests while one is active", function()
    local next_calls = 0
    local complete_next
    with_aql(
      base_client({
        execute_aql_async = function(_, _, _, _, done)
          return async({ result = { 1 }, count = 2, hasMore = true, id = "cursor-busy" }, done)
        end,
        next_aql_page_async = function(_, _, done)
          next_calls = next_calls + 1
          complete_next = done
          return { cancel = function() end }
        end,
      }),
      function(aql)
        local session = aql.open({ config = config, query = "FOR value IN 1..2 RETURN value" })
        vim.api.nvim_buf_call(session.query_buf, function()
          vim.cmd("ArangoAqlExecute")
        end)
        assert(vim.wait(1000, function()
          return session.result_buf ~= nil
        end))

        vim.api.nvim_buf_call(session.result_buf, function()
          vim.cmd("ArangoAqlNextPage")
          vim.cmd("ArangoAqlNextPage")
        end)
        h.eq(1, next_calls)
        complete_next(nil, { result = { 2 }, hasMore = false })
        h.eq(2, session.page_index)
      end
    )
  end),

  h.test("closing an AQL result cancels in-flight pagination without reopening it", function()
    local complete_next
    local cancelled = false
    with_aql(
      base_client({
        execute_aql_async = function(_, _, _, _, done)
          return async({ result = { 1 }, count = 2, hasMore = true, id = "cursor-close-pending" }, done)
        end,
        next_aql_page_async = function(_, _, done)
          complete_next = done
          return {
            cancel = function()
              cancelled = true
            end,
          }
        end,
      }),
      function(aql)
        local session = aql.open({ config = config, query = "FOR value IN 1..2 RETURN value" })
        vim.api.nvim_buf_call(session.query_buf, function()
          vim.cmd("ArangoAqlExecute")
        end)
        assert(vim.wait(1000, function()
          return session.result_buf ~= nil
        end))

        vim.api.nvim_buf_call(session.result_buf, function()
          vim.cmd("ArangoAqlNextPage")
        end)
        local result_buf = session.result_buf
        vim.api.nvim_buf_delete(result_buf, { force = true })
        h.eq(true, cancelled)
        h.eq(nil, session.result_buf)

        complete_next(nil, { result = { 2 }, hasMore = false })
        h.eq(nil, session.result_buf)
        h.eq(1, session.page_index)
      end
    )
  end),

  h.test("AQL result buffers toggle table format and export the current page", function()
    with_aql(
      base_client({
        execute_aql_async = function(_, _, _, _, done)
          return async({ result = { { name = "Alice", score = 10 } }, count = 1, hasMore = false }, done)
        end,
      }),
      function(aql)
        local session = aql.open({ config = config, query = "FOR user IN users RETURN user" })
        vim.api.nvim_buf_call(session.query_buf, function()
          vim.cmd("ArangoAqlExecute")
        end)
        assert(vim.wait(1000, function()
          return session.result_buf ~= nil
        end))
        local path = vim.fn.tempname() .. ".csv"
        vim.api.nvim_buf_call(session.result_buf, function()
          vim.cmd("ArangoAqlResultFormat table")
          vim.cmd("ArangoAqlExport " .. vim.fn.fnameescape(path))
        end)
        h.eq("markdown", vim.bo[session.result_buf].filetype)
        h.matches("| name | score |", table.concat(vim.api.nvim_buf_get_lines(session.result_buf, 0, -1, false), "\n"))
        h.eq(1, vim.fn.filereadable(path))
        h.matches("Alice,10", table.concat(vim.fn.readfile(path, "b"), "\n"))

        local original_confirm = vim.fn.confirm
        local answer = 2
        vim.fn.writefile({ "keep me" }, path)
        vim.fn.confirm = function(message)
          h.matches("Overwrite existing AQL export", message)
          h.matches(vim.pesc(path), message)
          return answer
        end
        vim.api.nvim_buf_call(session.result_buf, function()
          vim.cmd("ArangoAqlExport " .. vim.fn.fnameescape(path))
        end)
        h.eq("keep me", vim.fn.readfile(path)[1])
        answer = 1
        vim.api.nvim_buf_call(session.result_buf, function()
          vim.cmd("ArangoAqlExport " .. vim.fn.fnameescape(path))
        end)
        vim.fn.confirm = original_confirm
        h.matches("Alice,10", table.concat(vim.fn.readfile(path, "b"), "\n"))
        vim.fn.delete(path)
      end
    )
  end),

  h.test("real .aql buffers can be attached without becoming scratch buffers", function()
    with_aql(base_client(), function(aql)
      local buf = vim.api.nvim_create_buf(true, false)
      vim.api.nvim_buf_set_name(buf, vim.fn.tempname() .. ".aql")
      vim.api.nvim_buf_set_lines(buf, 0, -1, false, { "RETURN 42" })
      vim.bo[buf].filetype = "aql"
      vim.api.nvim_set_current_buf(buf)

      local session = aql.attach({ config = config, buf = buf })
      h.eq(buf, session.query_buf)
      h.eq(true, session.attached)
      h.eq("", vim.bo[buf].buftype)
      h.eq("RETURN 42", table.concat(vim.api.nvim_buf_get_lines(buf, 0, -1, false), "\n"))
      h.eq(2, vim.fn.exists(":ArangoAqlExecute"))
    end)
  end),

  h.test("loading history or library protects dirty attached AQL files", function()
    local entry = {
      name = "saved query",
      connection = "local",
      database = "test",
      query = "RETURN @value",
      bind_vars = { value = 42 },
    }
    for _, command in ipairs({ "ArangoAqlHistory", "ArangoAqlLibrary" }) do
      with_saved_query_picker(entry, function(confirmation)
        with_aql(base_client(), function(aql)
          with_query_file(function(buf, path)
            local session = aql.attach({ config = config, connection = "local", buf = buf, bind_vars = { old = 1 } })
            session.library_name = "original query"
            vim.api.nvim_buf_set_lines(buf, 0, -1, false, { "RETURN 'unsaved'" })
            vim.api.nvim_buf_set_lines(session.bind_buf, 0, -1, false, { '{"old": 2}' })
            vim.api.nvim_buf_call(buf, function()
              vim.cmd(command)
            end)
            h.eq({ "RETURN 'unsaved'" }, vim.api.nvim_buf_get_lines(buf, 0, -1, false))
            h.eq(true, vim.bo[buf].modified)
            h.eq({ old = 1 }, session.bind_vars)
            h.eq({ '{"old": 2}' }, vim.api.nvim_buf_get_lines(session.bind_buf, 0, -1, false))
            h.eq("original query", session.library_name)
            h.eq(1, #confirmation.prompts)
            h.matches("Replace unsaved changes", confirmation.prompts[1])
            h.eq(true, confirmation.prompts[1]:find(path, 1, true) ~= nil)

            confirmation.answer = 1
            vim.api.nvim_buf_call(buf, function()
              vim.cmd(command)
            end)
            h.eq({ entry.query }, vim.api.nvim_buf_get_lines(buf, 0, -1, false))
            h.eq(true, vim.bo[buf].modified)
            h.eq(entry.bind_vars, session.bind_vars)
            h.eq(
              entry.bind_vars,
              vim.json.decode(table.concat(vim.api.nvim_buf_get_lines(session.bind_buf, 0, -1, false), "\n"))
            )
            h.eq(command == "ArangoAqlLibrary" and entry.name or "original query", session.library_name)
            h.eq({ "RETURN 1" }, vim.fn.readfile(path))
            h.eq(2, #confirmation.prompts)
            vim.api.nvim_buf_call(buf, function()
              vim.cmd("write")
            end)
            h.eq({ entry.query }, vim.fn.readfile(path))
            h.eq(false, vim.bo[buf].modified)
          end)
        end)
      end)
    end
  end),

  h.test("loading saved queries marks clean attached files dirty only when text changes", function()
    local entry = { name = "saved", connection = "local", database = "test", query = "RETURN 42" }
    for _, command in ipairs({ "ArangoAqlHistory", "ArangoAqlLibrary" }) do
      with_saved_query_picker(entry, function(confirmation)
        with_aql(base_client(), function(aql)
          with_query_file(function(buf, path)
            aql.attach({ config = config, connection = "local", buf = buf })
            h.eq(false, vim.bo[buf].modified)
            vim.api.nvim_buf_call(buf, function()
              vim.cmd(command)
            end)
            h.eq(true, vim.bo[buf].modified)
            h.eq({ entry.query }, vim.api.nvim_buf_get_lines(buf, 0, -1, false))
            h.eq({ "RETURN 1" }, vim.fn.readfile(path))
            vim.api.nvim_buf_call(buf, function()
              vim.cmd("write")
              vim.cmd(command)
            end)
            h.eq(false, vim.bo[buf].modified)
            h.eq(0, #confirmation.prompts)
          end)
        end)
      end)
    end
  end),

  h.test("loading saved queries retains scratch buffer behavior", function()
    local entry = { name = "saved", connection = "local", database = "test", query = "RETURN 42" }
    for _, command in ipairs({ "ArangoAqlHistory", "ArangoAqlLibrary" }) do
      with_saved_query_picker(entry, function(confirmation)
        with_aql(base_client(), function(aql)
          local session = aql.open({ config = config, connection = "local", query = "RETURN 1" })
          vim.api.nvim_buf_set_lines(session.query_buf, 0, -1, false, { "RETURN 2" })
          vim.api.nvim_buf_call(session.query_buf, function()
            vim.cmd(command)
          end)
          h.eq({ entry.query }, vim.api.nvim_buf_get_lines(session.query_buf, 0, -1, false))
          h.eq("nofile", vim.bo[session.query_buf].buftype)
          h.eq(false, vim.bo[session.query_buf].modified)
          h.eq(0, #confirmation.prompts)
        end)
      end)
    end
  end),

  h.test("table preference preserves explain and validate responses and resumes for execution", function()
    with_aql(base_client(), function(aql)
      require("arangodb.config").setup({ aql = { result_format = "table", history = { enabled = false } } })
      local session = aql.open({ config = config, query = "RETURN 1" })
      for _, mode in ipairs({ "explain", "validate" }) do
        vim.api.nvim_buf_call(session.query_buf, function()
          vim.cmd(mode == "explain" and "ArangoAqlExplain" or "ArangoAqlValidate")
        end)
        assert(vim.wait(1000, function()
          return session.current_result ~= nil and session.current_result.mode == mode
        end))
        vim.api.nvim_buf_call(session.result_buf, function()
          vim.cmd("ArangoAqlResultFormat table")
        end)
        h.eq("json", vim.bo[session.result_buf].filetype)
        local rendered =
          vim.json.decode(table.concat(vim.api.nvim_buf_get_lines(session.result_buf, 0, -1, false), "\n"))
        h.eq(session.current_result.response, rendered.response)
        h.eq("table", session.result_format)
      end
      vim.api.nvim_buf_call(session.query_buf, function()
        vim.cmd("ArangoAqlExecute")
      end)
      assert(vim.wait(1000, function()
        return session.current_result.mode == "execute"
      end))
      h.eq("markdown", vim.bo[session.result_buf].filetype)
      h.matches("| 1 |", table.concat(vim.api.nvim_buf_get_lines(session.result_buf, 0, -1, false), "\n"))
    end)
  end),
}
