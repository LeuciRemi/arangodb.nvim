--- Small cancellable sequence helper for multi-request client operations.
local M = {}

--- Create a cancellable task and return task, step, and finish functions.
function M.sequence(callback)
  local active
  local cancelled = false
  local finished = false
  local task = {}

  local function finish(err, value)
    if cancelled or finished then
      return
    end
    finished = true
    active = nil
    callback(err, value)
  end

  local step
  step = function(starter, on_success)
    if cancelled or finished then
      return
    end
    local completed = false
    local ok, handle = pcall(starter, function(err, value)
      completed = true
      if cancelled or finished then
        return
      end
      active = nil
      if err then
        finish(err)
        return
      end
      if not on_success then
        finish(nil, value)
        return
      end
      local success_ok, success_err = pcall(on_success, value, step, finish)
      if not success_ok then
        finish(success_err)
      end
    end)
    if not ok then
      finish(handle)
    elseif not completed then
      active = handle
    end
  end

  function task.cancel()
    if cancelled or finished then
      return
    end
    cancelled = true
    if active and active.cancel then
      active.cancel()
    end
    active = nil
  end

  return task, step, finish
end

--- Schedule an already available result with the same cancellable handle shape.
function M.resolved(callback, value)
  local cancelled = false
  vim.schedule(function()
    if not cancelled then
      callback(nil, value)
    end
  end)
  return {
    cancel = function()
      cancelled = true
    end,
  }
end

return M
