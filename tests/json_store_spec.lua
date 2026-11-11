local h = require("tests.helpers")

local uv = vim.uv or vim.loop

local function temporary_path()
  local path = vim.fn.tempname() .. ".json"
  vim.fn.mkdir(vim.fs.dirname(path), "p")
  return path
end

local function create_lock(path)
  local lock = path .. ".lock"
  local fd = assert(uv.fs_open(lock, "wx", 384))
  uv.fs_close(fd)
  return lock
end

return {
  h.test("updates see entries written by another writer in between", function()
    local store = require("arangodb.json_store")
    local path = temporary_path()
    local seen = {}

    local function append(name)
      return store.update(path, function(entries)
        seen[#seen + 1] = vim.deepcopy(entries)
        local updated = vim.deepcopy(entries)
        updated[#updated + 1] = { name = name }
        return updated
      end)
    end

    append("first")
    append("second")
    local entries = append("third")

    h.eq({ { name = "first" }, { name = "second" } }, seen[3])
    h.eq(3, #entries)
    h.eq({ { name = "first" }, { name = "second" }, { name = "third" } }, store.read(path))
    vim.fn.delete(path)
  end),

  h.test("a held lock fails the update without touching the data file", function()
    local store = require("arangodb.json_store")
    local path = temporary_path()
    vim.fn.writefile({ '[{"name":"kept"}]' }, path)
    local lock = create_lock(path)

    local started = uv.now()
    h.fails("is locked by another Neovim instance", function()
      store.update(path, function()
        return { { name = "lost" } }
      end, { label = "AQL library", lock_timeout = 50 })
    end)

    h.eq(true, uv.now() - started >= 50)
    h.eq({ { name = "kept" } }, store.read(path))
    h.eq(1, vim.fn.filereadable(lock))
    vim.fn.delete(lock)
    vim.fn.delete(path)
  end),

  h.test("a lock file older than ten seconds is taken over", function()
    local store = require("arangodb.json_store")
    local path = temporary_path()
    local lock = create_lock(path)
    assert(uv.fs_utime(lock, os.time() - 20, os.time() - 20))

    local entries = store.update(path, function()
      return { { name = "taken over" } }
    end, { lock_timeout = 50 })

    h.eq({ { name = "taken over" } }, entries)
    h.eq(0, vim.fn.filereadable(lock))
    h.eq({ { name = "taken over" } }, store.read(path))
    vim.fn.delete(path)
  end),

  h.test("the lock file is released when the update fails", function()
    local store = require("arangodb.json_store")
    local path = temporary_path()
    vim.fn.writefile({ '[{"name":"kept"}]' }, path)

    h.fails("boom", function()
      store.update(path, function()
        error("boom")
      end)
    end)

    h.eq(0, vim.fn.filereadable(path .. ".lock"))
    h.eq({ { name = "kept" } }, store.read(path))
    vim.fn.delete(path)
  end),
}
