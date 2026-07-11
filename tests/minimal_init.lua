vim.opt.runtimepath:prepend(vim.fn.getcwd())
package.path = vim.fn.getcwd() .. "/?.lua;" .. package.path

vim.env.NVIM_ARANGO_HOST = nil
vim.env.NVIM_ARANGO_PORT = nil
vim.env.NVIM_ARANGO_SCHEME = nil
vim.env.NVIM_ARANGO_USER = nil
vim.env.NVIM_ARANGO_PASSWORD = nil
vim.env.NVIM_ARANGO_SYSTEM_URL = nil
for name, _ in pairs(vim.fn.environ()) do
  if name:match("^NVIM_ARANGO_.+_URL$") then
    vim.env[name] = nil
  end
end
