local root = vim.fn.getcwd()
vim.opt.rtp:prepend(root)
local snacks = vim.env.SNACKS_PATH or (vim.fn.stdpath("data") .. "/lazy/snacks.nvim")
vim.opt.rtp:append(snacks)
vim.opt.termguicolors = true
vim.opt.swapfile = false
vim.opt.shadafile = "NONE"
vim.opt.number = true
vim.opt.hlsearch = false
vim.opt.laststatus = 2
vim.opt.showtabline = 0
vim.opt.signcolumn = "no"
vim.g.mapleader = " "
vim.cmd.colorscheme("habamax")
require("snacks").setup({ picker = { enabled = true }, input = { enabled = true } })
require("arangodb").setup({
  connections = { demo = "http://root:demo-only@127.0.0.1:18529/demo" },
  default_database = "demo",
  aql = { history = { enabled = false } },
})
vim.api.nvim_set_hl(0, "Normal", { fg = "#cdd6f4", bg = "#1e1e2e" })
vim.api.nvim_set_hl(0, "NormalFloat", { fg = "#cdd6f4", bg = "#1e1e2e" })
vim.api.nvim_set_hl(0, "FloatBorder", { fg = "#89b4fa", bg = "#1e1e2e" })
vim.api.nvim_set_hl(0, "Pmenu", { fg = "#cdd6f4", bg = "#1e1e2e" })
vim.api.nvim_set_hl(0, "PmenuSel", { fg = "#cdd6f4", bg = "#313e59" })
