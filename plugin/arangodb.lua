if vim.g.loaded_arangodb_plugin == 1 then
  return
end

vim.g.loaded_arangodb_plugin = 1

require("arangodb.commands").setup()
