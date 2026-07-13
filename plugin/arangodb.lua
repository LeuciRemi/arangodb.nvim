-- Guard the plugin entry point so commands are only registered once.
if vim.g.loaded_arangodb_plugin == 1 then
  return
end

vim.g.loaded_arangodb_plugin = 1

vim.filetype.add({ extension = { aql = "aql" } })
require("arangodb.commands").setup()
