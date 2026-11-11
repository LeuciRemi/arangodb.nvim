--- Default settings for AQL query buffers.
if vim.b.did_ftplugin then
  return
end

vim.bo.omnifunc = "v:lua.require'arangodb.aql_completion'.omnifunc"
vim.bo.commentstring = "// %s"

vim.b.did_ftplugin = 1
