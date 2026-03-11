# arangodb.nvim

Browse and edit ArangoDB documents from Neovim.

This plugin extracts the ArangoDB browser workflow from my personal config into a reusable Neovim plugin with a small public API, user commands, and a healthcheck.

## Features

- Pick a database and collection from inside Neovim
- Search documents live with `snacks.nvim`
- Open documents in JSON buffers and save them back to ArangoDB
- Jump to related documents based on `_id`, `_key`, or nested relation-like fields
- Delete documents, rename collections, and truncate collections
- Discover databases from environment variables or explicit connection config

## Requirements

- Neovim `>= 0.9`
- `python3`
- `folke/snacks.nvim`

The Python runner uses the standard library only, so there is no extra Python package to install.

## Installation

### lazy.nvim

```lua
{
  "LeuciRemi/arangodb.nvim",
  dependencies = {
    "folke/snacks.nvim",
  },
  config = function()
    require("arangodb").setup({
      default_database = "_system",
      connections = {
        _system = "arangodb://root:root@127.0.0.1:8529/_system",
      },
      keymaps = {
        browse = "<leader>ea",
        resume = "<leader>eA",
      },
      document_keymaps = {
        save = "<leader>w",
        delete = "<leader>d",
        related = "gr",
      },
    })
  end,
}
```

## Setup

```lua
require("arangodb").setup({
  connections = {
    _system = "arangodb://root:root@127.0.0.1:8529/_system",
    kore = "arangodb://root:root@127.0.0.1:8529/kore",
  },
  default_database = "kore",
  python_command = "python3",
  field_sample_size = 200,
  page_size = 50,
  legacy_globals = true,
})
```

### Options

- `connections`: table of named connection URLs, either `{ name = url }` or `{ { name = "db", url = "..." } }`
- `default_database`: preferred database name or `{ name, url }`
- `python_command`: string or argv-style table used to run the Python runner
- `runner`: optional absolute path or function returning the runner path
- `keymaps.browse`: global normal-mode keymap for `require("arangodb").browse()`
- `keymaps.resume`: global normal-mode keymap for `require("arangodb").resume()`
- `document_keymaps.save`: buffer-local keymap for saving the current document
- `document_keymaps.delete`: buffer-local keymap for deleting the current document
- `document_keymaps.related`: buffer-local keymap for opening related documents
- `field_sample_size`: number of documents sampled when listing candidate filter fields
- `page_size`: number of documents fetched per picker page
- `legacy_globals`: also read `vim.g.arango_connections` and `vim.g.dbs`

## Environment variables

You can use environment variables instead of explicit `connections`:

- `NVIM_ARANGO_HOST`
- `NVIM_ARANGO_PORT`
- `NVIM_ARANGO_USER`
- `NVIM_ARANGO_PASSWORD`
- `NVIM_ARANGO_SYSTEM_URL`
- `NVIM_ARANGO_<DATABASE>_URL`

Example:

```bash
export NVIM_ARANGO_USER=root
export NVIM_ARANGO_PASSWORD=root
export NVIM_ARANGO_HOST=127.0.0.1
export NVIM_ARANGO_PORT=8529
export NVIM_ARANGO_KORE_URL='arangodb://root:root@127.0.0.1:8529/kore'
```

## Commands

- `:ArangoBrowse` - pick a database, then browse a collection
- `:ArangoBrowse {database}` - browse a specific database directly
- `:ArangoResume` - reopen the current browser picker
- `:ArangoDocumentSave` - save the current document buffer
- `:ArangoDocumentDelete` - delete the current document buffer
- `:ArangoDocumentRelated` - open a related document from the current buffer

## Lua API

```lua
require("arangodb").setup(opts)
require("arangodb").browse({ database = "kore" })
require("arangodb").resume()
```

## Healthcheck

Run:

```vim
:checkhealth arangodb
```

It checks the Python command, the bundled runner script, `snacks.nvim`, and detected database candidates.

## Notes

- This plugin currently relies on `folke/snacks.nvim` for the live picker UI.
- Connection strings may contain credentials, so prefer environment variables if you do not want them stored in your config.
- Add a license before making the repository fully public for reuse.
