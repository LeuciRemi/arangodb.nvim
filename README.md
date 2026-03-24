# arangodb.nvim

Browse and edit ArangoDB documents from Neovim.

This plugin extracts the ArangoDB browser workflow from my personal config into a reusable Neovim plugin with a small public API, user commands, and a healthcheck.

## Features

- Pick a database and manage collections from inside Neovim
- Search documents live with `snacks.nvim`
- Open documents in JSON buffers and save them back to ArangoDB
- Create draft documents with prefilled `_key`, `_id`, and `_rev`, then insert them on first save
- Jump to related documents from direct foreign keys, nested relation objects, and reverse links discovered in other collections
- Create, duplicate, rename, and truncate collections from the collections picker
- Duplicate draftable documents, delete documents, rename collections, and truncate collections
- Discover databases from environment variables or explicit connection config

## Requirements

- Neovim `>= 0.9`
- `folke/snacks.nvim`
- `curl` for `https://` connections

The plugin now talks to ArangoDB through a built-in Lua HTTP client.
Plain `http://` URLs use the built-in Lua transport.
`https://` URLs are supported through `curl`.

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
        _system = "http://root:root@127.0.0.1:8529/_system",
      },
      keymaps = {
        browse = "<leader>ea",
        resume = "<leader>eA",
      },
      document_keymaps = {
        save = "<leader>w",
        delete = "<leader>d",
        duplicate = "<leader>y",
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
    _system = "http://root:root@127.0.0.1:8529/_system",
    kore = "https://root:root@db.example.com:8529/kore",
  },
  default_database = "kore",
  field_sample_size = 200,
  page_size = 50,
  http_timeout = 30000,
  tls_verify = true,
  tls_ca_file = nil,
})
```

### Options

- `connections`: table of named connection URLs, either `{ name = url }` or `{ { name = "db", url = "..." } }`; accepts `http://` and `https://`
- `default_database`: preferred database name or `{ name, url }`
- `keymaps.browse`: global normal-mode keymap for `require("arangodb").browse()`
- `keymaps.resume`: global normal-mode keymap for `require("arangodb").resume()`
- `document_keymaps.save`: buffer-local keymap for saving the current document
- `document_keymaps.delete`: buffer-local keymap for deleting the current document
- `document_keymaps.duplicate`: buffer-local keymap for duplicating the current document into a new draft
- `document_keymaps.related`: buffer-local keymap for opening related documents
- `field_sample_size`: number of documents sampled when listing candidate filter fields
- `page_size`: number of documents fetched per picker page
- `http_timeout`: timeout in milliseconds for ArangoDB HTTP requests
- `tls_verify`: verify HTTPS certificates when using `https://` URLs (default: `true`)
- `tls_ca_file`: custom CA bundle path passed to `curl --cacert` for `https://` URLs

## Environment variables

You can use environment variables instead of explicit `connections`:

- `NVIM_ARANGO_HOST`
- `NVIM_ARANGO_PORT`
- `NVIM_ARANGO_SCHEME`
- `NVIM_ARANGO_USER`
- `NVIM_ARANGO_PASSWORD`
- `NVIM_ARANGO_SYSTEM_URL`
- `NVIM_ARANGO_<DATABASE>_URL`

Example:

```bash
export NVIM_ARANGO_USER=root
export NVIM_ARANGO_PASSWORD=root
export NVIM_ARANGO_SCHEME=https
export NVIM_ARANGO_HOST=db.example.com
export NVIM_ARANGO_PORT=8529
export NVIM_ARANGO_KORE_URL='https://root:root@db.example.com:8529/kore'
```

## Commands

- `:ArangoBrowse` - pick a database, then open the collections picker
- `:ArangoBrowse {database}` - open the collections picker for a specific database
- `:ArangoResume` - reopen the current browser picker
- `:ArangoBack` - return to the previous ArangoDB picker or document view
- `:ArangoDocumentSave` - buffer-local command that saves the current document buffer, or creates a draft document on first save
- `:ArangoDocumentDuplicate` - buffer-local command that duplicates the current document buffer into a new draft with a fresh id
- `:ArangoDocumentDelete` - buffer-local command that deletes the current document buffer, or discards a draft document
- `:ArangoDocumentRelated` - buffer-local command that opens a related document from direct keys, nested relations, or reverse links in the current buffer

## Picker actions

- Collections picker: `Enter` open collection, `Ctrl-a` create a draft document, `Ctrl-n` create a collection, `Ctrl-d` duplicate a collection, `Ctrl-r` rename a collection, `Ctrl-t` truncate a collection, `Ctrl-x` open the actions menu, `Ctrl-b` go back to the database picker when available
- Documents picker: `Ctrl-a` create a draft document in the current collection, `Ctrl-y` duplicate the selected document into a new draft with a fresh id, `Ctrl-d` delete the selected document, `Ctrl-t` truncate the current collection after confirmation, `Ctrl-x` open the actions menu for the current document listing

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

It checks the HTTP transport setup, optional HTTPS support through `curl`, `snacks.nvim`, and detected database candidates.

## Notes

- This plugin currently relies on `folke/snacks.nvim` for the live picker UI.
- Use `http://` for plain connections or `https://` for TLS-enabled instances.
- Install `curl` to use `https://` connections.
- Connection strings may contain credentials, so prefer environment variables if you do not want them stored in your config.
- Add a license before making the repository fully public for reuse.
