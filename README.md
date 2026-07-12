# arangodb.nvim

[![CI](https://github.com/LeuciRemi/arangodb.nvim/actions/workflows/ci.yml/badge.svg)](https://github.com/LeuciRemi/arangodb.nvim/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Browse, edit, and manage ArangoDB data without leaving Neovim.

English | [Français](README.fr.md) | [`:help arangodb.nvim`](doc/arangodb.nvim.txt)

## Features

- Browse databases, collections, and documents with `snacks.nvim`.
- Search any sampled document field with asynchronous, cursor-backed AQL pages.
- Edit JSON documents with `:write` and resolve concurrent `_rev` conflicts.
- Create or duplicate documents as drafts before inserting them.
- Create, duplicate, rename, and truncate document or edge collections.
- Follow direct foreign keys, nested relation objects, and inferred reverse links.
- Connect over HTTP with the built-in Lua transport or HTTPS through `curl`.
- Diagnose the local setup with `:checkhealth arangodb`.

## Requirements

- Neovim `>= 0.10` (CI covers `0.10.4` and the current stable release).
- [`folke/snacks.nvim`](https://github.com/folke/snacks.nvim) with its picker enabled; `v2.31.0` is tested in CI.
- `curl` for HTTPS connections. Plain HTTP uses the built-in libuv transport.
- An ArangoDB server reachable through its HTTP API. Integration tests cover ArangoDB `3.12`.

## Installation

### lazy.nvim

```lua
{
  "LeuciRemi/arangodb.nvim",
  dependencies = {
    { "folke/snacks.nvim", opts = { picker = { enabled = true } } },
  },
  opts = {
    connections = {
      local_db = "http://root:password@127.0.0.1:8529/my_database",
    },
    default_database = "local_db",
  },
}
```

### vim-plug

```vim
Plug 'folke/snacks.nvim'
Plug 'LeuciRemi/arangodb.nvim'
```

Then configure both plugins from Lua:

```lua
require("snacks").setup({ picker = { enabled = true } })
require("arangodb").setup({
  connections = {
    local_db = "http://root:password@127.0.0.1:8529/my_database",
  },
})
```

## Configuration

Calling `setup()` is recommended, even when connections are supplied only through environment variables.

```lua
require("arangodb").setup({
  connections = nil,
  default_database = nil,
  auto_discover = false,

  keymaps = {
    browse = nil,
    resume = nil,
    back = nil,
  },
  picker_keymaps = {
    execute = "<C-x>",
    create = "<C-a>",
    create_collection = "<C-n>",
    duplicate_collection = "<C-d>",
    next_page = "<C-n>",
    prev_page = "<C-p>",
    back = "<C-b>",
    change_field = "<C-f>",
    reset = "<C-u>",
    related = "<C-o>",
    delete = "<C-d>",
    duplicate = "<C-y>",
    truncate = "<C-t>",
    rename = "<C-r>",
  },
  document_keymaps = {
    save = nil,
    delete = nil,
    duplicate = nil,
    related = nil,
  },

  layout = {
    preset = "auto",
    preview = true,
  },
  field_sample_size = 200,
  page_size = 50,
  json_indent = 2,
  truncate_length = 120,
  max_field_depth = 4,
  aql_batch_size = 1000,
  cache_ttl = 5000,
  default_sort = "doc._key ASC",
  show_system_collections = false,
  http_timeout = 30000,
  tls_verify = true,
  tls_ca_file = nil,
  diagnostics = {
    enabled = false,
    path = nil,
    max_size = 1048576,
  },
})
```

Set any keymap to `false` to disable it. Global keymaps are unset by default so the plugin does not claim user mappings. `layout.preset = "auto"` selects a side-by-side view on wide screens and a stacked view on smaller screens.

`auto_discover` is deliberately disabled by default. When enabled, the plugin queries `/_api/database/user` using the `NVIM_ARANGO_HOST`, port, scheme, and credential variables. This avoids unexpected network requests during command completion and health checks.

Collection metadata, sampled fields, and figures use the short `cache_ttl` cache. Set it to `0` to disable caching. The optional diagnostic journal writes sanitized JSON-lines request metadata, never credentials, headers, or request bodies. Its default path is `stdpath("log") .. "/arangodb.nvim.log"`.

## Connections and credentials

Connections use this form:

```text
http[s]://[user:password@]host[:port]/database
```

Authentication is optional, percent-encoded credentials are supported, and IPv6 addresses must use brackets:

```lua
connections = {
  no_auth = "http://127.0.0.1:8529/example",
  encoded = "https://user%40example.com:p%40ssword@db.example.com:8529/example",
  ipv6 = "http://[::1]:8529/example",
}
```

Avoid committing credentials. Every `NVIM_ARANGO_<NAME>_URL` value is detected automatically, and its database name is read from the URL:

```bash
export NVIM_ARANGO_WORK_URL='https://reader:secret@db.example.com:8529/work'
```

The following variables configure server-wide discovery when `auto_discover = true`, and are also used to build a connection for `:ArangoBrowse {database}`:

- `NVIM_ARANGO_HOST` (default `127.0.0.1`)
- `NVIM_ARANGO_PORT` (default `8529`)
- `NVIM_ARANGO_SCHEME` (`http`, `https`, `ssl`, or `tls`)
- `NVIM_ARANGO_USER` (default `root`)
- `NVIM_ARANGO_PASSWORD` (default `root`)
- `NVIM_ARANGO_SYSTEM_URL`

For private certificate authorities, set `tls_ca_file`. Disabling `tls_verify` is supported but not recommended.

## Usage

```vim
:ArangoBrowse
:ArangoBrowse my_database
:ArangoResume
:ArangoBack
```

Inside a document buffer:

```vim
:write
:ArangoDocumentSave
:ArangoDocumentDuplicate
:ArangoDocumentDelete
:ArangoDocumentRelated
```

Default collection-picker actions:

| Key | Action |
| --- | --- |
| `<Enter>` | Open the selected collection |
| `<C-a>` | Create a draft document |
| `<C-n>` | Create a collection |
| `<C-d>` | Duplicate the selected collection |
| `<C-r>` | Rename the selected collection |
| `<C-t>` | Truncate the selected collection |
| `<C-x>` | Open the actions menu |
| `<C-b>` | Return to database selection when available |

Default document-picker actions:

| Key | Action |
| --- | --- |
| `<Enter>` | Open the selected document |
| `<C-a>` | Create a draft document |
| `<C-y>` | Duplicate the selected document as a draft |
| `<C-d>` | Delete the selected document |
| `<C-o>` | Browse inferred relations |
| `<C-f>` | Change the search field |
| `<C-u>` | Reset the search |
| `<C-p>` / `<C-n>` | Previous / next page |
| `<C-t>` | Truncate the collection |
| `<C-x>` | Open the actions menu |
| `<C-b>` | Go back |

Destructive operations request confirmation. Renaming or truncating a collection is refused while a matching ArangoDB buffer has unsaved changes.

Document saves use `_rev` as an optimistic concurrency guard. When the remote document changed, the plugin offers to reload it, compare local and remote JSON, or explicitly force the overwrite. Picker reads are asynchronous and cancellable; document pages use ArangoDB cursors and previously visited pages remain available locally.

## Lua API

```lua
require("arangodb").setup(opts)
require("arangodb").browse({ database = "work" })
require("arangodb").resume()
require("arangodb").back()
```

## Health check

Run `:checkhealth arangodb` to inspect Neovim compatibility, transports, TLS settings, `snacks.nvim`, and detected database candidates. Passwords are never printed.

## Limitations

- Picker reads are asynchronous; explicit mutation commands still wait for their server response.
- HTTPS currently requires the external `curl` executable.
- Related-document navigation is heuristic: it recognizes `_id`, `_key`, `*_id`, `*_key`, plural variants, nested relation objects, and reverse fields sampled from other collections.
- Collection duplication copies documents and the collection type, but not indexes, schemas, computed values, or other collection properties.

## Development

The test suite has no Lua runtime dependencies beyond Neovim:

```bash
make test
make lint       # StyLua 2.5.2 plus tests
make docs       # regenerate help tags
```

An optional integration test needs a disposable database because it performs destructive collection operations (it also attempts to clean up):

```bash
ARANGODB_TEST_URL=http://127.0.0.1:8529/_system make integration
```

Issues and pull requests are welcome. Please include your Neovim, `snacks.nvim`, and ArangoDB versions, a minimal configuration with credentials removed, and the smallest reproducible sequence of actions. See [CONTRIBUTING.md](CONTRIBUTING.md) for the workflow and [SECURITY.md](SECURITY.md) for private vulnerability reports.

## License

[MIT](LICENSE) © Remi Leuci and contributors.
