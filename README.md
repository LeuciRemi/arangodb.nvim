# arangodb.nvim

[![CI](https://github.com/LeuciRemi/arangodb.nvim/actions/workflows/ci.yml/badge.svg)](https://github.com/LeuciRemi/arangodb.nvim/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Browse, edit, and manage ArangoDB data without leaving Neovim.

English | [Français](README.fr.md) | [`:help arangodb.nvim`](doc/arangodb.nvim.txt)

## Features

- Browse databases, collections, and documents with `snacks.nvim`.
- Search any sampled document field with asynchronous, cursor-backed AQL pages.
- Write, validate, explain, execute, and profile AQL in dedicated Neovim buffers.
- Edit JSON documents with `:write` and resolve concurrent `_rev` conflicts.
- Create or duplicate documents as drafts before inserting them.
- Create, duplicate, rename, and truncate document or edge collections asynchronously.
- Edit collection schemas/properties and create, inspect, or delete indexes.
- Duplicate collections with their supported properties, schemas, computed values, and secondary indexes.
- Follow direct foreign keys, nested relation objects, and inferred reverse links.
- Save named AQL queries, attach tooling to real `.aql` files, and export result pages as JSON, CSV, or Markdown.
- Explore bounded neighborhoods in named graphs from a command or document.
- Resolve passwords lazily from callbacks, environment variables, or external commands.
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
    create = false,
    create_collection = false,
    duplicate_collection = false,
    next_page = "<C-n>",
    prev_page = "<C-p>",
    back = "<C-b>",
    change_field = "<C-f>",
    reset = "<C-u>",
    related = "<C-o>",
    delete = false,
    duplicate = false,
    truncate = false,
    rename = false,
  },
  document_keymaps = {
    save = nil,
    delete = nil,
    duplicate = nil,
    related = nil,
    graph = nil,
  },
  graph_keymaps = {
    open = "<CR>",
    start = "s",
    refresh = "r",
    depth = "d",
    direction = "t",
  },
  aql_keymaps = {
    execute = "<leader>ar",
    validate = "<leader>av",
    explain = "<leader>ae",
    profile = "<leader>ap",
    bind_vars = "<leader>ab",
    history = "<leader>ah",
    library = "<leader>al",
    save = "<leader>as",
    cancel = "<leader>ac",
    result_format = nil,
    export = nil,
    next_page = "<C-n>",
    prev_page = "<C-p>",
  },
  aql = {
    batch_size = 100,
    cursor_ttl = 300,
    max_runtime = nil,
    result_split = "auto",
    result_format = "json",
    history = {
      enabled = true,
      max_entries = 100,
      path = nil,
      store_bind_vars = true,
    },
    library = {
      path = nil,
    },
  },
  graph = {
    depth = 2,
    max_nodes = 100,
    direction = "ANY",
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

Set any keymap to `false` to disable it. Global keymaps and picker write mappings are unset by default so the plugin does not claim user mappings or bind database mutations to input-editing keys. `layout.preset = "auto"` selects a side-by-side view on wide screens and a stacked view on smaller screens.

`auto_discover` is deliberately disabled by default. When enabled, the plugin queries `/_api/database/user` using the `NVIM_ARANGO_HOST`, port, scheme, and credential variables. This avoids unexpected network requests during command completion and health checks.

Collection metadata, sampled fields, and figures use the short `cache_ttl` cache. Set it to `0` to disable caching. Collection previews load per-collection figures and database-wide document and approximate-size totals in the background. The optional diagnostic journal writes sanitized JSON-lines request metadata, never credentials, headers, or request bodies. Its default path is `stdpath("log") .. "/arangodb.nvim.log"`.

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

Structured profiles keep the password out of the URL and resolve it only when the connection is opened. `password` may also be a callback; `password_command` accepts a command list (preferred) or a shell command string and defaults to a 10-second timeout (`password_command_timeout`):

```lua
connections = {
  work = {
    url = "https://db.example.com:8529/work",
    username = "reader",
    password_env = "ARANGODB_WORK_PASSWORD",
  },
  vault = {
    url = "https://db.example.com:8529/vault",
    username = "reader",
    password_command = { "security", "find-generic-password", "-w", "-s", "arangodb-vault" },
  },
  dynamic = {
    url = "http://127.0.0.1:8529/example",
    password = function(context)
      return load_secret(context.name)
    end,
  },
}
```

Provider output is never added to completion entries or health output. A command provider must print only the password to standard output.

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
:ArangoAql
:ArangoAql my_database
:ArangoAqlAttach my_database
:ArangoAqlLibrary my_database
:ArangoGraph my_database
```

Inside a document buffer:

```vim
:write
:ArangoDocumentSave
:ArangoDocumentDuplicate
:ArangoDocumentDelete
:ArangoDocumentRelated
:ArangoDocumentGraph
```

Inside an AQL editor opened by `:ArangoAql`:

```vim
:ArangoAqlExecute
:ArangoAqlValidate
:ArangoAqlExplain
:ArangoAqlProfile
:ArangoAqlBindVars
:ArangoAqlHistory
:ArangoAqlLibrary
:ArangoAqlSave
:ArangoAqlCancel
```

Result buffers provide `:ArangoAqlResultFormat [json|table]` and `:ArangoAqlExport [path]`. The export format is inferred from `.json`, `.csv`, `.md`, or `.markdown`. Exports are written atomically; parent directories are created as needed, and replacing an existing file requires explicit confirmation. Use `:ArangoAqlAttach [database]` from a real `.aql` file to add the same execution, bind-variable, history, and library commands without turning it into a scratch buffer.

The query buffer uses the `aql` filetype. Its unlisted companion JSON bind-variable buffer opens automatically below it while focus stays on the query; selecting another listed AQL query buffer automatically switches the companion split to that session's variables. The AQL commands above and their normal-mode mappings are available from both buffers and always target the associated query; visual-selection mappings remain query-only. Collection variables such as `@@collection` use a key such as `"@collection"`. If the current tab page already contains an AQL session, `:ArangoAql` opens the next session in a new tab page instead of stacking its splits. Results stay with their session and open in a read-only JSON split, at the right on wide screens and below on smaller screens. Cursor pages already visited stay cached locally.

Execution and profiling first ask ArangoDB for the optimized plan. Queries with `plan.isModificationQuery = true` require explicit confirmation showing the target database and write collections. Explain and validation never execute the query.

History is searchable through `snacks.nvim` and is stored by default in `stdpath("data") .. "/arangodb.nvim/aql_history.json"` with user-only permissions. It never stores connection URLs, credentials, results, or errors. Queries and bind variables may themselves contain sensitive data; set `aql.history.enabled = false` or `store_bind_vars = false` when needed. After `store_bind_vars` is disabled, the next history write also removes bind variables from retained entries.

Named queries are stored separately in `stdpath("data") .. "/arangodb.nvim/aql_library.json"`, scoped by connection and database, with user-only permissions. They persist the current bind variables, which may contain sensitive values. `:ArangoAqlSave` creates or replaces a name; `:ArangoAqlLibrary` loads or deletes an entry without executing it.

Default AQL-buffer mappings:

| Key | Action |
| --- | --- |
| `<leader>ar` | Execute the query or visual selection |
| `<leader>av` | Validate without execution |
| `<leader>ae` | Explain without execution |
| `<leader>ap` | Execute with profiling |
| `<leader>ab` | Edit bind variables |
| `<leader>ah` | Browse local history |
| `<leader>al` | Browse named queries |
| `<leader>as` | Save the query by name |
| `<leader>ac` | Cancel and close the active cursor |
| `<C-p>` / `<C-n>` | Previous / next result page |

Default collection-picker actions:

| Key | Action |
| --- | --- |
| `<Enter>` | Open the selected collection |
| `<C-x>` | Open the actions menu |
| `<C-b>` | Return to database selection when available |

Default document-picker actions:

| Key | Action |
| --- | --- |
| `<Enter>` | Open the selected document |
| `<C-o>` | Browse inferred relations |
| `<C-f>` | Change the search field |
| `<C-u>` | Reset the search |
| `<C-p>` / `<C-n>` | Previous / next page |
| `<C-x>` | Open the actions menu |
| `<C-b>` | Go back |

Navigation and the actions menu are available in both normal and insert mode. Picker write mappings are disabled by default; create, duplicate, rename, delete, and truncate remain available from `<C-x>`. When explicitly configured, write mappings are normal-mode-only. Destructive operations request confirmation showing the target database and resource. Truncation uses an irreversible-action warning. Renaming, truncating, or deleting through an affected document buffer is refused while a matching ArangoDB buffer has unsaved changes.

The collection and document actions menus expose index management. The index selector uses the configured picker action mapping (`<C-x>` by default) for index creation, JSON inspection, and deletion. `<Esc>` returns from the action menu to the index selector, then from the index selector to the previous collection or document picker. The collection actions menu also exposes the JSON editor for mutable collection properties, including document validation schemas. Collection duplication copies supported creation properties and all non-system indexes before copying documents; a failed copy, or cancellation after target creation, removes the partially created target. A cleanup failure is reported explicitly.

### Required ArangoDB permissions

Grant only the access needed by the enabled workflows. Browsing, AQL reads, and graph traversal require read access to the database and every collection read by the query or graph. Document writes and modification AQL require write access to each affected collection. Creating, renaming, truncating, or duplicating collections and changing properties, schemas, or indexes require database/collection administration privileges appropriate to the ArangoDB deployment. The plugin does not bypass ArangoDB authorization; exact role and permission requirements can vary between single-server, cluster, and managed deployments.

Document saves use `_rev` as an optimistic concurrency guard. When the remote document changed, the plugin offers to reload it, compare local and remote JSON, or explicitly force the overwrite. Picker reads are asynchronous and cancellable; document pages use ArangoDB cursors and previously visited pages remain available locally.

## Graph explorer

`:ArangoGraph [database]` lists named graphs, asks for a start document ID such as `users/alice`, and opens a bounded breadth-first neighborhood. From an existing document use `:ArangoDocumentGraph` or the picker actions menu. In the graph buffer, `<CR>` opens a vertex document, `s` traverses from the selected vertex, `r` refreshes, `d` changes depth, and `t` cycles `ANY`, `OUTBOUND`, and `INBOUND`. These mappings are configurable or may be disabled through `graph_keymaps`. Depth is capped at 10 and `graph.max_nodes` bounds each result.

## Lua API

```lua
require("arangodb").setup(opts)
require("arangodb").browse({ database = "work" })
require("arangodb").aql({ database = "work", query = "RETURN 1" })
require("arangodb").aql_attach({ database = "work" })
require("arangodb").aql_library({ database = "work" })
require("arangodb").graph({ database = "work", graph = "social", start = "users/alice" })
require("arangodb").resume()
require("arangodb").back()
```

## Health check

Run `:checkhealth arangodb` to inspect Neovim compatibility, transports, TLS settings, `snacks.nvim`, and detected database candidates. Passwords are never printed.

## Limitations

- Remote picker, document, collection, metadata, and graph operations are asynchronous and cancellable. Local password commands are resolved when a connection opens and may briefly block Neovim while the command exits.
- AQL cancellation stops the local request and closes known cursors. Without `aql.max_runtime`, an in-flight server query may continue according to the server configuration.
- HTTPS currently requires the external `curl` executable.
- Related-document navigation is heuristic: it recognizes `_id`, `_key`, `*_id`, `*_key`, plural variants, nested relation objects, and reverse fields sampled from other collections.
- Named-graph exploration is a bounded textual neighborhood, not a force-directed canvas. It intentionally limits depth and returned vertices.

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
