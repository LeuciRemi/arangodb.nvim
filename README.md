# arangodb.nvim

[![CI](https://github.com/LeuciRemi/arangodb.nvim/actions/workflows/ci.yml/badge.svg)](https://github.com/LeuciRemi/arangodb.nvim/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Browse, edit, and manage ArangoDB data without leaving Neovim.

English | [Français](README.fr.md) | [`:help arangodb.nvim`](doc/arangodb.nvim.txt)

![Browse users, edit and save a JSON document, then execute AQL and display results as a table](doc/assets/demo.gif)

Recorded with a local ArangoDB container and fictional data. [Reproduce the demo](demo/README.md).

[Installation](#installation) · [Quick start](#quick-start) · [Configuration](#configuration) · [Connections](#connections-and-credentials) · [Usage](#usage) · [AQL example](#aql-example) · [Troubleshooting](#troubleshooting) · [Development](#development)

## Features

- Browse and search databases, collections, and documents with `snacks.nvim`.
- Edit JSON with `:write`, handle revision conflicts, and create document drafts.
- Manage collections, schemas, properties, and indexes.
- Write, validate, explain, profile, and save AQL queries; export result pages.
- Follow inferred document relations and explore named graphs.
- Connect over HTTP or HTTPS, with optional password providers.

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

<details>
<summary>Installation with vim-plug</summary>

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

</details>

## Quick start

After installing the plugin, only a connection is needed; the full configuration below is optional.

1. Configure `connections` and `default_database` as in the installation example, replacing the dummy URL with your server and an existing database. The key `local_db` is a connection alias; `my_database` in the URL is the actual database name.
2. Run `:checkhealth arangodb` to check local dependencies and detected connections. It does not verify that you can authenticate or read a collection.
3. Run `:ArangoBrowse local_db`, select a collection with `<Enter>`, then open a document with `<Enter>`.
4. Edit its JSON and run `:write` to save to ArangoDB. Use `<C-x>` in a picker for actions such as creating a draft or managing indexes.
5. Run `:ArangoResume` to return to browsing, or `:ArangoAql local_db` to open a query editor.

For a disposable server with sample data, follow [the Docker demo](demo/README.md) and use the alias `demo`.

## Configuration

Call `setup()` even when using environment variables. Only override the options you need. Set a keymap to `false` to disable it; global mappings and picker write mappings are unset by default. Auto-discovery is also disabled by default.

<details>
<summary>All configuration defaults and option units</summary>

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

The values above are defaults, not a required setup template. Useful distinctions:

| Option | Meaning and unit |
| --- | --- |
| `page_size` | Documents per browser page (50) |
| `aql.batch_size` | Rows per AQL editor result page (100) |
| `aql_batch_size` | Batch size for other client AQL operations (1000) |
| `field_sample_size` | Documents inspected to discover fields (200); fields absent from this sample may not be offered |
| `cache_ttl` | Metadata cache lifetime in milliseconds (5000); `0` disables it |
| `http_timeout` | Timeout per HTTP request in milliseconds (30000) |
| `aql.cursor_ttl` | Server cursor lifetime in seconds (300) |
| `aql.max_runtime` | Query runtime limit in seconds; `nil` uses the server default |
| `diagnostics.max_size` | Journal rotation threshold in bytes (1048576); the previous file is kept as `.1` |

</details>

See [`:help arangodb.nvim-options`](doc/arangodb.nvim.txt) for option descriptions, layout, caching, and diagnostics.

## Connections and credentials

Connections use this form:

```text
http[s]://[user:password@]host[:port]/database
```

Use a structured profile to keep passwords out of your configuration:

```lua
connections = {
  work = {
    url = "https://db.example.com:8529/work",
    username = "reader",
    password_env = "ARANGODB_WORK_PASSWORD",
  },
}
```

Export `ARANGODB_WORK_PASSWORD` before launching Neovim. Alternatively, `NVIM_ARANGO_<NAME>_URL` supplies a complete URL; the connection is named after the database in that URL.

Sources are checked in order: `setup().connections`, `vim.g.arangodb_connections`, then environment URLs. The first source wins for duplicate names. Authentication is optional; percent-encoded credentials and bracketed IPv6 hosts are supported.

For password callbacks, external commands, and server discovery variables, see [`:help arangodb.nvim-connections`](doc/arangodb.nvim.txt). Use `tls_ca_file` for a private CA and keep `tls_verify = true`.

## Usage

| Command | Action |
| --- | --- |
| `:ArangoBrowse [database]` | Browse a connection; use its configured alias |
| `:ArangoResume` / `:ArangoBack` | Resume browsing / go back |
| `:ArangoAql [database]` | Open an AQL editor |
| `:ArangoAqlAttach [database]` | Attach AQL tooling to an existing `.aql` file |
| `:ArangoAqlLibrary [database]` | Browse saved queries |
| `:ArangoGraph [database]` | Explore a named graph |

In a document buffer, `:write` saves to ArangoDB. `:ArangoDocumentDuplicate`, `:ArangoDocumentDelete`, `:ArangoDocumentRelated`, and `:ArangoDocumentGraph` provide document actions. Saves detect revision conflicts and offer reload, comparison, or explicit overwrite.

AQL queries open with a companion JSON bind-variable buffer. Commands and normal-mode mappings work in both buffers; visual selections apply only to the query. Execution and profiling ask for confirmation before modification queries; validation and explain do not execute them.

History and named queries persist locally, including bind variables. For sensitive queries, disable `aql.history.enabled` or `aql.history.store_bind_vars`, and avoid saving sensitive values in the query library. See [`:help arangodb.nvim-aql`](doc/arangodb.nvim.txt) for storage, session, and command details.

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

Picker shortcuts: `<Enter>` opens the selection, `<C-x>` opens its actions, and `<C-b>` goes back when available.

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

Picker navigation works in normal and insert mode. Collection/document creation, duplication, schema and index management, and destructive actions are available through `<C-x>`. Explicitly configured write mappings are normal-mode-only. Destructive operations require confirmation, and modified-buffer guards protect unsaved edits.

ArangoDB enforces permissions: reads need read access, writes need write access, and collection/index administration needs the corresponding privileges. See [`:help arangodb.nvim-development`](doc/arangodb.nvim.txt) for permissions and collection-copy limitations.

## AQL example

With the [demo database](demo/README.md) running, open `:ArangoAql demo` and enter:

```aql
FOR user IN @@collection
  FILTER user.active == @active
  SORT user.name
  RETURN { name: user.name, role: user.role }
```

In the companion JSON buffer (`:ArangoAqlBindVars`), enter:

```json
{
  "@collection": "users",
  "active": true
}
```

Run `:write` in that buffer to validate the JSON, then `:ArangoAqlExecute` to run the query. The collection placeholder `@@collection` maps to the JSON key `"@collection"`; the value placeholder `@active` maps to `"active"`.

After a fresh seed, the `result` array contains:

```json
[
  { "name": "Alice Martin", "role": "Engineer" },
  { "name": "Ben Taylor", "role": "Designer" },
  { "name": "Chloe Dubois", "role": "Engineer" }
]
```

The GIF first changes Alice's role to `Maintainer`, so its query shows that saved value. In the result buffer, use `:ArangoAqlResultFormat table` for a compact view, or `:ArangoAqlExport /tmp/active-users.csv` to export **the current page**. The export does not fetch all remaining cursor pages.

## Graph explorer

Run `:ArangoGraph [database]` and select a graph and start document, or use `:ArangoDocumentGraph` from a document. Exploration is bounded by depth and node count. See [`:help arangodb.nvim-graph`](doc/arangodb.nvim.txt) for navigation keys and limits.

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

`:checkhealth arangodb` checks local dependencies and detected connections without printing passwords. It does not test server authorization.

## Troubleshooting

| Symptom | What to check |
| --- | --- |
| No connection appears | Call `setup()`, check the `connections` table, or export `NVIM_ARANGO_<NAME>_URL` before launching Neovim. Auto-discovery is off by default. |
| The wrong server or database opens | Use the configured alias, e.g. `:ArangoBrowse local_db`. An unknown name is used to build a URL from the `NVIM_ARANGO_HOST` settings. Environment URL connections use the database name from the URL, not the variable's `<NAME>`. |
| Connection refused or request timeout | Check the host, port, server/container status, and network reachability. For the demo, use port `18529`, not `8529`. |
| Authentication or permission error | Check `username`, the password provider, and access to the target database/collections. A successful health check does not test server authorization. |
| HTTPS certificate error | Install `curl` and configure `tls_ca_file` for a private CA. Keep certificate verification enabled. |
| Picker fails to open | Check `:checkhealth arangodb`, confirm Snacks is installed and loaded, and enable `picker` in its setup. |
| A field or relation is missing | Discovery is sampled and heuristic; check `field_sample_size` and `max_field_depth`, or query the field directly with AQL. |

For a bug report, include the output of `:messages` and the versions listed in [CONTRIBUTING.md](CONTRIBUTING.md). If you enable `diagnostics`, review the journal before sharing: it excludes credentials and bodies but includes server hostnames and request paths.

## Limitations

- AQL cancellation closes known cursors, but an in-flight server query may continue; set `aql.max_runtime` to bound execution.
- Related-document discovery is heuristic and sampled; graph exploration is a bounded textual view.
- External password commands may briefly block Neovim while resolving a secret.

## Development

Run `make lint` for formatting checks and the Neovim test suite, and `make docs` after editing Vim help. Integration tests need a disposable ArangoDB database.

See [CONTRIBUTING.md](CONTRIBUTING.md) for setup and reporting bugs, [the Docker demo](demo/README.md) for sample data and GIF recording, and [SECURITY.md](SECURITY.md) for private vulnerability reports.

## License

[MIT](LICENSE) © Remi Leuci and contributors.
