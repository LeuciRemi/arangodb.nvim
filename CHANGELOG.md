# Changelog

All notable changes to arangodb.nvim are documented here.

## [Unreleased]

### Added

- Write AQL history and named-query files under an exclusive `<path>.lock` file (retries every 10 ms for up to one second, stale locks older than 10 seconds taken over), so concurrent Neovim instances no longer lose each other's entries; a history entry that cannot be recorded in time is skipped with one warning instead of failing the query.
- Add "Create with properties…" after the collection type prompt: a JSON editor seeded with `numberOfShards`, `replicationFactor`, `writeConcern`, `shardKeys`, and `waitForSync` defaults creates the collection on `:write`, then wipes the buffer and reopens the collections list filtered on the new name.
- Send `http://` connections through `curl` when `http_proxy`, `all_proxy`, or `ALL_PROXY` is set, so proxy and `no_proxy` settings apply as for `https://`; without a proxy or without curl the built-in transport is still used, and `:checkhealth` reports the proxy transport.
- Mark positioned AQL syntax errors as `ERROR` diagnostics (namespace `arangodb.nvim.aql`, source `ArangoDB`) in the query buffer when Execute, Profile, Explain, or Validate fails; the mark clears on the next request and after a success.
- Add `:ArangoAqlExport!` to fetch every remaining cursor page and export the rows of all pages in order. It is asynchronous, cancellable with `:ArangoAqlCancel`, leaves the displayed page unchanged, and writes no file when a page request fails; `:ArangoAqlExport` still exports the displayed page.
- Add `remove()` and `clear()` to `arangodb.aql_history`, plus a history picker actions menu with Restore, Delete entry, and confirmed Clear history.
- Add omni-completion for AQL buffers through `ftplugin/aql.lua`: keywords, ArangoDB 3.12 built-in functions, and asynchronously loaded collection names, with `// %s` comments and no completion in bind buffers.
- Add "Drop collection" to the collections actions menu with system-collection refusal, modified-buffer guards, a confirmation naming the collection and its document count, and cleanup of its unmodified document buffers.
- Add "Sort by field…" and "Open in AQL" to the document picker actions menu for collection views, kept in the route across paging and navigation, with optional `picker_keymaps.sort` and `picker_keymaps.open_aql` keys and a shared `arangodb.client.browse_query()` builder.

### Changed

- Rebuild the named-query library as a snacks.nvim picker with search and preview instead of nested `vim.ui.select` prompts; `<C-x>` opens Load and confirmed Delete.
- Explain AQL queries before Execute and Profile only when the text contains `INSERT`, `UPDATE`, `REPLACE`, `REMOVE`, or `UPSERT` as a whole word, so read queries no longer pay for an explain request.
- Request one row beyond `graph.max_nodes` and report omitted vertices in the header as `Limit: N (truncated — raise graph.max_nodes)` instead of dropping them silently.

### Fixed

- Retarget a modified document buffer, or one with a pending save, when its collection is renamed, so the next `:write` saves to the new collection without reloading or discarding the buffer state.
- Scan related documents across at most four collections concurrently and report failures once as a single warning, while keeping the successful results.
- Resolve HTTP hostnames asynchronously and try available addresses while preserving timeout and cancellation behavior.
- Preserve raw HTTPS response bytes so curl and the HTTP parser do not both decode chunked transfer encoding.
- Allow synchronous AQL helpers to read more than two batches from the same cursor.
- Protect unsaved attached AQL files when loading history or named queries, and keep loaded changes marked as modified.
- Keep Explain and Validate responses visible as JSON when table results are configured.
- Preserve the complete contents of document drafts when their collection is renamed.
- Retain edits made during document deletion or collection truncation in detached recovery buffers.
- Report document saves and creations as successful with the submitted document when the follow-up refresh fails, and invalidate cached metadata as soon as the write lands.
- Treat every HTTP status outside 200-299 as an error, including redirects, and name the redirect target.
- Release the server cursor when an AQL page request fails before re-raising the original error.
- Raise a transport error when a synchronous HTTP request times out or is interrupted instead of reporting a silent cancellation.
- Back up corrupt AQL history and named-query JSON files to `<path>.corrupt-<timestamp>` before replacing them, including when history removal or clearing rewrites the file.
- Confirm before overwriting a named AQL query with the same name, connection, and database.
- Reject connection URLs that cannot be parsed during `setup()`, naming the offending connection.
- Reset AQL result pagination when Explain or Validate renders a new response.
- Highlight `ASC` and `DESC` as keywords, highlight function calls, and treat `ANY` only as an operator in AQL buffers.
- Skip JSON `null` values when labelling graph nodes.
- Do not report caller-initiated cancellations as errors.
- Restore the last browser view with `:ArangoResume` when the picker was closed.
- Reopen the collections list after a collection rename succeeds, fails, or is cancelled, filtered on the new name after a success.
- Return from the index inspect/delete menu to the index list with `<Esc>`.
- Start a fresh query after a failed document page fetch instead of retrying the dead cursor.
- Restore the history entry when `:ArangoBack` cannot reopen a document.

### Changed

- Update StyLua to 2.6.0 in CI and contributor docs. Neovim CI keeps the 0.10.4 support floor and the current stable release (0.12.6 at update time); ArangoDB 3.12.12, snacks.nvim 2.31.0 and all GitHub Actions were already at their latest releases.
- Complete database arguments from configured connections and databases discovered during the session, without any network request.
- Offer "Create document" in the collections actions menu.
- Fail the test runner when it collects no tests, and ignore `nvim.log` in Git.

### Tests

- Cover cross-instance JSON store locking and stale-lock takeover, plain-HTTP proxy transport selection, related-scan batching and warning aggregation, collection-rename buffer retargeting, and the collection creation properties editor.
- Cover hostname resolution, address fallback, transport cancellation, chunked responses, cursor batching, and the AQL and document-buffer regressions above.
- Cover save and create refresh failures, non-2xx and redirect responses, cursor release on paging failures, synchronous transport timeouts, corrupt-file backups, named-query overwrite confirmation, connection URL validation, result page resets, graph node labels, resume and back navigation, collection rename reopening, index menu navigation, and completion without network I/O.

### Documentation

- Document the plain-HTTP proxy transport, the "Create with properties…" collection flow, and cross-instance history and named-query locking in both READMEs and Vim help.
- Document the AQL `bind_vars` option, `:ArangoResume` restoring the last view, connection URL validation, corrupt-file backups, and completion without network access in the READMEs and Vim help.
- Move the result-page mappings to the result buffer, add the missing French Lua API and health check sections, and verify in CI that `doc/tags` matches the help file.
- Document AQL error diagnostics and omni-completion, `:ArangoAqlExport!`, history and library picker actions, drop collection, sort and Open in AQL, graph truncation, and the new picker keymaps in both READMEs and Vim help.

## [0.6.2] - 2026-09-07

### Fixed

- Preserve unsaved document and metadata edits when reopening buffers or completing asynchronous saves. Draft insertion also preserves newer edits, and subsequent document saves use the updated server revision.
- Resolve conflicts using current buffer contents for forced overwrites, and skip remote reloads when new local edits arrive during the request.
- Isolate document and metadata buffers across connections with identical database names and document IDs. Scope destructive-operation guards and buffer cleanup to the matching server and database.
- Restore index-picker navigation: the configured action key opens index actions, and cancelling returns to the index selector or previous collection/document picker. Index management is also available from document pickers.

### Tests

- Add regression coverage for buffer isolation, edits during saves, draft insertion, and index navigation, including smoke coverage with real Snacks pickers.

### Documentation

- Add a reproducible Docker demo with fictional data and a recorded Neovim GIF.
- Add quick-start guides, a complete AQL example, and troubleshooting in English and French.
- Shorten the READMEs, fold detailed configuration, and keep advanced behavior in Vim help.
- Clarify connection credentials, option units, and local checks; fix incomplete help text.

## [0.6.1] - 2026-08-05

### Fixed

- Picker-to-UI transitions now wait for Snacks to destroy the previous layout before opening document buffers, editors, graph views, database pickers, or navigating back, preventing residual popups and backdrop flicker.

### Tests

- Added lifecycle-aware picker mocks and regression coverage for document, collection, graph, database, cancellation, and error handoffs.
- Added a headless smoke test using the real Snacks runtime for picker-to-document-buffer transitions.

## [0.6.0] - 2026-08-05

### Added

- Asynchronous, cancellable document and collection mutations throughout the browser UI.
- Collection property and JSON Schema editing plus secondary-index creation, inspection, and deletion.
- Faithful collection duplication for supported creation properties, computed values, schemas, and non-system indexes.
- Persistent named AQL queries, real `.aql` buffer attachment, table results, and JSON/CSV/Markdown exports.
- Bounded named-graph exploration with document navigation and configurable depth, direction, and result limits.
- Structured connection profiles with lazy password callbacks, environment providers, and external commands.
- ArangoDB 3.12 integration coverage on both `main` and `develop` CI workflows.

### Changed

- Split transport, picker UI, metadata editors, AQL persistence/results, and graph exploration into focused modules.
- Collection and document actions no longer block Neovim while waiting for the server.
- Collection duplication now rolls back the target if a property, index, or document-copy step fails or if the operation is cancelled after target creation.
- AQL exports require confirmation before overwrite and use atomic file replacement.
- Picker navigation and the actions menu work in normal and insert mode, while write mappings are unset by default and remain normal-mode-only when configured. Graph-buffer mappings are configurable.

### Fixed

- Collection editor transitions close the active picker cleanly and no longer recreate an existing Snacks backdrop, preventing background flicker.

[0.6.2]: https://github.com/LeuciRemi/arangodb.nvim/compare/v0.6.1...v0.6.2
[0.6.1]: https://github.com/LeuciRemi/arangodb.nvim/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/LeuciRemi/arangodb.nvim/compare/v0.5.2...v0.6.0
