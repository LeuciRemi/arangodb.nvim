# Changelog

All notable changes to arangodb.nvim are documented here.

## [Unreleased]

### Fixed

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
- Back up corrupt AQL history and named-query JSON files to `<path>.corrupt-<timestamp>` before replacing them.
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

- Cover hostname resolution, address fallback, transport cancellation, chunked responses, cursor batching, and the AQL and document-buffer regressions above.
- Cover save and create refresh failures, non-2xx and redirect responses, cursor release on paging failures, synchronous transport timeouts, corrupt-file backups, named-query overwrite confirmation, connection URL validation, result page resets, graph node labels, resume and back navigation, collection rename reopening, index menu navigation, and completion without network I/O.

### Documentation

- Document the AQL `bind_vars` option, `:ArangoResume` restoring the last view, connection URL validation, corrupt-file backups, and completion without network access in the READMEs and Vim help.
- Move the result-page mappings to the result buffer, add the missing French Lua API and health check sections, and verify in CI that `doc/tags` matches the help file.

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
