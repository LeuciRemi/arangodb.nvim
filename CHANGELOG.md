# Changelog

All notable changes to arangodb.nvim are documented here.

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
