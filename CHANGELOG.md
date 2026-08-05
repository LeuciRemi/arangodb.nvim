# Changelog

All notable changes to arangodb.nvim are documented here.

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

[0.6.0]: https://github.com/LeuciRemi/arangodb.nvim/compare/v0.5.2...v0.6.0
