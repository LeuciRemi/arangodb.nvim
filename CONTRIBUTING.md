# Contributing to arangodb.nvim

Contributions from all experience levels are welcome. Bug reports, documentation improvements, tests, accessibility work, and focused feature proposals are all useful.

## Before opening an issue

1. Update to a supported Neovim and `snacks.nvim` version.
2. Run `:checkhealth arangodb`.
3. Remove passwords and private hostnames from logs and examples.
4. Check existing issues and reduce the problem to the smallest reproducible configuration.

Please include the Neovim, `snacks.nvim`, and ArangoDB versions and whether the connection uses HTTP or HTTPS.

## Local development

```bash
make test
make lint
make docs
```

The unit suite runs on Neovim 0.10.4 and the current stable version in CI. Formatting uses StyLua 2.5.2. CI also runs the real-server suite against ArangoDB 3.12. You can run it locally against a disposable database:

```bash
ARANGODB_TEST_URL=http://127.0.0.1:8529/_system make integration
```

## Pull requests

- Keep changes focused and explain the user-visible behavior.
- Add regression tests for bug fixes when practical.
- Update `README.md`, `README.fr.md`, and Vim help when behavior or configuration changes.
- Do not include credentials, database contents, generated editor files, or unrelated formatting.
- Confirm that `make lint` passes before requesting review.

By participating, you agree to be respectful, constructive, and welcoming. Harassment, discrimination, and personal attacks are not accepted in project spaces.
