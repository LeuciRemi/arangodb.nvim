# Security policy

## Reporting a vulnerability

Please report security issues privately through GitHub's **Security > Report a vulnerability** flow for this repository. Do not open a public issue containing an exploit, credentials, private connection URLs, or database contents.

Include the affected version, impact, minimal reproduction, and any suggested mitigation. You can expect an initial acknowledgement before public disclosure is coordinated.

## Credential safety

arangodb.nvim accepts credentials in connection URLs, but structured connection profiles can resolve passwords lazily through a callback, `password_env`, or `password_command`. Prefer these providers instead of committing credentials to a Neovim configuration repository. The health check and command completion do not resolve or print provider output, but users should still review logs and reproductions before sharing them.

Keep `tls_verify = true` unless you fully control the network and understand the risk. Use `tls_ca_file` for a private certificate authority.
