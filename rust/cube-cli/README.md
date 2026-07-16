# Cube CLI (`cube`)

A fully native, single-binary command line interface for the Cube Cloud
public REST API, written in Rust. Structured after the
[Railway CLI](https://github.com/railwayapp/cli): one module per command
group under `src/commands/`, a shared HTTP client, a config module, and
plain clap-derive dispatch in `main.rs`.

## Install / build

```bash
cd rust/cube-cli
cargo build --release
# binary at target/release/cube
```

The binary is fully static-friendly: TLS is provided by rustls, so there is
no OpenSSL dependency and musl builds work out of the box.

## Authentication

Credentials resolve in this order:

1. `--token` / `--api-url` flags
2. `CUBE_API_KEY` / `CUBE_API_URL` environment variables (for CI)
3. The active context in the config file, written by `cube login`

The config file lives at `~/.config/cube/config.toml` on Linux/macOS (XDG)
and `%APPDATA%\cube\config.toml` on Windows, created with `0600`
permissions. Multiple tenants are supported as named contexts.

`cube login` uses the browser **device authorization flow** (OAuth 2.0
device grant, RFC 8628), the same style as the Railway CLI: it prints a URL
and a short code, opens your browser, and waits while you approve. The
resulting access token (and refresh token) are saved to the active context.

```bash
cube login --name staging          # device flow: opens browser, waits for approval
cube login --api-key <key>         # non-interactive: use an API key instead
cube context list
cube context use staging
cube whoami
```

Access tokens are short-lived; the CLI **auto-refreshes** them. When a
request gets a `401` and the active context has a refresh token, the client
transparently exchanges it at `/auth/oauth2/refresh`, saves the new token
pair back to the config, and retries — so a saved login keeps working
without re-authenticating every hour. If the refresh token itself is dead
(e.g. revoked), the CLI falls back to a clear "session expired — run
`cube login`" message. Auto-refresh is disabled when an explicit `--token`
/ `CUBE_API_KEY` is supplied (that token stands on its own).

The device-flow endpoints, CLI `client_id`, scope, and (if the client is
confidential) secret can be overridden without a rebuild via
`CUBE_OAUTH_CLIENT_ID`, `CUBE_OAUTH_CLIENT_SECRET`, and `CUBE_OAUTH_SCOPE`.
For CI, skip login entirely and pass `CUBE_API_KEY` / `CUBE_API_URL`.

## Commands

Every endpoint of the Console Server public API is covered:

| Group | Endpoints |
|---|---|
| `deployments` | list, get, create, update, delete, token |
| `regions` | list available deployment regions |
| `environments` | list, tokens, create-token (incl. `--meta-sync`) |
| `variables` | list, set (`KEY=VALUE` upserts) |
| `folders` | list, create, update, delete, ancestors |
| `workbooks` | list, get, create, update, delete, duplicate, publish, dashboard, ai-thread |
| `reports` | list, get, create, update, delete, refresh, connect-workbook, folders |
| `workspace` | list, shared, move |
| `notifications` | list, get, create, update, delete, recipients list/add/remove |
| `users` | list, me, create, update, delete, embed-theme |
| `groups` | list, delete |
| `attributes` | list, create, update, delete, values get/set |
| `policies` | get, set-user, set-group |
| `tenant` | settings, update |
| `embed` | generate-session, token, dashboard, tenant delete/groups/delete-group |
| `integrations` | list, get, create, update, delete, tokens list/get/revoke/initiate |
| `oidc` | list, get, create, update, delete |
| `agents` | list, skills |
| `ai-engineer` | settings |
| `app` | config, theme |
| `meta` | POST /api/v1/meta/ |
| `scim` | Users/Groups CRUD + patch, resource-types, schemas, service-provider-config |
| `api` | raw escape hatch: `cube api GET /api/v1/... -q key=value -d '{...}'` |

Conventions:

- List commands render tables by default; `--json` prints the raw response.
  Get/create/update commands always print JSON.
- Complex request bodies are passed with `-d/--data`, accepting inline JSON,
  `@file.json`, or `-` for stdin (same convention as `gh api`).
- Common fields also have dedicated flags (e.g.
  `cube reports create 1 --name x --json-query '...'`), which override
  values in `--data`.
- `cube completion <shell>` generates bash/zsh/fish/powershell completions.

## Development

```bash
cargo build
cargo test
cargo clippy
```

This crate is a standalone workspace, intentionally not a member of
`rust/cube`, so it can be released on its own cadence and built with plain
stable Rust.
