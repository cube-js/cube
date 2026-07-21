# Cube CLI (`cube`)

A fully native, single-binary command line interface for the Cube Cloud
public REST API, written in Rust. Structured after the
[Railway CLI](https://github.com/railwayapp/cli): one module per command
group under `src/commands/`, a shared HTTP client, a config module, and
plain clap-derive dispatch in `main.rs`.

## Install

Linux / macOS:

```bash
curl -fsSL https://raw.githubusercontent.com/cube-js/cube/master/install-cli.sh | sh
```

Windows (PowerShell):

```powershell
irm https://raw.githubusercontent.com/cube-js/cube/master/install-cli.ps1 | iex
```

Both scripts download the latest release binary for your platform and put it
on your `PATH` (`CUBE_INSTALL_DIR` overrides the location; `CUBE_VERSION`
pins a specific release tag).

### Updates

Every run checks GitHub for a newer release in the background and prints a
notice when one is available (set `CUBE_NO_UPDATE_CHECK=1` to disable, e.g.
in CI; the notice only goes to interactive terminals, on stderr). Update
in place any time with:

```bash
cube update          # download the latest release and replace this binary
cube update --check  # just report what's available
```

### Telemetry

The CLI sends anonymous usage events (command group, success/failure,
version, platform) to `track.cube.dev` — the same pipeline and wire format
as the legacy `cubejs` CLI. No personal data is collected; the anonymous id
is a SHA-256 hash of the OS machine id. Telemetry is disabled automatically
in CI (the `CI` env var), or explicitly with `CUBE_NO_TELEMETRY=1` (or the
legacy `CUBEJS_TELEMETRY=false`).

### Build from source

```bash
cd rust/cube-cli
cargo build --release
# binary at target/release/cube
```

The binary is fully static-friendly: TLS is provided by rustls, so there is
no OpenSSL dependency and musl builds work out of the box.

## Versioning & releases

The CLI version tracks the Cube monorepo version (`lerna.json`); `Cargo.toml`
is kept in sync and the release build overrides it from the pushed tag, so
`cube --version` always matches the Cube release.

The CLI is built and published by the **same release workflow as the rest of
Cube** (`.github/workflows/publish.yml`, on `v*.*.*` tags). Its `cube-cli`
job builds a single static binary per platform and attaches them to the same
GitHub release as the Cube version, via `svenstaro/upload-release-action`:

| Platform | Target | Asset |
|---|---|---|
| Linux x86_64 | `x86_64-unknown-linux-musl` | `cube-x86_64-unknown-linux-musl.tar.gz` |
| Linux arm64 | `aarch64-unknown-linux-musl` | `cube-aarch64-unknown-linux-musl.tar.gz` |
| macOS Intel | `x86_64-apple-darwin` | `cube-x86_64-apple-darwin.tar.gz` |
| macOS Apple Silicon | `aarch64-apple-darwin` | `cube-aarch64-apple-darwin.tar.gz` |
| Windows x86_64 | `x86_64-pc-windows-msvc` | `cube-x86_64-pc-windows-msvc.tar.gz` |

The Linux builds are fully static (musl + rustls); each archive contains just
the `cube` binary.

Pull-request CI (`.github/workflows/cube-cli.yml`) runs fmt, clippy, tests,
and a release build on every change under `rust/cube-cli/`.

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
| `deployments` | list, get, create (`--bootstrap` scaffolds + builds a serving deployment), update, delete, token, advance-step, reset-step |
| `regions` | list available deployment regions |
| `logs` | tail deployment pod logs (`--pod`, `-c/--container`; defaults to the Cube API container) |
| `github` (`gh`) | status, installations, repos, branches, connect (import a repo into a deployment + first build) |
| `data-model` (`dm`) | list, get, put, delete, rename files; branches, create-branch, dev-mode, exit-dev-mode, commit, pull |
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
