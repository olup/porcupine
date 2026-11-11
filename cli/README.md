# Porcupine CLI

Use `porcupine` to query an instance, manage projects and upload import chunks from the terminal. It is a standalone HTTP client. Its help works offline.

## Install

Choose the `porcupine-cli-<tag>-<os>-<arch>` archive from [GitHub Releases](https://github.com/olup/porcupine/releases). Use the release that matches your server. Project rename and deletion require v0.9.0 or later on both sides. Choose `darwin` for macOS, `linux` for Linux, or `windows` for Windows. Apple Silicon and other ARM64 systems use `arm64`. Intel and AMD 64-bit systems use `amd64`. Verify the archive against the release's `SHA256SUMS`, extract it and place `porcupine` or `porcupine.exe` on your PATH.

To build from the repository, run `make cli`. The executable is written to `bin/cli/porcupine`.

## Connect and query

Set the origin to match your deployment's OAuth issuer. Local HTTP is supported on loopback addresses. The default origin is `http://127.0.0.1:8080`.

```sh
export PORCUPINE_URL=https://analytics.example.com
porcupine status
porcupine login
porcupine list-projects
porcupine query-events --id PROJECT_ID --body '{"sql":"SELECT count() FROM events"}'
```

On Windows PowerShell, use `$env:PORCUPINE_URL = "https://analytics.example.com"`. You can also pass the origin before a command with `porcupine --url URL ...`.

On an email-enabled instance, login opens the browser for approval using OAuth with PKCE. The default client requests event and management read access. An instance administrator can run `porcupine login --admin` to authorize administration. Login needs the local callback port 43179. The latest login replaces the previous session for that origin. Tokens are refreshed on demand and stored in the user's configuration directory under `porcupine/credentials`.

For automation, set `PORCUPINE_API_KEY` from your secret manager to an [API-audience service key](../docs/service-keys.md). The CLI uses it ahead of any browser session and enforces its project grants. Service keys work on OAuth-enabled instances with SMTP disabled. The upstream OIDC login option is for read-only MCP clients. See [team setup](../docs/connect-your-team.md) to enable CLI browser login.

## Rename or delete a project

Use `porcupine list-projects` to find the project ID. Renaming requires `management:write` for the selected project.

```sh
porcupine rename-project --id PROJECT_ID --body '{"name":"Research"}'
```

For permanent deletion, use an instance administrator credential. With browser login, run `porcupine login --admin`, then `porcupine delete-project --id PROJECT_ID`. The command exits successfully when cleanup finishes. See [project lifecycle and cleanup scope](../docs/management-api.md#operations).

## Discover commands

Run `porcupine --help` for the catalog or `porcupine query-events --help` for parameters and body fields. Operation names match the HTTP API's OpenAPI operation IDs. JSON results go to stdout. An HTTP failure exits with a nonzero status and prints its status code and the API’s error message to stderr. Unreadable responses fall back to the HTTP status. Operations that issue access keys return secrets on stdout.

Pass JSON inline, use `--body @query.json` to read a file, or use `--body @-` to read from stdin. The binary upload operation streams bytes from a file or stdin. See [resumable imports](../docs/posthog-import.md) for manifests, checksums and chunk offsets.

## Develop

The server registers management operations with Huma. From the repository root, `make cli-generate` regenerates the command catalog, and `make cli-check` checks for drift and tests the CLI module. Commit the generated catalog with changes to the API. An operation that depends on an optional server capability appears in help even when that capability is disabled on the chosen instance.

Custom commands such as `login` and `status` register alongside the generated commands in `cli/cmd/porcupine/main.go`. Both are compiled into the same executable. The CLI module builds independently using the checked-in catalog.
