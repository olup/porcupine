# Porcupine CLI

Use `porcupine` to query an instance, manage projects and upload import chunks from the terminal. It is a standalone HTTP client. Its help works offline.

## Install

Choose the `porcupine-cli-<tag>-<os>-<arch>` archive from [GitHub Releases](https://github.com/olup/porcupine/releases). Use the release that matches your server. Choose `darwin` for macOS, `linux` for Linux, or `windows` for Windows. Apple Silicon and other ARM64 systems use `arm64`. Intel and AMD 64-bit systems use `amd64`. Verify the archive against the release's `SHA256SUMS`, extract it and place `porcupine` or `porcupine.exe` on your PATH.

To build from the repository, run `make cli`. The executable is written to `bin/cli/porcupine`.

## Connect and query

The CLI connects to Porcupine Cloud at `https://api.getporcupine.eu` by default. Sign in with the email address invited to your team.

```sh
porcupine status
porcupine login
porcupine list-projects
porcupine query-events --id PROJECT_ID --body '{"sql":"SELECT count() FROM events"}'
```

If `porcupine --help` shows a local default, set `PORCUPINE_URL=https://api.getporcupine.eu` and use `porcupine login --client-id porcupine-cloud-cli`.

Use `porcupine get-instance-status` for an authenticated report of dependency health and optional service configuration. The `status` command checks public readiness. See [instance status](../docs/reference/instance-status.md).

Use `porcupine visibility-probe` to send a synthetic event and measure how long it takes to become queryable after capture acknowledgment. [Visibility probe setup](../docs/reference/observability.md#measure-live-query-visibility)

For a self-hosted instance, set `PORCUPINE_URL` to its API origin, or pass `--url` before the command. The flag takes precedence over the environment variable. HTTP is supported on loopback addresses.

```sh
export PORCUPINE_URL=https://analytics.example.com
porcupine login
```

On Windows PowerShell, use `$env:PORCUPINE_URL = "https://analytics.example.com"`. To sign in to a local instance for one command, run `porcupine --url http://127.0.0.1:8080 login`.

Login opens the browser for approval using OAuth with PKCE. The default client requests event and management read access. An instance administrator can run `porcupine login --admin` to authorize administration. Login needs the local callback port 43179. The latest login replaces the previous session for that origin. Tokens are refreshed on demand and stored in the user's configuration directory under `porcupine/credentials`.

For automation, set the destination explicitly with `PORCUPINE_URL` or `--url` and load an [API-audience service key](../docs/reference/service-keys.md) into `PORCUPINE_API_KEY` from your secret manager. The CLI uses the key ahead of any browser session and enforces its project grants. Service keys work on OAuth-enabled instances with SMTP disabled. [Google or another trusted OIDC provider](../docs/reference/mcp-interactive-oauth.md) can also authenticate invited humans for CLI login, using the same account permissions as email OTP. See [team setup](../docs/reference/connect-your-team.md).

Use `porcupine login --tenant-admin` to authorize administration of one tenant, or `porcupine login --scopes 'events:read events:write management:read'` for selected project permissions. The default remains read-only. Project scopes cannot be combined with `--admin` or `--tenant-admin`, and requesting a scope never adds rights to the human account.

Porcupine Cloud selects its registered public client automatically. For another server configured with [external authentication](../docs/reference/external-auth.md), use `porcupine login --client-id YOUR_EXTERNAL_PUBLIC_CLIENT`. Register that public client with the external issuer. The CLI discovers its endpoints, keeps the API resource through exchange and refresh, and requires PKCE and issuer identification in the authorization response. A session without a refresh token works until its access token expires, then requires another login.

## Import event history

Import native, PostHog, and Segment JSONL files or directories in one command:

```sh
porcupine import --project PROJECT_ID --format native ./events.jsonl
porcupine import --project PROJECT_ID --format posthog ./posthog-export/
porcupine import --project PROJECT_ID --format segment ./events.jsonl.gz
```

The credential needs `imports:write` and `events:write` on the destination project. The CLI streams gzip, checks source IDs and dates, manages checksums and chunks, resumes server offsets, and waits for analytical delivery. Rerun the same command to resume, or select a job with `--resume IMPORT_ID`. Use `--no-wait` for durable acceptance only. Progress goes to stderr and the final JSON summary to stdout. [Import guide](../docs/imports.md)

## Rename or delete a project

Use `porcupine list-projects` to find the project ID. Renaming requires `management:write` for the selected project.

```sh
porcupine rename-project --id PROJECT_ID --body '{"name":"Research"}'
```

To delete a project, sign in with `porcupine login --tenant-admin` for the owning tenant or `porcupine login --admin` for the instance. Run `porcupine delete-project --id PROJECT_ID`. See [project deletion and cleanup](../docs/reference/projects.md#delete-a-project).

## Discover commands

Run `porcupine --help` for the catalog or `porcupine query-events --help` for parameters and body fields. Operation names match the HTTP API's OpenAPI operation IDs. JSON results go to stdout. An HTTP failure exits with a nonzero status and prints its status code and the API’s error message to stderr. Unreadable responses fall back to the HTTP status. Operations that issue access keys return secrets on stdout.

Pass JSON inline, use `--body @query.json` to read a file, or use `--body @-` to read from stdin. The binary upload operation streams bytes from a file or stdin. See [resumable imports](../docs/reference/posthog-import.md) for manifests, checksums and chunk offsets.

Run `porcupine get-event-profile --id PROJECT_ID --event EVENT_NAME` to inspect activity, recent examples, and sampled property coverage before writing SQL. See [event profiles](../docs/reference/event-discovery.md) for sampling and payload limits.

Use `porcupine get-event-catalog --id PROJECT_ID` to find missing or stale event descriptions. See [how to review catalog documentation](../docs/reference/event-discovery.md#review-catalog-documentation) for property discovery and updates with `set-catalog-description`.

`porcupine restore` is an offline operation that runs the matching native server image through Docker. It restores into a fresh volume and resets old access and delivery jobs. Builtin mode reads its recovery secret from a private file. External mode uses `--auth-mode external` without `--bootstrap-token-file`, then recovers administration through an external operator JWT. Archive and destination modes must match. See [remote backups and recovery](../docs/reference/native-backup-recovery.md). The CLI itself still has no native database dependency.

## Develop

The server registers management operations with Huma. From the repository root, `make cli-generate` regenerates the command catalog, and `make cli-check` checks for drift and tests the CLI module. Commit the generated catalog with changes to the API. An operation that depends on an optional server capability appears in help even when that capability is disabled on the chosen instance.

Custom commands such as `login`, `status`, historical `import`, and offline `restore` register alongside generated commands in `cli/cmd/porcupine/main.go`. They compile into the same executable. The CLI module builds independently using the checked-in catalog.
