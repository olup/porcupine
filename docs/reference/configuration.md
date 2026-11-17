# Server configuration

**Unreleased.** These settings belong to the server executable, not the standalone API client. The published v0.14.0 image uses the older environment/Compose setup.

`porcupine init` creates `porcupine.json` with private permissions and random bootstrap/pipeline secrets. The server reads that file by default. `--config FILE` takes precedence over `PORCUPINE_CONFIG`. A missing explicitly selected file is an error. Environment settings on the server process override matching JSON values. A host shell's environment is not automatically passed into a container.

Unknown JSON keys, trailing JSON input, and unknown `PORCUPINE_` server variables are errors. `config check` validates local configuration and prints a redacted environment-key view. Database identity, provider discovery, and remote storage connectivity are checked at actual startup.

## Configuration sections

The generated file groups settings by purpose. Keep existing generated secrets when editing. Fields below are JSON paths; the detailed capability references retain their environment equivalents.

| Section | Common keys and purpose |
| --- | --- |
| Top level | `listen`, `bootstrap_token`, `allowed_origins`, `trusted_proxies` |
| `storage` | `journal`, `control`, `ducklake`: local paths; omit `control` for shared PostgreSQL |
| `queue` | `max_events`, `max_bytes`, `receipt_retention`; retention is a duration string, default `"720h"`, or `"0"` to stop future pruning |
| `query` | `max_concurrent`, `timeout_ms`, `max_rows`, `max_result_bytes`: deployment ceilings |
| `oauth` | `issuer`, `token_lifetime_seconds`, `allow_legacy_keys`, `restore_identity_bootstrap` |
| `email` | `admin_email`, `host`, `port`, `tls`, `from`, `username`, `password_file` |
| `oidc` | `issuer`, `client_id`, and either `client_secret` or `client_secret_file` |
| `ducklake` | `memory_mb`, `threads`, `max_open_projects`, maintenance/cleanup settings, `postgres_url` or `postgres_url_file`, and analytical `s3` |
| `shared` | `control_database_url` or its `_file` variant, `worker_id`, `worker_url`, and `routing_secret` or its `_file` variant |
| `pipeline` | `key` or `key_file` for destination encryption, plus allowed `s3_endpoints` |
| `export` | Separate export-result `s3` settings and job/output limits |
| `backup` | `directory` or separate backup `s3`, plus `interval`, `timeout`, and `retain` |

The default generated local control path is the journal path plus `.control`. Shared mode requires PostgreSQL control, PostgreSQL DuckLake catalogs, analytical S3, and a separate local journal per worker. See [shared deployment](shared-deployment.md).

`ducklake.s3`, `backup.s3`, and `export.s3` have distinct destinations. Common fields are `bucket`, `prefix`, `region`, `endpoint`, `path_style`, `access_key_id`, `secret_access_key`, and `session_token`. Empty explicit credentials use the configured AWS credential chain. Analytical and backup S3 also support `ca_cert_file` and `request_timeout_seconds`. A query destination does not configure analytical storage or backups.

## Secrets and container paths

A `_file` setting refers to a file readable by the server process. In Docker it is a container path; mount the secret explicitly. The image runs as UID 65532. Choose a value or its file variant, not both.

The initial `email.admin_email` gives that address instance administration through configured email or OIDC sign-in. It is an operator setting, not a tenant member invitation. [Sign-in setup](connect-your-team.md), [email delivery](email-transport.md), and [OIDC configuration](mcp-interactive-oauth.md) describe the required provider values.

## Replace an existing configuration

`porcupine config install --config FILE` reads a complete JSON replacement from stdin, validates it, and atomically installs it with mode 0600. Invalid input leaves the old file unchanged. It does not start or reconfigure a running server; restart after a successful install.

For a container-owned volume, run installation through the same nonroot server image. A plain copy from the host can change ownership and make a private file unreadable. The [hosting workflow](../hosting.md#configure-the-development-build) supplies the tested commands.
