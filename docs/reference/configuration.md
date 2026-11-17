# Server configuration

**Unreleased.** These settings belong to the server executable, not the standalone API client. The published v0.14.0 image uses the older environment/Compose setup.

`porcupine init` creates `porcupine.json` with mode 0600 and random bootstrap and pipeline secrets. It refuses to overwrite an existing file. The server reads that file by default. `--config FILE` takes precedence over `PORCUPINE_CONFIG`. A missing explicitly selected file is an error. Environment settings on the server process override matching JSON values. A host shell's environment is not automatically passed into a container.

Unknown JSON keys, trailing JSON input, and unknown `PORCUPINE_` server variables are errors. `config check` validates local configuration and prints a redacted environment-key view. Database identity, provider discovery, and remote storage connectivity are checked at actual startup.

A fresh instance needs the generated bootstrap token. An already initialized instance may omit it; a configured token must contain at least 16 characters. Changing the configured token does not replace an existing stored credential. Preserve generated secrets when applying the [recommended laptop and VM profiles](../hosting.md#configure-the-development-build).

## Configuration sections

The generated file groups settings by purpose. Keep existing generated secrets when editing. Fields below are JSON paths; the detailed capability references retain their environment equivalents.

| Section | Common keys and purpose |
| --- | --- |
| Top level | `listen`, `bootstrap_token`, `allowed_origins`, `trusted_proxies` |
| `storage` | `journal`, `control`, `ducklake`: local paths; omit `control` for shared PostgreSQL |
| `queue` | `max_events`, `max_bytes`, `receipt_retention`; retention defaults to `"0"` to keep local receipts; a positive duration such as `"720h"` enables optional pruning |
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

`init` explicitly writes `queue.receipt_retention: "0"`. Optional pruning makes acceptance depend on DuckLake after a project's first prune. Changing back to zero stops pruning but does not restore missing receipts. See the [receipt contract](receipts.md#bounded-receipts).

When switching a generated local configuration to shared PostgreSQL, remove `storage.control` before setting `shared.control_database_url` or its file variant. A local control file and PostgreSQL control cannot both be configured. This setting does not migrate existing data.

`ducklake.s3`, `backup.s3`, and `export.s3` have distinct destinations. Common fields are `bucket`, `prefix`, `region`, `endpoint`, `path_style`, `access_key_id`, `secret_access_key`, and `session_token`. Empty explicit credentials use the configured AWS credential chain. Analytical and backup S3 also support `ca_cert_file` and `request_timeout_seconds`. A query destination does not configure analytical storage or backups.

## Secrets and container paths

A `_file` setting refers to a file readable by the server process. In Docker it is a container path; mount the secret explicitly. The image runs as UID 65532. Choose a value or its file variant, not both.

The initial `email.admin_email` gives that address instance administration through configured email or OIDC sign-in. It is an operator setting, not a tenant member invitation. [Sign-in setup](connect-your-team.md), [email delivery](email-transport.md), and [OIDC configuration](mcp-interactive-oauth.md) describe the required provider values.

## Container setup

Use a fresh data volume for the unreleased schema. Build and initialize the source checkout:

```sh
docker compose build porcupine
docker compose --profile setup run --rm init
docker compose --profile tools run --rm config-check
docker compose up -d porcupine
```

The private configuration is `/data/porcupine.json` in `porcupine-data`. `init --docker` selects the container listener and storage paths. Preserve those values when adapting a native example. The single Compose file has profiles for setup tools and optional PostgreSQL; providers use the same JSON file.

The published v0.14.0 image lacks these config commands. Do not apply its release overlay to this source-build setup. Use the [tagged release procedure](hosting-options.md) for that image.

## Replace an existing configuration

`porcupine config install --config FILE` reads a complete JSON replacement from stdin, validates it, and atomically installs it with mode 0600. Invalid input leaves the old file unchanged. It does not start or reconfigure a running server; restart after a successful install.

For a container-owned volume, stop the server and edit a private copy. Install it through the same nonroot server image:

```sh
umask 077
porcupine_config_copy=$(mktemp -d)
docker compose stop porcupine
docker compose cp porcupine:/data/porcupine.json "$porcupine_config_copy/porcupine.json"
chmod 600 "$porcupine_config_copy/porcupine.json"
# Edit $porcupine_config_copy/porcupine.json with your local editor.
docker compose run --rm --no-deps -T porcupine config install --config /data/porcupine.json < "$porcupine_config_copy/porcupine.json"
docker compose --profile tools run --rm config-check
docker compose up -d porcupine
```

A plain host copy can change ownership and make a private file unreadable to UID 65532. `config install` writes as the container user. Keep the copy outside source control, remove it when finished, and preserve the complete private configuration with recovery material.

## Interactive query ceilings

These deployment settings belong to the unreleased tenant implementation. A tenant budget can lower a ceiling but cannot raise it.

| JSON path | Environment equivalent | Default | Accepted range |
| --- | --- | --- | --- |
| `query.max_concurrent` | `PORCUPINE_QUERY_MAX_CONCURRENT` | 2 | 2–64 deployment analytical places |
| `query.timeout_ms` | `PORCUPINE_QUERY_TIMEOUT_MS` | 5,000 | 1–60,000 ms |
| `query.max_rows` | `PORCUPINE_QUERY_MAX_ROWS` | 1,000 | 1–100,000 rows |
| `query.max_result_bytes` | `PORCUPINE_QUERY_MAX_RESULT_BYTES` | 1,048,576 | 256–67,108,864 bytes |

An unset value uses its default. The concurrency pool also retains the background export safety slot. These settings do not change per-engine memory budgets or provide process isolation. Configure them within the host's measured resource capacity. Query readers discover their effective limits through `get-event-schema`. [Tenant budgets](tenants.md#interactive-query-limits)

Shared PostgreSQL also has [metadata timeouts and worker supervision](shared-deployment.md#native-timeouts-and-worker-supervision). Those I/O allowances can extend native execution beyond a SQL deadline. Shared workers retain execution-scoped reservations and quarantine affected interactive engines; automatic expiry can allow physical concurrency overshoot during a partition. Local mode has no shared reservation keeper.
