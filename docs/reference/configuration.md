# Server configuration

These settings belong to the server executable. The standalone API client has its own connection settings.

`porcupine init` creates `porcupine.json` with mode 0600 and random bootstrap and pipeline secrets. It refuses to overwrite an existing file. The server reads that file by default. `--config FILE` takes precedence over `PORCUPINE_CONFIG`. A missing explicitly selected file is an error. Environment settings on the server process override matching JSON values. A host shell's environment is not automatically passed into a container.

Unknown JSON keys, trailing JSON input, and unknown `PORCUPINE_` server variables are errors. `config check` validates local configuration and prints a redacted environment-key view. It does not open databases or contact remote providers. Startup checks the configured database and analytical registry. Confirm analytical S3 access with an ingestion and query before routing traffic.

A fresh builtin instance needs the generated bootstrap token. An already initialized builtin instance may omit it; a configured token must contain at least 16 characters. Changing the configured token does not replace an existing stored credential. [External authentication](external-auth.md) instead uses a trusted operator JWT and rejects bootstrap, SMTP, and builtin provider settings. Preserve generated secrets when applying the [recommended laptop and VM profiles](../hosting.md#configure-the-development-build).

## Configuration sections

The generated file groups settings by purpose. Keep existing generated secrets when editing. Fields below are JSON paths; the detailed capability references retain their environment equivalents.

| Section | Common keys and purpose |
| --- | --- |
| Top level | `profile`, `listen`, `bootstrap_token`, `allowed_origins`, `trusted_proxies`, `geoip_database` |
| `metrics` | `enabled`, `listen`: private Prometheus listener. Requires a checkout with [operator monitoring](observability.md). |
| `acceptance` | `mode` confirms the acknowledgment policy. Standalone defaults to `"durable"`. Hosted requires explicit `"buffered"`. |
| `storage` | `journal`, `control`, `ducklake`: standalone paths. Hosted accepts a private disposable `ducklake` path. |
| `queue` | `max_events`, `max_bytes`, `receipt_retention`; retention defaults to `"0"` to keep local receipts; a positive duration such as `"720h"` enables optional pruning |
| `query` | `max_concurrent`, `timeout_ms`, `max_rows`, `max_result_bytes`: deployment ceilings |
| `usage` | `retention_days` (365), `max_snapshots` (16), `max_snapshot_bytes` (134217728): [numerical history and report capacity](usage.md) |
| `oauth` | `issuer`, `token_lifetime_seconds`, `allow_legacy_keys`, `restore_identity_bootstrap` |
| `auth` | `mode` defaults to `builtin`; `external` selects a resource server with `public_url` and the explicit issuer, JWKS, audiences, algorithms, token types, clock tolerance, and token lifetime in `auth.external`. See the [external contract](external-auth.md). |
| `email` | `admin_email`, `host`, `port`, `tls`, `from`, `username`, `password_file` |
| `oidc` | `issuer`, `client_id`, and either `client_secret` or `client_secret_file` |
| `ducklake` | `memory_mb`, `threads`, `max_open_projects`, maintenance/cleanup settings, `postgres_url` or `postgres_url_file`, and analytical `s3` |
| `shared` | `control_database_url` or `control_database_url_file`: hosted PostgreSQL connection |
| `pipeline` | `key` or `key_file` for destination encryption, plus allowed `s3_endpoints` |
| `export` | Separate export-result `s3` settings and job/output limits |
| `backup` | `directory` or separate backup `s3`, plus `interval`, `timeout`, and `retain` |

`PORCUPINE_PROFILE` and `PORCUPINE_ACCEPTANCE_MODE` override `profile` and
`acceptance.mode`. Buffered acknowledgment requires both `profile: "hosted"`
and `acceptance.mode: "buffered"`. See the [hosted configuration](shared-deployment.md#configuration)
for PostgreSQL, object storage, and recovery-floor settings.

In standalone the generated control path is the journal path plus `.control`. See [storage by profile](../storage.md#deployment-profiles).

`init` explicitly writes `queue.receipt_retention: "0"`. Optional pruning makes acceptance depend on DuckLake after a project's first prune. Changing back to zero stops pruning but does not restore missing receipts. See the [receipt contract](receipts.md#bounded-receipts).

`ducklake.s3`, `backup.s3`, and `export.s3` have distinct destinations. Common fields are `bucket`, `prefix`, `region`, `endpoint`, `path_style`, `access_key_id`, `secret_access_key`, and `session_token`. Empty explicit credentials use the configured AWS credential chain. Analytical and backup S3 also support `ca_cert_file` and `request_timeout_seconds`. A query destination does not configure analytical storage or backups.

## Browser ingestion

Native, PostHog, and Segment capture accept cross-origin browser requests from any origin by default. Browsers can call these endpoints without including cookies. Use a `mixed` project's public ingestion key. Project authentication applies to every write.

To restrict browser capture across the instance, set `allowed_origins` to an array of exact HTTP or HTTPS origins, such as `["https://app.example.com", "http://localhost:3000"]`. The environment override `PORCUPINE_ALLOWED_ORIGINS` takes a comma-separated list. Omit the setting or use an empty array for the default. The setting applies only to capture endpoints.

### Country lookup

Set `geoip_database` (or `PORCUPINE_GEOIP_DATABASE`) to a local MMDB file, for example
`/data/GeoLite2-Country.mmdb`. Country and City databases are supported if they provide
`country.iso_code`. Leave the setting empty to disable country lookup. It applies to
eligible PostHog, native, and Segment capture on that server, including cookieless events.
[Eligibility and privacy](../data-and-identity.md#optional-country-lookup)

Supply the database under its provider's license. Porcupine reads the file into memory
at startup and performs lookups locally. For containers, mount the file read-only and
make it readable by the server user. Put the same database on every hosted ingestion
worker and restart the workers after updating it. If the file is missing or invalid,
Porcupine logs a startup warning and continues accepting events without country lookup.
[MaxMind database updates](https://dev.maxmind.com/geoip/updating-databases/)

## Secrets and container paths

A `_file` setting refers to a file readable by the server process. In Docker it is a container path; mount the secret explicitly. The image runs as UID 65532. Choose a value or its file variant, not both.

The initial `email.admin_email` gives that address instance administration through configured email or OIDC sign-in. It is an operator setting, not a tenant member invitation. [Sign-in setup](connect-your-team.md), [email delivery](email-transport.md), and [OIDC configuration](mcp-interactive-oauth.md) describe the required provider values.

## Container setup

Follow the [tagged Compose quickstart](../../README.md#local-setup) to initialize the volume, validate the configuration, and start the server.

The private configuration is `/data/porcupine.json` in `porcupine-data`. `init --docker` selects the container listener and storage paths. Preserve those values when adapting a native example. The Compose example runs standalone. For hosted workers use the [hosted runbook](shared-deployment.md#configuration).

For a source build, run `docker compose build porcupine` first and omit the release overlay. For an existing v0.14.0 volume, use the [fresh-storage cutover procedure](../upgrades.md#move-from-v0140).

## Replace an existing configuration

`porcupine config install --config FILE` reads a complete JSON replacement from stdin, validates it, and atomically installs it with mode 0600. Invalid input leaves the old file unchanged. It does not start or reconfigure a running server; restart after a successful install.

For a container-owned volume, stop the server and edit a private copy. Install it through the same nonroot server image:

```sh
umask 077
porcupine_config_copy=$(mktemp -d)
docker compose -f compose.yaml -f compose.release.yaml stop porcupine
docker compose -f compose.yaml -f compose.release.yaml cp porcupine:/data/porcupine.json "$porcupine_config_copy/porcupine.json"
chmod 600 "$porcupine_config_copy/porcupine.json"
# Edit $porcupine_config_copy/porcupine.json with your local editor.
docker compose -f compose.yaml -f compose.release.yaml run --rm --no-deps -T porcupine config install --config /data/porcupine.json < "$porcupine_config_copy/porcupine.json"
docker compose -f compose.yaml -f compose.release.yaml --profile tools run --rm config-check
docker compose -f compose.yaml -f compose.release.yaml up -d porcupine
```

A plain host copy can change ownership and make a private file unreadable to UID 65532. `config install` writes as the container user. Keep the copy outside source control, remove it when finished, and preserve the complete private configuration with recovery material.

## Interactive query ceilings

A tenant budget can lower a deployment ceiling.

| JSON path | Environment equivalent | Default | Accepted range |
| --- | --- | --- | --- |
| `query.max_concurrent` | `PORCUPINE_QUERY_MAX_CONCURRENT` | 2 | 2–64 analytical slots per server process |
| `query.timeout_ms` | `PORCUPINE_QUERY_TIMEOUT_MS` | 5,000 | 1–60,000 ms |
| `query.max_rows` | `PORCUPINE_QUERY_MAX_ROWS` | 1,000 | 1–100,000 rows |
| `query.max_result_bytes` | `PORCUPINE_QUERY_MAX_RESULT_BYTES` | 1,048,576 | 256–67,108,864 bytes |

An unset value uses its default. The pool includes the slot used by [exports and scheduled S3 queries](export-api.md#consistency-retries-and-revocation). These settings do not change per-engine memory budgets or provide process isolation. Configure them within the host's measured resource capacity. Query readers discover their effective limits through `get-event-schema`. [Tenant budgets](tenants.md#interactive-query-limits)

The hosted profile also has [metadata timeouts and worker supervision](shared-deployment.md#native-timeouts-and-worker-supervision). Those I/O allowances can extend native execution beyond a SQL deadline. Hosted workers retain execution-scoped reservations and quarantine affected interactive engines; automatic expiry can allow physical concurrency overshoot during a partition. Local mode has no shared reservation keeper.
