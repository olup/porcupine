# Host Porcupine 0.14.0

The [v0.14.0 release](https://github.com/olup/porcupine/releases/tag/v0.14.0) runs one server with embedded DuckDB and DuckLake. Local SSD storage is the default. A separate analytics database is not required.

Start with the [quickstart](../README.md#quickstart), which creates a private bootstrap secret, starts the matching release image, sends a test event, and waits for a SQL result. Keep the server and CLI versions matched.

## Pick a deployment

| Goal | Setup |
| --- | --- |
| Try it on a laptop | Docker Compose, loopback HTTP, persistent local volume |
| Run on one VM | The same server behind HTTPS, private API port, persistent SSD, backups |
| Keep analytical files in object storage | Configure [analytical S3](reference/ducklake-s3.md) before initializing a fresh archive |
| Run several workers | Unreleased [shared PostgreSQL mode](reference/shared-deployment.md), durable volumes, and S3 |

The [detailed hosting reference](reference/hosting-options.md) has the v0.14.0 Compose commands, offline image checks, a Caddy HTTPS example, and resource settings.

## Configure the development build

**Unreleased.** The development server adds `porcupine init` and `porcupine config check`. These are server setup commands, separate from the standalone API client. Build the current checkout before using them. The published v0.14.0 image does not contain them.

For a native server build:

```sh
make build
./bin/porcupine init --config porcupine.json
./bin/porcupine config check --config porcupine.json
./bin/porcupine --config porcupine.json
```

`init` creates a private JSON file with random bootstrap and pipeline secrets, and refuses to overwrite an existing file. Inspect and edit it privately. `config check` validates the configuration and masks secrets without opening databases or sending email. It does not prove that remote services are reachable. A fresh instance needs the generated bootstrap token; an already initialized instance may omit it. If provided, it must meet the server's minimum length.

The server loads `porcupine.json` by default. Select another file with `--config FILE` or `PORCUPINE_CONFIG`. Existing environment variables override matching file settings. Unknown JSON keys and unknown `PORCUPINE_` variables are errors.

The main sections group storage, queue, query, OAuth, email, OIDC, analytical S3, exports, backups, and shared metadata. See the [configuration key reference](reference/configuration.md). For example, these fields choose the local files and query ceilings inside the generated config:

```json
{
	"storage": {"journal": "./data/porcupine.db", "control": "./data/porcupine.db.control", "ducklake": "./data/ducklake"},
	"query": {"max_concurrent": 2, "timeout_ms": 5000, "max_rows": 1000, "max_result_bytes": 1048576}
}
```

Keep the generated secrets and other required settings when editing. Do not replace the whole configuration with this excerpt.

For Docker development, build the source first, then initialize the private configuration in the persistent volume:

```sh
docker compose build porcupine
docker compose --profile setup run --rm init
docker compose --profile tools run --rm config-check
docker compose up -d porcupine
```

The generated file is `/data/porcupine.json` in the `porcupine-data` volume. The one Compose file uses profiles for setup tools and optional PostgreSQL. Email, S3, and backups use external providers configured in the same JSON file.

To change the container's configuration, keep a private local copy. Stop the server before replacing the file, edit the copy, and copy it back before checking and starting:

```sh
umask 077
porcupine_config_copy=$(mktemp -d)
docker compose stop porcupine
docker compose cp porcupine:/data/porcupine.json "$porcupine_config_copy/porcupine.json"
chmod 600 "$porcupine_config_copy/porcupine.json"
# Edit the file at $porcupine_config_copy/porcupine.json with your local editor.
docker compose run --rm --no-deps -T porcupine config install --config /data/porcupine.json < "$porcupine_config_copy/porcupine.json"
docker compose --profile tools run --rm config-check
docker compose up -d porcupine
```

`config install` validates stdin and replaces the private file atomically as the container user. Invalid input leaves the previous file in place. Preserve generated secrets when editing. Keep the private copy outside source control and remove it when no longer needed. The image runs as user 65532, so any host-mounted private file must be readable by that container user. Preserve the configuration with recovery material. Do not apply the v0.14.0 release overlay to this source-build setup; that published image does not understand the new config commands.

Use the quickstart's tagged checkout for the older v0.14.0 Compose workflow.

## Choose local SQLite or shared PostgreSQL (unreleased)

Both modes use the same server binary and support tenants.

| State | One server | Shared workers |
| --- | --- | --- |
| Accounts, grants, tenants, budgets | Separate local control SQLite | Shared PostgreSQL |
| Accepted events, receipts, identities, jobs | Local journal SQLite | Project owner's local journal SQLite |
| DuckLake catalogs | Local catalogs | PostgreSQL metadata |
| Analytical event files | Local SSD or S3 | S3 Parquet |

Local mode has two database files. `PORCUPINE_DB` selects the journal, default `./data/porcupine.db`. `PORCUPINE_CONTROL_DB` selects control, default the journal path plus `.control`. Preserve both with their auxiliary files and DuckLake state.

When selecting shared PostgreSQL, remove `storage.control` from a generated local config before setting `shared.control_database_url` or its file variant. A local control file and PostgreSQL control cannot both be configured.

PostgreSQL replaces control and catalog metadata. Event payloads still commit to the owner's SQLite queue, then go to S3 Parquet. No PostgreSQL event inbox is introduced. DuckLake catalog statistics may still contain derived event values, so the catalog belongs inside the analytical data's hosting and access boundary. Shared mode has no automatic owner failover; acknowledged work depends on recovering that owner's volume.

For one server, keep SQLite. PostgreSQL is not required for tenant budgets, people, the API, CLI, or MCP. A fresh development installation creates the tenant `default`; [invite its first administrator](access.md).

## Keep data and resources safe

Run one server process per local data directory. Preserve the complete `/data` volume and private configuration across container replacements. The image runs as user 65532; bind mounts must be writable by that user. Do not share local catalogs or SQLite files between writers or put them on a network filesystem.

The default Compose memory limit is 2 GiB. It is a test profile, not a hosted capacity guarantee. Per-project DuckDB memory settings do not cap total process RSS. Size for active project engines, query concurrency, ingestion, maintenance, and the operating system together.

`/readyz` checks SQLite. Before routing events, check the authenticated [instance report](reference/instance-status.md), capture a synthetic event, and verify SQL visibility. Watch pending-event age, queue bytes, disk space, memory pressure, and delivery errors.

## Configure access and backups

Keep the bootstrap secret privately. Configure [email or Google/OIDC sign-in](reference/connect-your-team.md), then [invite people and create service credentials](access.md). Expose the public service over HTTPS and keep the application port private.

Configure [backups](backup-and-restore.md) before storing important data. Record the image digest and complete configuration before an upgrade. Preserve a matching checkpoint; cross-version rollback is not guaranteed.

The unreleased tenant-enabled schema requires fresh storage. It rejects pre-tenant databases instead of silently assigning their projects to a tenant. Subsequent recognized schemas use numbered migrations and refuse newer or unknown databases before changing them. See [storage versions](storage.md#unreleased-schema-versions).
