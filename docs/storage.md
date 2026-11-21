# Storage

Porcupine embeds DuckDB and DuckLake. Each project has its own DuckLake catalog and managed analytical files.

<a id="hosted-profile"></a>

## Deployment profiles

| Profile | Control and catalogs | Analytical files | Live acknowledgment |
| --- | --- | --- | --- |
| Standalone | Local SQLite control and journal, with local DuckLake catalogs | Local SSD by default, or a configured private S3 bucket and prefix | Durable after the SQLite journal commits |
| Hosted | Shared PostgreSQL control and DuckLake catalogs, with disposable worker scratch space | Configured private S3 bucket and prefix | Best-effort admission to bounded worker memory |

A hosted worker crash or incomplete drain can lose acknowledged events before they commit. See the [live acknowledgment contract](reference/native-event-contract.md#acknowledgment-metadata) and [durable checkpoints for hosted imports and mutations](design/hosted-architecture.md#imports-and-mutations).

## Local files or S3

For standalone, use one server process per local data directory. Keep both SQLite databases and their auxiliary files, the DuckLake registry, and catalogs on persistent local storage. Keep local analytical files and private configuration with that state. Keep SQLite files and local catalogs off network filesystems such as NFS.

A fresh standalone deployment can [store analytical files on S3](reference/ducklake-s3.md#configure-a-new-deployment). Queries use the same project-scoped DuckDB SQL for local and remote files.

Analytical storage, [destination pipelines](pipelines.md), [one-time exports](exports.md), and [backup destinations](reference/native-backup-recovery.md) are separate. A scheduled query delivered to S3 is an external copy, not the analytical archive or an instance backup.

Storage identity is pinned. An unexpected registry, catalog, bucket, or prefix change fails rather than opening an empty replacement. Project agents cannot change deployment storage settings or retrieve storage credentials through HTTP or MCP.

## Encrypt analytical Parquet files

Set `ducklake.encrypted` before initializing a new analytical store in either profile. DuckLake encrypts the Parquet data and delete files and stores the keys in each project's catalog. A copy of the S3 analytical objects or local `files/` directory alone lacks the keys. The server uses the catalog to decrypt files for queries.

In a default standalone installation, the DuckLake directory holds both the project catalogs and their `files/` directories. Protect the whole directory, SQLite journal, and control database separately. Catalog metadata can contain plaintext column statistics. Backups that include both files and catalogs also contain everything needed to read them.

One-time exports and scheduled S3 pipeline outputs remain readable in their configured formats. They are user deliverables and follow the destination's access and encryption policy. [Configuration and migration options](reference/configuration.md#ducklake-parquet-encryption)

<a id="unreleased-separate-control-and-journal"></a><!-- Legacy URL fragment. -->

## Separate control and journal

Standalone uses two SQLite databases. Control holds accounts, grants, tenants, projects, and policies. The journal holds accepted live events, retry receipts, identity state, imports, and jobs. See [server configuration](reference/configuration.md#configuration-sections) for the paths.

Startup rejects missing or mismatched database identities. See [native bundle contents](reference/native-backup-recovery.md#inspect-a-backup) and [upgrade steps for v0.14.0](hosting.md#move-from-v0140).

<a id="unreleased-schema-versions"></a><!-- Legacy URL fragment. -->

## Schema versions

Each operational database records a schema version: SQLite in `PRAGMA user_version`, shared PostgreSQL control in `metadata.schema_version`. On start, the server applies the missing numbered steps in one transaction. It refuses a database written by a newer release, and an unversioned database it does not recognize, before changing anything. A native backup restores at its own version; the next start upgrades it.

Each project DuckLake catalog records its own version in `lake.schema_version`. Version 2 drops the per-row copy of the whole event, keeps an original snapshot only for corrected events, and partitions current events by year and month of occurrence. Opening an older catalog upgrades it in place; a catalog from a newer release is refused.

## Retention and deletion

Live events have no automatic age limit. Edits and deletions change the current analytical state once projected. Payload-free heads preserve revision and deletion facts so a late original cannot resurrect deleted data.

DuckLake snapshots can retain old payloads. Porcupine schedules [deleted-row rewrites, snapshot expiration, and obsolete-file cleanup](reference/ducklake-cleanup.md#deployment-policy). Active readers, backup pins, and maintenance backlogs can delay reclamation.

Hosted reclamation depends on the [cleanup grace and verified recovery floor](reference/ducklake-cleanup.md#hosted-profile).

SQLite keeps retry receipts indefinitely by default. [Optional receipt pruning](reference/receipts.md#bounded-receipts) changes the storage required for deduplication.

Logical deletion does not promise physical erasure within 30 days. SQLite free pages or WAL, snapshots, S3 noncurrent versions, backups, and external copies can retain earlier payloads. Their retention policies need to match the operator's requirements.

Whole-project deletion removes managed data. See [project cleanup and retries](reference/projects.md#delete-a-project) and [deletion of managed S3 objects](reference/ducklake-s3.md#isolation-and-cleanup).

## Recovery

Choose the [backup and restore method](backup-and-restore.md#choose-a-method) for your deployment. If analytical files are on S3, the recovery point must include the referenced objects as well as their catalogs and operational state.

## Earlier analytical backends

To move from ClickHouse, set up a fresh DuckLake instance, convert any ClickHouse SQL you still use, and follow the [historical import guide](imports.md). Keep the older deployment and its backups until you have verified the imported history. The old implementation remains on the `reference/clickhouse-before-ducklake` branch.
