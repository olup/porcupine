# Storage

Porcupine embeds DuckDB and DuckLake. Each project has its own DuckLake catalog and managed analytical files.

<a id="hosted-profile"></a>

## Deployment profiles

| Profile | Control and catalogs | Analytical files | Live acknowledgment |
| --- | --- | --- | --- |
| Standalone | Local SQLite control and journal, with local DuckLake catalogs | Local SSD by default, or a configured private S3 bucket and prefix | Durable after the SQLite journal commits |
| Hosted | Shared PostgreSQL control and DuckLake catalogs, with disposable worker scratch space | Configured private S3 bucket and prefix | Best-effort admission to bounded worker memory |

A hosted worker crash or incomplete drain can lose acknowledged events before the DuckLake commit. The flush interval does not bound loss during an outage. Hosted imports and explicit mutations use durable shared checkpoints and staged objects. See the [native acknowledgment contract](reference/native-event-contract.md#unreleased-acknowledgment-metadata) and [hosted setup](design/hosted-architecture.md#configuration).

## Local files or S3

For standalone, use one server process per local data directory. Keep both SQLite databases and their auxiliary files, the DuckLake registry, and catalogs on persistent local storage. Keep local analytical files and private configuration with that state. Keep SQLite files and local catalogs off network filesystems such as NFS.

A fresh standalone deployment can place analytical files on [S3](reference/ducklake-s3.md). Queries use the same project-scoped DuckDB SQL with local or remote files. Select S3 before archive initialization. Changing the setting cannot move an existing archive or move older data to S3 automatically.

Analytical storage, [destination pipelines](pipelines.md), [one-time exports](exports.md), and [backup destinations](reference/native-backup-recovery.md) are separate. A scheduled query delivered to S3 is an external copy, not the analytical archive or an instance backup.

Storage identity is pinned. An unexpected registry, catalog, bucket, or prefix change fails rather than opening an empty replacement. Project agents cannot change deployment storage settings or retrieve storage credentials through HTTP or MCP.

## Separate control and journal {#unreleased-separate-control-and-journal}

Standalone uses two SQLite databases. Control holds accounts, grants, tenants, projects, and policies. The journal holds accepted live events, retry receipts, identity state, imports, and jobs. See [server configuration](reference/configuration.md#configuration-sections) for the paths.

Preserve both databases, their SQLite auxiliary files, and DuckLake together. Startup rejects missing or mismatched database identities. Native v2 backups contain `acceptance.sqlite` and `control.sqlite`. Moving from v0.14.0 requires a fresh data directory. [Upgrade steps](hosting.md#move-from-v0140)

## Schema versions {#unreleased-schema-versions}

Each operational database records a schema version: SQLite in `PRAGMA user_version`, shared PostgreSQL control in `metadata.schema_version`. On start, the server applies the missing numbered steps in one transaction. It refuses a database written by a newer release, and an unversioned database it does not recognize, before changing anything. A native backup restores at its own version; the next start upgrades it.

Each project DuckLake catalog records its own version in `lake.schema_version`. Version 2 drops the per-row copy of the whole event, keeps an original snapshot only for corrected events, and partitions current events by year and month of occurrence. Opening an older catalog upgrades it in place; a catalog from a newer release is refused.

## Retention and deletion

Live events have no automatic age limit. Edits and deletions change the current analytical state once projected. Payload-free heads preserve revision and deletion facts so a late original cannot resurrect deleted data.

DuckLake snapshots can retain old payloads. Porcupine schedules deleted-row rewrites, snapshot expiration, and obsolete-file cleanup. The default policy keeps seven days of snapshots and waits two days before removing obsolete files. Active readers, backup pins, and maintenance backlogs can delay reclamation. See [history cleanup](reference/ducklake-cleanup.md) for settings and progress.

Hosted retention must protect PostgreSQL recovery points and active readers. Physical reclamation waits for the configured grace and the operator’s verified recovery floor. [Hosted recovery requirements](design/hosted-architecture.md#recovery-and-retention)

Retry receipts are separate from payload history and stay in SQLite indefinitely by default. Journal deployments can opt into pruning, which introduces an analytical dependency after the first prune. The [receipt reference](reference/receipts.md) describes that tradeoff and the effect of changing retention back to zero.

Logical deletion does not promise physical erasure within 30 days. SQLite free pages or WAL, snapshots, S3 noncurrent versions, backups, and external copies can retain earlier payloads. Their retention policies need to match the operator's requirements.

In standalone, whole-project deletion removes its SQLite state and managed analytical directory or exact S3 prefix. In hosted, deletion fences access before eventual reclamation. Cleanup is durable and retried after failures. Other projects remain separate. Existing backups and copies already delivered to customers are outside this deletion scope. [Project lifecycle](reference/projects.md)

## Recovery

Use [coordinated native backups](reference/native-backup-recovery.md) for local or analytical S3 deployments. A local bundle scheduler alone is insufficient for analytical S3 because it does not include remote objects. Complete S3 backups use an independent destination and verify the copied catalog references and data inventory before publication.

Hosted recovery coordinates PostgreSQL point-in-time recovery, versioned S3 objects, and deployment secrets. Recover into an isolated environment before replacing the live deployment. [Hosted recovery requirements](design/hosted-architecture.md#recovery-and-retention)

A local-only deployment can also use a [stopped-volume copy](reference/restore.md). Copy the entire data volume after its single writer stops. A live copy of selected SQLite or catalog files is not a consistent recovery point. An S3 bucket listing alone is not a recovery procedure.

Keep private deployment configuration, signing and encryption keys, and the matching image version independently. Recover into isolated fresh storage and verify counts, identity, revisions, deletions, and retries before reconnecting clients.

## Earlier analytical backends

Version v0.14.0 supports DuckDB/DuckLake only. It does not automatically convert an older external-database deployment or its SQL. Preserve an older deployment and its backups until you have exported and imported the required history into a fresh native instance. The historical implementation is retained on `reference/clickhouse-before-ducklake`; its old operator settings are not supported by the native server.
