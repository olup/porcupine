# Storage

Porcupine embeds DuckDB and DuckLake. By default, SQLite holds operational state and accepted work, and each project has its own local DuckLake catalog and managed analytical files. Run one application writer per SQLite database and DuckLake root.

## Local files or S3

Local SSD is the default. Keep SQLite, the DuckLake registry, project catalogs, and Parquet files on persistent storage. No external database or S3 account is required.

A fresh deployment can place analytical data files on [S3](reference/ducklake-s3.md). SQLite and catalogs remain local. Queries use the same project-scoped DuckDB SQL whether the files are local or remote. S3 is selected before archive initialization; changing configuration does not migrate an existing archive or enable age-based SSD-to-S3 tiering.

Analytical storage, [destination pipelines](pipelines.md), [one-time exports](exports.md), and [backup destinations](reference/native-backup-recovery.md) are separate. A scheduled query delivered to S3 is an external copy, not the analytical archive or an instance backup.

Storage identity is pinned. An unexpected registry, catalog, bucket, or prefix change fails rather than opening an empty replacement. Project agents cannot change deployment storage settings or retrieve storage credentials through HTTP or MCP.

## Separate control and journal {#unreleased-separate-control-and-journal}

Standalone uses two SQLite databases. Control holds accounts, grants, tenants, projects, and policies. The journal holds accepted live events, retry receipts, identity state, imports, and jobs. The default control database path is the journal path plus `.control`.

Preserve both databases, their SQLite auxiliary files, and DuckLake together. Startup rejects missing or mismatched database identities. Native v2 backups contain `acceptance.sqlite` and `control.sqlite`. Moving from v0.14.0 requires a fresh data directory. [Upgrade steps](hosting.md#move-from-v0140)

## Hosted profile

The opt-in hosted profile keeps control metadata and DuckLake catalogs in PostgreSQL and analytical payloads in S3. Workers use disposable scratch space. Live ingress acknowledges admission to bounded memory. Until a flush commits, a worker crash or incomplete drain can lose an accepted event. Durable imports and event mutations use shared checkpoints and staged objects. [Profile setup and recovery](design/hosted-profile-composition.md)

## Schema versions {#unreleased-schema-versions}

Each operational database records a schema version: SQLite in `PRAGMA user_version`, shared PostgreSQL control in `metadata.schema_version`. On start, the server applies the missing numbered steps in one transaction. It refuses a database written by a newer release, and an unversioned database it does not recognize, before changing anything. A native backup restores at its own version; the next start upgrades it.

Each project DuckLake catalog records its own version in `lake.schema_version`. Version 2 drops the per-row copy of the whole event, keeps an original snapshot only for corrected events, and partitions current events by year and month of occurrence. Opening an older catalog upgrades it in place; a catalog from a newer release is refused.

## Retention and deletion

Live events have no automatic age limit. Edits and deletions change the current analytical state once projected. Payload-free heads preserve revision and deletion facts so a late original cannot resurrect deleted data.

DuckLake snapshots can retain old payloads. Porcupine schedules deleted-row rewrites, snapshot expiration, and obsolete-file cleanup. The default policy keeps seven days of snapshots and waits two days before removing obsolete files. Active readers, backup pins, and maintenance backlogs can delay reclamation. See [history cleanup](reference/ducklake-cleanup.md) for settings and progress.

Hosted retention must protect PostgreSQL recovery points and active readers. Physical reclamation waits for the configured grace and the operator’s verified recovery floor. [Hosted recovery requirements](design/hosted-profile-composition.md)

Retry receipts are separate from payload history and stay in SQLite indefinitely by default. Journal deployments can opt into pruning, which introduces an analytical dependency after the first prune. The [receipt reference](reference/receipts.md) describes that tradeoff and the effect of changing retention back to zero.

Logical deletion does not promise physical erasure within 30 days. SQLite free pages or WAL, snapshots, S3 noncurrent versions, backups, and external copies can retain earlier payloads. Their retention policies need to match the operator's requirements.

In standalone, whole-project deletion removes its SQLite state and managed analytical directory or exact S3 prefix. In hosted, deletion fences access before eventual reclamation. Cleanup is durable and retried after failures. Other projects remain separate. Existing backups and copies already delivered to customers are outside this deletion scope. [Project lifecycle](reference/projects.md)

## Recovery

Use [coordinated native backups](reference/native-backup-recovery.md) for local or analytical S3 deployments. A local bundle scheduler alone is insufficient for analytical S3 because it does not include remote objects. Complete S3 backups use an independent destination and verify the copied catalog references and data inventory before publication.

Hosted recovery coordinates PostgreSQL point-in-time recovery, versioned S3 objects, and deployment secrets. Recover into an isolated environment before replacing the live deployment. [Hosted recovery drill](design/hosted-restore-drill.md)

A local-only deployment can also use a [stopped-volume copy](reference/restore.md). Copy the entire data volume after its single writer stops. A live copy of selected SQLite or catalog files is not a consistent recovery point. An S3 bucket listing alone is not a recovery procedure.

Keep private deployment configuration, signing and encryption keys, and the matching image version independently. Recover into isolated fresh storage and verify counts, identity, revisions, deletions, and retries before reconnecting clients.

## Earlier analytical backends

Version v0.14.0 supports DuckDB/DuckLake only. It does not automatically convert an older external-database deployment or its SQL. Preserve an older deployment and its backups until you have exported and imported the required history into a fresh native instance. The historical implementation is retained on `reference/clickhouse-before-ducklake`; its old operator settings are not supported by the native server.
