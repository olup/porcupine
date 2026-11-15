# Storage

Porcupine embeds DuckDB and DuckLake. SQLite holds operational state and accepted work. Each project has its own local DuckLake catalog and managed analytical data files. Run one application writer per SQLite database and DuckLake root.

## Local files or S3

Local SSD is the default. Keep SQLite, the DuckLake registry, project catalogs, and Parquet files on persistent storage. No external database or S3 account is required.

A fresh deployment can place analytical data files on [S3](ducklake-s3.md). SQLite and catalogs remain local. Queries use the same project-scoped DuckDB SQL whether the files are local or remote. S3 is selected before archive initialization; changing configuration does not migrate an existing archive or enable age-based SSD-to-S3 tiering.

Analytical storage, [destination pipelines](pipelines.md), [one-time exports](exports.md), and [backup destinations](native-backup-recovery.md) are separate. A scheduled query delivered to S3 is an external copy, not the analytical archive or an instance backup.

Storage identity is pinned. An unexpected registry, catalog, bucket, or prefix change fails rather than opening an empty replacement. Project agents cannot change deployment storage settings or retrieve storage credentials through HTTP or MCP.

## Retention and deletion

Live events have no automatic age limit. Edits and deletions change the current analytical state once projected. Payload-free heads preserve revision and deletion facts so a late original cannot resurrect deleted data.

DuckLake snapshots can retain old payloads. Porcupine schedules deleted-row rewrites, snapshot expiration, and obsolete-file cleanup. The default policy keeps seven days of snapshots and waits two days before removing obsolete files. Active readers, backup pins, and maintenance backlogs can delay reclamation. See [history cleanup](ducklake-cleanup.md) for settings and progress.

Logical deletion does not promise physical erasure within 30 days. SQLite free pages or WAL, snapshots, S3 noncurrent versions, backups, and external copies can retain earlier payloads. Their retention policies need to match the operator's requirements.

Whole-project deletion removes its SQLite state and managed analytical directory or exact S3 prefix. Cleanup is durable and retried after failures. Other projects remain separate. Existing backups and copies already delivered to customers are outside this deletion scope. [Project lifecycle](projects.md)

## Recovery

Use [coordinated native backups](native-backup-recovery.md) for local or analytical S3 deployments. A local bundle scheduler alone is insufficient for analytical S3 because it does not include remote objects. Complete S3 backups use an independent destination and verify the copied catalog references and data inventory before publication.

A local-only deployment can also use a [stopped-volume copy](restore.md). Copy the entire data volume after its single writer stops. A live copy of selected SQLite or catalog files is not a consistent recovery point. An S3 bucket listing alone is not a recovery procedure.

Keep private deployment configuration, signing and encryption keys, and the matching image version independently. Recover into isolated fresh storage and verify counts, identity, revisions, deletions, and retries before reconnecting clients.

## Earlier analytical backends

Version v0.14.0 supports DuckDB/DuckLake only. It does not automatically convert an older external-database deployment or its SQL. Preserve an older deployment and its backups until you have exported and imported the required history into a fresh native instance. The historical implementation is retained on `reference/clickhouse-before-ducklake`; its old operator settings are not supported by the native server.
