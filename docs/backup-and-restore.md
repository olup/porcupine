# Back up and restore an instance

A query export captures results only. A recoverable instance needs its operational databases, DuckLake catalogs, analytical files, and private deployment configuration. See [backup consistency](reference/online-backup.md).

## Choose a method

| Deployment | Backup method |
| --- | --- |
| Local analytics on one server | [Scheduled local recovery bundles](reference/native-backups.md), [independent S3 backups](reference/native-backup-recovery.md#choose-a-backup-destination), or a [stopped-volume copy](reference/restore.md) |
| Analytical files on S3 | [Complete backups to an independent S3 destination](reference/native-backup-recovery.md) |
| Hosted profile | [Coordinate PostgreSQL point-in-time recovery with versioned S3 data](design/hosted-architecture.md#recovery-and-retention) |

Keep backups away from the source disk. Preserve the matching image and keep private configuration, signing keys, and the pipeline encryption key separate from backup bundles. For a manual copy, stop the writer and copy all persistent state together.

## Configure and inspect backups

Use the configuration and status procedure for your backup destination. Check the latest completed checkpoint and any reported error as an instance administrator. [Inspect local backups](reference/native-backups.md#check-the-result) · [Inspect S3 backups](reference/native-backup-recovery.md#inspect-a-backup)

## Restore into fresh storage

Stop producers. For a native backup, use the server version that created it and the matching CLI. Restore into isolated fresh storage. [Restore a local bundle](reference/native-backup-recovery.md#restore-a-local-bundle) · [Restore an S3 bundle](reference/native-backup-recovery.md#restore-an-s3-bundle)

Offline CLI recovery resets old machine credentials, rotates ingestion keys, and pauses pipelines. Follow the [reconnection procedure for your authentication mode](reference/native-backup-recovery.md#reconnect-deliberately). A [manual volume restore](reference/restore.md#recover-into-isolation) preserves old credentials and outbound jobs. Verify event counts, identity, corrections, deletions, and stable-ID retries before reconnecting producers.

## Plan for copies outside the instance

Configure retention for [copies that can outlive event deletion](storage.md#retention-and-deletion). Replay deletion requests made after the backup before reopening access.
