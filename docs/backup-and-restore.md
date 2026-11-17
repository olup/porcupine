# Back up and restore an instance

A query export is not a backup. A recoverable instance needs its operational databases, DuckLake catalogs, analytical files, and private deployment configuration.

## Choose a method

| Deployment | Backup method |
| --- | --- |
| Local analytics on one server | [Scheduled local recovery bundles](reference/native-backups.md), independent S3 backups, or a stopped-volume copy |
| Analytical files on S3 | [Complete backups to an independent S3 destination](reference/native-backup-recovery.md) |
| Owner-routed shared mode | Coordinate PostgreSQL metadata, S3 files, and every owner's local volume; native whole-instance restore is unavailable |
| Hosted profile | [Coordinate PostgreSQL point-in-time recovery with versioned S3 data](design/hosted-restore-drill.md) |

Keep backups away from the source disk. Keep the pipeline encryption key and deployment secrets privately, separately from backup bundles. A live copy of selected database files is not a consistent checkpoint.

## Configure and inspect backups

The operator selects the destination, interval, timeout, and retained bundle count. Project credentials cannot change these settings. Follow the [local scheduler reference](reference/native-backups.md) or [S3 backup reference](reference/native-backup-recovery.md).

Inspect the latest backup as an instance administrator:

```sh
porcupine get-instance-status
```

Check `native_backup` for the completed checkpoint and any error. The server publishes the manifest only after it has copied and verified the bundle. An interrupted attempt must not replace the previous good backup.

## Restore into fresh storage

Use the exact server image and matching CLI that created the backup. Stop producers and restore into a new volume or S3 prefix, isolated from the original instance. The [offline restore procedure](reference/native-backup-recovery.md#restore-a-local-bundle) includes the commands and storage checks.

Restoration resets old credentials, rotates ingestion access, and pauses outgoing pipelines. Recreate human and machine access deliberately. Verify event counts, identity, corrections, deletions, and stable-ID retries before reconnecting producers.

The v0.15.0 native bundle contains **two** SQLite databases, `acceptance.sqlite` and `control.sqlite`, captured together. Restore v0.14.0's earlier single-database bundles with the matching v0.14.0 image and CLI.

## Plan for copies outside the instance

Snapshots, S3 object versions, backups, and delivered files can retain earlier data after an event is deleted. Configure their retention separately. A backup made before a deletion cannot infer that deletion during restore; replay later deletion requests before reopening access.

For a manual local checkpoint, use the [stopped-volume recovery procedure](reference/restore.md). For consistency boundaries, see [backup consistency](reference/online-backup.md).
