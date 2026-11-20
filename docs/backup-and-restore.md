# Back up and restore an instance

A query export captures results only. A recoverable instance needs its operational databases, DuckLake catalogs, analytical files, and private deployment configuration. Use a complete recovery point for your deployment profile.

## Choose a method

| Deployment | Backup method |
| --- | --- |
| Local analytics on one server | [Scheduled local recovery bundles](reference/native-backups.md), [independent S3 backups](reference/native-backup-recovery.md#choose-a-backup-destination), or a [stopped-volume copy](#make-a-stopped-volume-copy) |
| Analytical files on S3 | [Complete backups to an independent S3 destination](reference/native-backup-recovery.md) |
| Hosted profile | [Coordinate PostgreSQL point-in-time recovery with versioned S3 data](reference/shared-deployment.md#recovery-and-retention) |

Keep backups away from the source disk. Preserve the matching image and keep private configuration, signing keys, and the pipeline encryption key separate from backup bundles. For a manual copy, stop the writer and copy all persistent state together.

## Configure and inspect backups

Set one destination in the [private server configuration](reference/configuration.md). Use `backup.directory` when the analytical files are local, or `backup.s3` with a bucket or prefix separate from the live data. In Compose, keep the instance's release overlay and use the directory ownership in the [local backup setup](reference/native-backups.md#enable-a-schedule).

Check the latest completed checkpoint and any reported error as an instance administrator. [Inspect local backups](reference/native-backups.md#check-the-result) · [Inspect S3 backups](reference/native-backup-recovery.md#inspect-a-backup)

## Restore into fresh storage

Stop producers. For a native backup, use the server version that created it and the matching CLI. Restore into isolated fresh storage. [Restore a local bundle](reference/native-backup-recovery.md#restore-a-local-bundle) · [Restore an S3 bundle](reference/native-backup-recovery.md#restore-an-s3-bundle)

Offline CLI recovery resets old machine credentials, rotates ingestion keys, and pauses pipelines. Follow the [reconnection procedure for your authentication mode](reference/native-backup-recovery.md#reconnect-deliberately). A [manual volume restore](#recover-into-isolation) preserves old credentials and outbound jobs. Verify event counts, identity, corrections, deletions, and stable-ID retries before reconnecting producers.

## Make a stopped-volume copy

Use this procedure when all analytical files are local. Include both SQLite databases and their WAL state, the DuckLake registry, every project catalog, and all referenced data files. Preserve their paths and ownership.

Standalone uses one SQLite journal and a separate SQLite control database. The default control pathname is the journal pathname followed by `.control`. If `PORCUPINE_CONTROL_DB` places it outside the main volume, include that location in the same stopped recovery point. Take the journal and control database from the same recovery point.

1. Stop the single Porcupine writer. Keep other processes from opening the volume.
2. Copy all persistent data locations to independent storage while the application remains stopped.
3. Preserve the image version or digest and private configuration separately, including signing and destination-encryption keys.
4. Verify the copied inventory and hashes. Keep the source untouched until the recovery copy has been tested.

With [analytical S3](reference/ducklake-s3.md), use [coordinated S3 backups](reference/native-backup-recovery.md) to capture the remote data files with their local references.

## Recover into isolation

Manual copies preserve old credentials and outbound jobs. Block access from clients and prevent destination egress before starting a recovery copy.

1. Create fresh isolated storage. Restore the full copied volume at the same internal paths with the normal server user's ownership.
2. Use the matching server image and private configuration. Keep backup destinations separate from the source's live backup namespace.
3. Start one writer, then pause restored pipelines before permitting any external traffic. Review pending exports and cleanup jobs.
4. Reconcile credential revocations and event or project deletions that happened after the checkpoint. A backup cannot infer later changes.
5. Check status, known event counts, identity resolution, corrections, and deletions. Drain pending originals and mutations. Retry known producer IDs and confirm that their event IDs stay unchanged. Query a second project to verify isolation.
6. Reconnect clients and resume destinations only after verification.

Webhook receivers and S3 destinations retain results already delivered. Backups, result downloads, and noncurrent object versions have their own retention policies.

## Plan for copies outside the instance

Configure retention for [copies that can outlive event deletion](storage.md#retention-and-deletion). Replay deletion requests made after the backup before reopening access.
