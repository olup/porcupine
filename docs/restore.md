# Back up and restore a stopped local instance

For scheduled complete checkpoints and the standalone offline restore command, use [native backups and recovery](native-backup-recovery.md). This page describes the manual alternative for deployments whose analytical files are entirely local.

## Make a stopped-volume copy

A recovery point needs the complete data volume from one stopped application: SQLite and its WAL state, the DuckLake registry, every project catalog, and all referenced data files. Preserve their paths and ownership.

1. Stop the single Porcupine writer. Keep other processes from opening the volume.
2. Copy the complete persistent data volume to independent storage. Do not select only the main SQLite file or individual catalogs.
3. Preserve the image version or digest and private configuration separately, including signing and destination-encryption keys.
4. Verify the copied inventory and hashes. Keep the source untouched until the recovery copy has been tested.

With [analytical S3](ducklake-s3.md), the volume contains SQLite and catalogs but not the remote data files. A stopped volume copy alone is incomplete. Use the coordinated S3 backup workflow to capture matching remote references and objects.

## Recover into isolation

Manual copies preserve old credentials and outbound jobs. Block access from clients and prevent destination egress before starting a recovery copy.

1. Create fresh isolated storage. Restore the full copied volume at the same internal paths with the normal server user's ownership.
2. Use the matching server image and private configuration. Keep backup destinations separate from the source's live backup namespace.
3. Start one writer, then pause restored pipelines before permitting any external traffic. Review pending exports and cleanup jobs.
4. Reconcile credential revocations and event or project deletions that happened after the checkpoint. A backup cannot infer later changes.
5. Check status, known event counts, identity resolution, corrections, and deletions. Drain pending originals and mutations. Retry known producer IDs and confirm that their event IDs stay unchanged. Query a second project to verify isolation.
6. Reconnect clients and resume destinations only after verification.

The [offline CLI restore](native-backup-recovery.md) performs a different, safer installation flow into a new volume: it resets archived access, rotates ingestion credentials, pauses destinations, and rebuilds current catalogs. A manual volume copy does not perform those transformations.

Neither method reverses side effects already delivered to an external webhook or S3 destination. Backups, result downloads, and noncurrent object versions have their own retention policies.
