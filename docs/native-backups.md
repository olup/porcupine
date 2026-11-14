# Configure native backups

Unreleased. Build this source checkout to use this feature. Published v0.12.1 images do not include it.

The DuckLake server can create coordinated recovery bundles on a schedule. Each bundle contains an independent SQLite acceptance database, the DuckLake registry, every registered project catalog, and its managed data files. No backup sidecar or Python script is needed.

Backups are disabled until the deployment operator sets a private destination directory. Copy completed bundles to independent off-host storage to protect against loss of the server.

## Enable a schedule

For a Linux Compose deployment, create a private destination owned by the container user:

```sh
export PORCUPINE_BACKUP_HOST_DIR=/srv/porcupine-backups/native
sudo install -d -m 0700 -o 65532 -g 65532 "$PORCUPINE_BACKUP_HOST_DIR"
```

Add `compose.native-backup.yaml` to the deployment's existing overlay list. For a new local build:

```sh
docker compose -f compose.yaml -f compose.native-backup.yaml up -d --build
```

Keep the OAuth, proxy and other overlays already used by your instance. This overlay selects `/backups` inside the container and mounts the private host directory there. It applies to the default DuckLake backend; optional ClickHouse deployments keep their [existing backup tool](online-backup.md).

For a directly hosted binary, set the environment variables below. The destination must be outside the managed DuckLake directory and accessible only to the server's owner. Use a dedicated backup directory.

| Setting | Default | Meaning |
| --- | --- | --- |
| `PORCUPINE_BACKUP_DIR` | Empty | Enables native backups when set to a private directory. |
| `PORCUPINE_BACKUP_INTERVAL` | `24h` | Gap after a completed backup before the next attempt. Minimum `1m`. |
| `PORCUPINE_BACKUP_TIMEOUT` | `5m` | Deadline for coordination, copying and verification. Range `1s` through `1h`. |
| `PORCUPINE_BACKUP_RETAIN` | `7` | Completed bundles retained for this native source. Range 1 through 1,000. |

To disable backups for a directly hosted binary, unset all four settings. For Compose, remove the native backup overlay.

The first backup starts about one second after startup when no recent success is recorded. Restarting after a successful backup preserves its next due time. A failure or interrupted attempt retries after at most one minute. Settings belong to the deployment; an agent cannot change them through project permissions.

## Check the result

An instance administrator can inspect the schedule and progress:

```sh
porcupine get-instance-status
```

The same `get-instance-status` operation is available through HTTP and MCP. `native_backup` is omitted for project-scoped callers. It reports `waiting`, `running`, `ok`, `failed`, or `interrupted`, plus `retain_completed`, the configured retention limit, last attempt, last success, last checkpoint time, and latest completed bundle ID. A recorded backup failure degrades the administrator's overall status.

`last_checkpoint_at` identifies the captured recovery point. `last_success` records later publication. Events accepted after the checkpoint can remain outside that bundle even if they arrived before backup publication.

Completed bundles have names such as `pc_backup_<id>`. Each contains `acceptance.sqlite`, `ducklake/`, and `manifest.json`. The manifest records file sizes and SHA-256 hashes, native source identity, server version, checkpoint time and original internal paths. Those private paths are not included in the API status response.

The server checks the copied SQLite databases, their source identities, the complete file inventory and every file hash before publishing a completed bundle. A failed attempt leaves the previous completed bundle intact. Retention runs only after a replacement passes verification. Incomplete destination copies and abandoned local staging directories are cleaned on retry or restart.

Unchanged Parquet files can share storage between bundles through hardlinks after checksum verification. Removing an older bundle leaves the newer bundle's links intact. SQLite and native catalogs are copied independently. Copying bundles to another filesystem can consume more space; these local links are not an incremental cloud-backup protocol.

## What pauses during capture

The server pauses native event projection, its SQLite acknowledgments and project deletion at the existing coordination lock. New events can still enter the bounded SQLite queue. Existing analytical reads continue while SQLite is captured through its online backup API.

Before capturing the native catalogs, the server waits for active native readers and maintenance to finish. It then briefly closes resident engines, copies their catalog state and pins immutable Parquet files in a private local stage. Queries can wait during this native staging phase. A long export can make the backup reach its deadline; the backup fails and retries instead of cancelling the export.

After staging, normal projection and native maintenance resume while the server copies and verifies the bundle. Backup I/O still competes with live work. Keep disk headroom for the checkpoint and size the queue for the projection pause. Local DuckLake storage must support hardlinks. A bundle is limited to 50,000 files and a 16 MiB manifest; larger inventories fail before publication. There is no host failover or point-in-time recovery between scheduled checkpoints.

The [DuckLake backup guide](https://ducklake.select/docs/stable/duckdb/guides/backups_and_recovery) explains why catalog state and referenced files must be preserved together. SQLite's [online backup API](https://www.sqlite.org/c3ref/backup_finish.html) provides the acceptance snapshot.

## Restore into isolated storage

Restore with the application stopped, into fresh storage, using the same internal paths and matching server image. Keep the original source and completed bundle intact. A recovery drill must not contact live clients or destinations.

1. Copy the completed bundle to the recovery host. Compare its inventory and hashes with `manifest.json` before copying files into the target. The source-side verification does not verify a later off-host transfer.
2. Record the server image digest and preserve private deployment configuration and pipeline encryption keys separately. They are not part of this bundle.
3. Disable the native backup overlay on a drill, or select a separate backup destination. A recovered copy retains the source identity and must not prune the live deployment's bundles. Keep destination egress blocked before the recovered server starts. Restoring SQLite restores its credentials and outbound jobs as of the checkpoint. Reconcile later credential revocations and project deletions from an independent record.
4. Copy `acceptance.sqlite` to the original SQLite pathname. Copy the contents of `ducklake/` to the original DuckLake root. Preserve private permissions and the container user's ownership. Do not reuse SQLite WAL files or any files from a newer recovery point.
5. Start one isolated writer. Pause restored pipeline destinations through the API or agent before permitting egress. Keep the recovery deployment isolated from the source's one-time export bucket while pending deletions are reviewed.
6. Check instance status, known counts, identify results, corrections and deletions. Drain pending events and mutations. Retry known producer IDs and verify that their event IDs are unchanged. Check another project to prove isolation.
7. Review credentials, pending exports and external side effects before reconnecting clients or resuming destinations. The automatic bundle does not perform the safety transformations of the separate ClickHouse restore tool.

A checkpoint cannot reconstruct revocations or deletions that occurred afterward. Restoring it can reactivate old credentials until you reconcile them. Keep independent recovery administration credentials available.

These steps describe a manual offline recovery. No API endpoint installs a backup over a running instance. A coordinated [stopped-volume copy](restore.md) remains available as another backup method.
