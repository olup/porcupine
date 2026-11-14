# Back up and restore an instance

For the source build's automated local bundles, see [scheduled native backups](native-backups.md). This page describes the stopped-volume alternative and the separate ClickHouse drills.

## Default DuckLake stopped-volume backup

A DuckLake recovery point needs the complete data volume from one stopped application: SQLite, its WAL state, the DuckLake registry, every project catalog, and every managed Parquet file. Saving only SQLite or only Parquet loses part of the recovery state. The current online ClickHouse checkpoint tool does not support DuckLake.

Use the same complete Compose overlay list as the live instance. The following example uses the local quickstart overlays. The archive stays on the host until you copy it to private off-host storage.

```sh
umask 077
mkdir -p backups
backup_name="porcupine-$(date -u +%Y%m%dT%H%M%SZ).tar"
docker compose -f compose.yaml -f compose.oauth.yaml -f compose.release.yaml stop porcupine
container_id=$(docker compose -f compose.yaml -f compose.oauth.yaml -f compose.release.yaml ps -a -q porcupine)
docker cp "$container_id:/data/." - > "backups/$backup_name"
tar -tf "backups/$backup_name" >/dev/null
docker compose -f compose.yaml -f compose.oauth.yaml -f compose.release.yaml start porcupine
```

The server must remain stopped for the entire copy. Check every command's exit status before resuming normal operation. Keep the matching image digest, private deployment configuration, encryption keys, and archive together. Protect the archive as credential-bearing data. Copy it off the host and verify that copy.

### Restore into isolated storage

Keep the original deployment intact. Create a fresh volume and a stopped container using the same image and the same internal `/data` paths. Extract the archive into that volume with the ownership expected by the nonroot server. Restore the matching pipeline encryption key separately.

Before starting a recovered instance, block destination egress and pause all restored pipelines. Reconcile credential revocations and project deletions that happened after the checkpoint. Saved OAuth refresh tokens and API credentials in a manual full-volume copy remain part of that copy; restoring the files does not automatically revoke them. Isolate the recovered instance from the source export bucket during a drill, because pending project cleanup can remove managed objects.

Start one recovered writer, then check SQLite readiness, instance status, project access, known event counts, identify results, edited and deleted events, and pending queue drainage. Retry known producer message IDs and confirm their event IDs are unchanged. Test a second project to verify isolation. Re-enable clients and destinations only after these checks pass.

Keep the same internal paths. Moving DuckLake files to another data root is not a supported relocation procedure in this release. Cross-version restore and restoration of mismatched SQLite/catalog checkpoints are also outside the contract.

## Optional ClickHouse restore drill

For a running instance see [online backup and restore](online-backup.md).

Run this synthetic drill to check a stopped-copy recovery point across SQLite, ClickHouse local metadata, and native S3 cold objects:

```sh
python3 scripts/restore_drill.py
```

Use Docker, Python 3, and the repository's Go toolchain. The drill uses the pinned ClickHouse and S3-compatible [Silo](https://github.com/pgsty/silo) images from the integration harness. It creates randomized containers, private Docker networks, and temporary files. Published ports bind to `127.0.0.1`. It does not connect to an existing Porcupine instance, use AWS, or deploy an application.

Expect two passing `TestRestoreDrillFixture` runs followed by a JSON report with `"status": "passed"`. The script removes its containers, volumes, networks, private credentials, and temporary backup files on exit. The report contains no credentials or event payloads. Run the command again to repeat the complete drill with fresh state.

### Create the recovery point

The script executes these steps against its own disposable storage:

1. Create two projects, public ingestion keys, active OAuth clients and access tokens, and a revoked OAuth client with an already issued token.
2. Deliver synthetic events. Apply one correction and one deletion. Store an older event on the native S3 disk.
3. Leave one original event, one correction, and one deletion pending in SQLite. Record the logical ClickHouse projections, including tombstones and private original snapshots.
4. Close the application fixture and its SQLite connection. Stop ClickHouse before stopping MinIO so no storage writer can change the checkpoint.
5. Use SQLite's backup API to copy the closed database. Check `integrity_check` and `foreign_key_check` on the copy.
6. Copy the complete stopped `/var/lib/clickhouse` directory and the complete stopped MinIO `/data` directory.

Keep all three copies at the same recovery point. ClickHouse's native S3 disk needs both its local metadata and its remote objects. A bucket listing or an object copy without the corresponding metadata does not establish a usable backup.

### Restore into separate storage

The script restores the copies into a different SQLite file and fresh ClickHouse and MinIO containers. The source containers remain stopped. A separate Docker network preserves the original MinIO endpoint name as a network alias, without connecting the restored storage to the source network. Before opening restored SQLite, the script pauses every pipeline destination in one transaction. Backups without pipeline tables need no pause update.

The restored ClickHouse container uses the same image, disk configuration, credentials, database identity, and S3 endpoint as the checkpoint. Its copied data includes access-control metadata and project reader objects. The restored MinIO container contains the copied bucket and cold objects. The drill does not start two writers against one native S3 disk prefix.

After copying files through the host, the script restores ClickHouse's file ownership inside the new container volume. This prevents host UID mapping from making copied private files unreadable by the database process.

The verification phase checks the following results:

- SQLite table counts, daily cookieless key state, and the complete latest ClickHouse projection for each event match the checkpoint before replay.
- Existing OAuth access tokens still authenticate within their original lifetime. The revoked client's token and secret remain unusable. Changing the bootstrap environment value does not reset its stored credential.
- A project reader cannot read the other project's events. The HTTP API and MCP return the same logical event count.
- Native S3 parts remain readable through ClickHouse after restoring both local metadata and remote objects.
- Pending corrections remain visible through `get-event`, and pending deletions still return `410`.
- Resuming the normal delivery functions drains the pending original and mutation queues. Mutation status changes to `applied`.
- The applied correction survives, and deleted events stay hidden. Retrying every original event UUID is a duplicate, including UUIDs with deletion tombstones.

The fixture gates ClickHouse requests while stopping the background delivery worker. It then invokes the production flush functions explicitly. This lets the test inspect the restored pending state before replay begins.

### Pause egress before starting a manual copy

A manual database or full-volume restore does not run the native operator tool's safety transformations. A copied active destination can resume sending webhook or S3 requests as soon as the recovered application starts. Keep the recovered application stopped and external network access restricted until you have paused every destination and reviewed pending delivery records.

From the repository root, pause the copied destinations before startup:

```sh
python3 - /recovery/porcupine.db <<'PY'
from contextlib import closing
from pathlib import Path
import sqlite3
import sys
sys.path.insert(0, "scripts")
from online_backup import lock, pause_restored_pipelines

path = Path(sys.argv[1]).resolve(strict=True)
with lock(str(path) + ".writer.lock", True, 0):
    with closing(sqlite3.connect(path)) as database:
        database.execute("PRAGMA synchronous=FULL")
        database.execute("BEGIN IMMEDIATE")
        count = pause_restored_pipelines(database)
        database.commit()
print("Paused pipeline destinations:", count)
PY
```

Project deletion also has its own retry queue. Before starting the restored service, isolate it from the source's one-time export bucket and prefix. This prevents pending project deletions from cleaning up the live deployment's objects during a drill. Replay deletions that occurred after the checkpoint before admitting clients on a recovery target. [Online recovery](online-backup.md#reconcile-credentials-exports-and-pipeline-destinations)

The lifetime lock refuses this change while a guarded application is running. The command changes only destination state. It preserves encrypted credentials and pending delivery records. Older snapshots without a `pipelines` table need no update. Do not run an older writer that ignores the lock protocol.

Preserve the original pipeline encryption key outside SQLite and ClickHouse backups. Restore the same `PORCUPINE_PIPELINE_KEY` value or the secure file configured by `PORCUPINE_PIPELINE_KEY_FILE`. Review destination ownership, pending deliveries, post-checkpoint credential revocations, and possible receiver-side effects before resuming any destination. Keeping the key available does not itself authorize restored egress.

### Preserve the recovery boundary

For a live single-host instance, use the [online backup and restore procedure](online-backup.md).

For an instance recovery procedure, stop the application writer before taking the coordinated storage copies. Preserve the matching SQLite database, ClickHouse metadata and data, S3 objects, disk configuration, credentials, and software version. Restore into isolated storage and verify the recovery point before admitting writes. Follow [the storage requirements](storage.md) and [the reliability contract](reliability.md) when designing that procedure.

Do not restore an older SQLite queue onto newer ClickHouse history and assume that revisions reconcile automatically. Do not restore newer SQLite receipts onto older ClickHouse facts: acknowledged payloads may already have left SQLite. These mismatched recovery points are outside the tested contract.

This drill does not cover online snapshots, concurrent writers, replica recovery, multiple ClickHouse nodes, AWS access or backup services, object-version recovery, corruption, a missing S3 object, changed disk endpoints, export job recovery, or a restore across software versions. Export objects and multipart uploads require their own [retention and cleanup policy](exports.md). The stopped MinIO filesystem copy models a complete synthetic object-store checkpoint, not an AWS backup procedure.

The fixture manifest contains synthetic bearer tokens and client secrets. The script keeps that manifest in a private temporary directory and deletes it with the other temporary files. Protect real backup data and credential material according to the deployment's access policy.
