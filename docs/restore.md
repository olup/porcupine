# Run a coordinated backup and restore drill

For a running instance see [online backup and restore](online-backup.md).

Run this synthetic drill to check a stopped-copy recovery point across SQLite, ClickHouse local metadata, and native S3 cold objects:

```sh
python3 scripts/restore_drill.py
```

Use Docker, Python 3, and the repository's Go toolchain. The drill uses the pinned ClickHouse and MinIO images from the integration harness. It creates randomized containers, private Docker networks, and temporary files. Published ports bind to `127.0.0.1`. It does not connect to an existing Porcupine instance, use AWS, or deploy an application.

Expect two passing `TestRestoreDrillFixture` runs followed by a JSON report with `"status": "passed"`. The script removes its containers, volumes, networks, private credentials, and temporary backup files on exit. The report contains no credentials or event payloads. Run the command again to repeat the complete drill with fresh state.

## Create the recovery point

The script executes these steps against its own disposable storage:

1. Create two projects, public ingestion keys, active OAuth clients and access tokens, and a revoked OAuth client with an already issued token.
2. Deliver synthetic events. Apply one correction and one deletion. Store an older event on the native S3 disk.
3. Leave one original event, one correction, and one deletion pending in SQLite. Record the logical ClickHouse projections, including tombstones and private original snapshots.
4. Close the application fixture and its SQLite connection. Stop ClickHouse before stopping MinIO so no storage writer can change the checkpoint.
5. Use SQLite's backup API to copy the closed database. Check `integrity_check` and `foreign_key_check` on the copy.
6. Copy the complete stopped `/var/lib/clickhouse` directory and the complete stopped MinIO `/data` directory.

Keep all three copies at the same recovery point. ClickHouse's native S3 disk needs both its local metadata and its remote objects. A bucket listing or an object copy without the corresponding metadata does not establish a usable backup.

## Restore into separate storage

The script restores the copies into a different SQLite file and fresh ClickHouse and MinIO containers. The source containers remain stopped. A separate Docker network preserves the original MinIO endpoint name as a network alias, without connecting the restored storage to the source network.

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

## Preserve the recovery boundary

Use this drill as a reproducible check of the repository's synthetic recovery case. It does not provide an online production backup command or an automated cross-database snapshot mechanism.

For an instance recovery procedure, stop the application writer before taking the coordinated storage copies. Preserve the matching SQLite database, ClickHouse metadata and data, S3 objects, disk configuration, credentials, and software version. Restore into isolated storage and verify the recovery point before admitting writes. Follow [the storage requirements](storage.md) and [the reliability contract](reliability.md) when designing that procedure.

Do not restore an older SQLite queue onto newer ClickHouse history and assume that revisions reconcile automatically. Do not restore newer SQLite receipts onto older ClickHouse facts: acknowledged payloads may already have left SQLite. These mismatched recovery points are outside the tested contract.

This drill does not cover online snapshots, concurrent writers, replica recovery, multiple ClickHouse nodes, AWS access or backup services, object-version recovery, corruption, a missing S3 object, changed disk endpoints, export job recovery, or a restore across software versions. Export objects and multipart uploads require their own [retention and cleanup policy](exports.md). The stopped MinIO filesystem copy models a complete synthetic object-store checkpoint, not an AWS backup procedure.

The fixture manifest contains synthetic bearer tokens and client secrets. The script keeps that manifest in a private temporary directory and deletes it with the other temporary files. Protect real backup data and credential material according to the deployment's access policy.
