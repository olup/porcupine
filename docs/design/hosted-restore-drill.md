# Hosted PostgreSQL and S3 restore drill

This opt-in test uses two disposable PostgreSQL data directories, a filesystem WAL archive, and the loopback-only S3 integration fixture. It exercises the unreleased hosted storage and publication components. Hosted service activation is a separate gate.

Run from the repository root as a non-root user with Docker, Go with CGO support, and a PostgreSQL 17 binary directory containing *initdb*, *pg_ctl*, and *pg_basebackup*.

    PORCUPINE_TEST_PITR_POSTGRES_BIN=/path/to/postgresql/bin make hosted-restore-drill

The runner creates a dedicated S3 test bucket and starts its own PostgreSQL server. It does not take a database URL or a bucket name. The same PostgreSQL 17.11 binaries cached by the shared PostgreSQL fixture can run the drill.

## What the drill checks

1. Take a physical base backup before the service writes any data. Place Porcupine control state and DuckLake catalogs in two databases in the same cluster.
2. Write two projects in separate tenants through the shared-native service, then write a separate fenced hosted project through the hosted flusher. Confirm project-scoped analytics and a pending webhook with an immutable S3 body. Create a named PostgreSQL restore point.
3. Edit and delete an event through the shared-native mutation path, and revoke a project key. Run the native snapshot and obsolete-file cleanup with its clock advanced past the shared readers' snapshot horizon. The configured file grace keeps the original Parquet readable. The legacy maintenance and physical project removal paths refuse the fenced hosted catalog.
   In the stateless hosted profile, delete a separate project after the restore point. Seal the native stream and record the managed object inventory. Verify that the recovery gate leaves the managed objects readable.
4. Archive WAL through the post-target changes, close the old worker and primary, and start recovery from the original base backup and the WAL archive with the named target. Check that PostgreSQL reaches that target and promotes.
5. Use a fresh scratch directory and a private listener with pipelines and exports disabled. Replay the post-target key revocation before any restored analytical query. Verify that the revoked key fails, each project key can read only its own project, and analytics returns the original pre-edit and pre-delete event. Read the hosted delivery body from its restored PostgreSQL object reference and verify its original delivery identity. Query the restored stateless hosted project and read the immutable manifest recorded in its restored native proof. Check the S3 key inventory before and after recovery.

The event changes exercise the journal-backed shared-native mutation path. The separate hosted cleanup fixture tests deletion after the recovery floor advances, including cold resumption, removal of exact managed objects, and an unaffected neighbor. Actual send replay and the hosted import and durable mutation object paths still need end-to-end PITR coverage.

## Configured cleanup policy

For PostgreSQL-backed DuckLake with S3, set all three policy durations under the *ducklake* config section or through the corresponding environment variables.

| Config field | Environment variable |
| --- | --- |
| restore_window | PORCUPINE_DUCKLAKE_RESTORE_WINDOW |
| restore_max_reader_time | PORCUPINE_DUCKLAKE_RESTORE_MAX_READER_TIME |
| restore_safety_margin | PORCUPINE_DUCKLAKE_RESTORE_SAFETY_MARGIN |

The server rejects incomplete policies and requires *cleanup_grace* to cover the sum of the three durations. Use the maximum time any supported reader can hold an older snapshot, including exports. The drill configures a 14-day restore window, seven-day readers, a two-day safety margin and 30-day obsolete-file grace. It advances the cleanup clock by nine days for the test.

The duration check covers DuckLake's obsolete-file grace. The legacy shared-native project purge removes its S3 prefix outside that grace. Provider lifecycle, physical project purges, orphan cleanup and job artifact deletion each need their own evidence that they respect the recovery hold.

### Hosted recovery floor

Set `ducklake.restore_floor_file` or `PORCUPINE_DUCKLAKE_RESTORE_FLOOR_FILE` to an absolute path to an operator-controlled file. Write one RFC3339 timestamp for the earliest restore point the deployment still supports. Include secondary backups and manual recovery procedures in that assessment. Use the same evidence across workers and never restore below a floor that a worker has already used.

Replace the file atomically when the verified floor advances. Each cleanup pass reads it again. A missing, unreadable, malformed, or stale floor leaves physical cleanup pending. The floor must reach the recorded inventory time and the PostgreSQL clock must show that `cleanup_grace` has elapsed since that inventory. The worker scopes deletion to inventory entries it can tie to the project. An unresolved activity ticket also leaves cleanup pending. Lease expiry alone gives no proof that an old executor returned from its call.

Tenant completion reports `logically_deleted`. Verification of backups and artifacts without a recorded owner remains separate from deleting the managed inventory.

## Provider acceptance before activation

- Restore the selected managed PostgreSQL service at a real target time with both databases in the same recovery domain. Verify the effective earliest recoverable time, required WAL, resumed writes and the time to restore. Independently fence pre-restore writers and keep the old endpoints offline.
- Inspect effective bucket lifecycle rules, versioning, delete markers and read permissions for the actual data prefixes. Verify that ordinary reads still reach Parquet and that all exact immutable references from restored metadata are readable, including hosted mutation requests, import originals and parser checkpoints, webhook rules, fragments and bodies. Exercise every deletion path that can remove those objects before the recovery horizon closes. A prior S3 version can require explicit restoration when the current object is a delete marker.
- Keep the clone private with outbound jobs paused. Reapply credential revocations and delete instructions recorded after the target, or rotate recovered access before opening traffic. Disable destination credentials while validating pending jobs. Shared PostgreSQL export recovery needs its own job and authority reconciliation, with artifact references held until they can be safely reclaimed. The receiver must deduplicate stable delivery IDs when jobs are allowed to resume. A database restore cannot undo a delivery the receiver has already accepted.
- Restore the separately retained deployment configuration and the pipeline encryption-key versions. Validate destination decryption offline before enabling sending. This fixture uses no encrypted destination credentials and performs no external sends.
