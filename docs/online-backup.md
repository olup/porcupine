# Back up a live single-host instance

Use `scripts/online_backup.py` to create a coordinated recovery point while ingestion continues. The operator tool takes a SQLite online snapshot and a full native ClickHouse backup. It exposes no HTTP or MCP operation and uses no agent credential.

The tool pauses event projection, not HTTP capture, for the checkpoint operation. Capture continues to commit new events to the bounded SQLite queue. Those accepted after the SQLite snapshot are outside this recovery point. If the queue fills during the pause, capture returns its normal backpressure response.

Restore SQLite and ClickHouse from the same checkpoint. SQLite holds the accepted identity links and profile versions. ClickHouse holds the delivered snapshots used to resolve historical events. Mixing checkpoints can give different answers at capture time and query time.

## Prepare the operator environment

Use one self-hosted Porcupine writer and one ClickHouse server on a POSIX host. Run the tool in the same kernel and against the same local SQLite files as the application. Python 3 needs only its standard library, including `sqlite3` and `fcntl`. No SQLite command-line client, Docker executable, cloud SDK, or Porcupine bearer token is required by the tool.

Use a local filesystem with working `flock` locks. Do not use a remote filesystem, copy the lock files to a different filesystem, delete a lock file while an application runs, or mix an older writer with the guarded binary. Unsupported platforms retain normal server support but cannot use this online checkpoint protocol. A containerized application and operator sidecar must see the same SQLite and lock-file inodes. [The Compose backup overlay](backup-compose.md) provides the single-host mounts and backup disk configuration.

Do not run direct ClickHouse event writes, manual data mutations, or schema upgrades during the checkpoint. The guard coordinates supported application writers and the operator tool. It cannot fence an unrelated administrator connection that ignores the protocol.

Configure a durable ClickHouse backup disk named `backups`, and permit that disk in ClickHouse's backup configuration. Preserve the native archive on that disk independently of the SQLite bundle. The tool does not create backup disks, change bucket policies, or manage retention. Use a destination separate from live ClickHouse S3 disk objects and result-export objects.

The ClickHouse operator needs native backup and restore privileges, table and system-metadata reads, and full visibility of every project. The tool does not grant those privileges or change source row policies. With row policies present, it requires an explicit permissive `USING 1` policy for the configured operator and rejects restrictive policies. Resolve unsupported policy configurations before taking a checkpoint.

Start the guarded application and allow its projection worker to initialize. The worker records its physical event-table UUID in SQLite. A backup fails if the operator points at another healthy database, if the source attestation is absent, or if only stale guard metadata remains after a downgrade.

Keep secrets in private environment values:

```sh
export PORCUPINE_CLICKHOUSE_USER=porcupine_admin
# Set PORCUPINE_CLICKHOUSE_PASSWORD through your private environment mechanism.
```

Do not put passwords in command arguments or commit them with the backup configuration. The SQLite snapshot contains credential hashes, the reader derivation secret, daily keys, anonymous-to-identified links, current person traits, queued event payloads, mutation state, and export request metadata. Protect the complete recovery set as private data.

## Create a checkpoint

Choose a new output directory whose parent already exists:

```sh
python3 scripts/online_backup.py backup \
	--db /data/porcupine.db \
	--clickhouse-url http://clickhouse:8123 \
	--database porcupine \
	--backup-disk backups \
	--output /checkpoints/checkpoint-2026-11-08 \
	--timeout 300 \
	--lock-timeout 30
```

The command refuses an existing output directory. The timeout covers the operation, including SQLite verification and native backup. Lock admission has its own bounded wait. An interrupted command can leave an incomplete local bundle or a partial native archive. It never publishes a usable manifest for that attempt.

The command emits JSON progress lines. `checkpoint_acquired` means every in-flight projection and its SQLite acknowledgment finished before the tool obtained the exclusive guard. `sqlite_snapshot_complete` identifies the copied SQLite recovery point. `native_backup_started` and `native_backup_finished` bracket the native operation. `backup_complete` includes the private manifest's SHA-256 digest.

The checkpoint runs one full-history validation pass while projection is paused, followed by the native backup's reads. Validation aggregates the latest revision, including tombstones, in primary-key order with 512 MiB memory and 1 GiB spill limits. Progress reports the validation time, native backup time, and total projection pause. Use those measurements to budget queue capacity and cold-storage read traffic. A large history can make the pause much longer than a small local drill.

Keep these items together:

- `sqlite.db` in the output directory.
- `manifest.json` in the output directory.
- The complete native archive identified by `native_backup.disk` and `native_backup.name` in the manifest.
- The operator-managed disk configuration, credentials, software version, and any required encryption keys.

The bundle directory alone is insufficient. The native archive is on the configured ClickHouse backup disk, which can be mounted or transferred to the recovery server. The tool does not back up that disk's underlying storage configuration or copy the archive into the SQLite bundle.

Treat a bundle as complete only when `manifest.json` exists, `INCOMPLETE.json` is absent, and the command reports success. The tool fsyncs its local files and publishes the manifest last. The manifest binds the SQLite hash, native archive, database, original table UUIDs, table definitions, and logical event summary. The summary includes deletion tombstones. Native restore also verifies the archive's own file checksums. The manifest is an integrity record, not a signature against an attacker who can rewrite the whole recovery set.

The native backup copies event data from both local and cold parts. Keep the full archive: a cold-object listing or live S3 prefix alone does not replace it. Refer to [ClickHouse's native backup documentation](https://clickhouse.com/docs/operations/backup) for backup disk configuration, archive retention, and server privileges.

## Restore into isolated storage

Stop the recovery application before restoring. Use a new SQLite pathname, an absent target ClickHouse database, and a recovery storage configuration isolated from the live instance. Keep the target's network access restricted until credential rollback and verification are complete. The tool refuses to overwrite SQLite, merge into an existing ClickHouse database, or bypass a live application's lifetime lock.

Make the native archive available on the target's configured backup disk. Set the target operator credentials, then run:

```sh
python3 scripts/online_backup.py restore \
	--bundle /checkpoints/checkpoint-2026-11-08 \
	--db /data/restored.db \
	--clickhouse-url http://recovery-clickhouse:8123 \
	--acknowledge-credential-rollback
```

The database name and native archive come from the private manifest. The tool checks SQLite integrity and its SHA-256 hash, restores the native database, and verifies table definitions and the logical event summary before publishing SQLite. The archive's event-table UUID can change during native restore. After proving the restored history matches, the tool explicitly rebinds the copied receipt-source pin and provenance to the new UUID and advances the receipt generation. It retains the permanent `receipt_fallback_required` flag and clears only obsolete lookup leases.

Native restore can place parts that were previously cold on the target's default hot disk. Provision capacity for the restore, then let the configured MOVE TTL or an operator-controlled part move establish the intended tier placement. The native archive contains the event data independently of the source S3 prefix. Restoring the content does not promise identical hot and cold placement.

If the operator connection differs from the daemon's configured connection, supply the daemon's exact URL and username:

```sh
python3 scripts/online_backup.py restore \
	--bundle /checkpoints/checkpoint-2026-11-08 \
	--db /data/restored.db \
	--clickhouse-url http://127.0.0.1:8123 \
	--service-clickhouse-url http://clickhouse:8123 \
	--service-clickhouse-user porcupine_admin \
	--acknowledge-credential-rollback
```

These values bind the restored receipt source to the daemon's configuration. They do not select a different restore destination. A wrong binding fails closed when the daemon checks its source. Project views and restricted ClickHouse reader accounts regenerate from the restored SQLite grants and derivation secret. Server-level ClickHouse credentials and configuration remain operator-managed.

## Reconcile credentials, exports, and pipeline destinations

Restoring the checkpoint also restores its authorization state. A client revoked after that point can otherwise become active again. Review an independent revocation record before opening the recovered service. Replay known revocations during restore with repeatable `--revoke-principal ID` options. The command refuses to leave no active instance administrator.

By default, restore deletes every saved OAuth access token. Clients must exchange their retained client secrets for fresh tokens. The stored client grants and checkpoint-time revocations remain. `--preserve-access-tokens` retains still-valid checkpoint tokens only when the operator deliberately chooses that behavior. Neither choice reconstructs post-checkpoint client revocations. `--acknowledge-credential-rollback` records the operator's acknowledgment of that recovery boundary.

SQLite stores hashes rather than recoverable client secrets. Keep the operator's recovery credentials separately. Changing the bootstrap environment value does not reset the restored bootstrap credential.

External result-export objects are outside this checkpoint. Restore cancels export jobs, clears stored request SQL and file references, and requires fresh export requests. It does not mint links to checkpoint-time exports, resume their uploads, or delete objects in the source deployment. Apply the source export prefix's independent retention and multipart-cleanup rules. Native cold event data is included through ClickHouse's backup and follows a different recovery path.

Restore also pauses every configured pipeline destination before publishing SQLite. This update commits with the credential and export changes. Existing pending delivery records remain, but restoring the checkpoint does not authorize sending them. Review destination ownership, credentials, filters, pending deliveries, and any external effects since the checkpoint before explicitly resuming a destination. A restore can otherwise repeat a webhook or an S3 write that the receiver accepted after the recovery point.

Older backups without pipeline tables still restore. The tool does not create pipeline tables or invent destinations for those snapshots. An unknown pipeline schema fails closed rather than silently restoring active egress.

Preserve the pipeline encryption key separately from the recovery set. Supply the original base64-encoded 32-byte value through `PORCUPINE_PIPELINE_KEY`, or preserve the secure file referenced by `PORCUPINE_PIPELINE_KEY_FILE`. The SQLite backup retains encrypted destination credentials and their key fingerprint, not the key needed to decrypt them. The restore tool pauses destinations without decrypting or rotating those credentials. A missing or mismatched key must be resolved before starting an instance that contains configured destinations.

Manual SQLite or volume copies bypass the operator tool's pause transaction. They can restore active destinations and immediately resume external delivery when the application starts. Follow [the manual-copy egress precaution](restore.md#pause-egress-before-starting-a-manual-copy) before admitting a physically restored instance.

## Recover from an interrupted operation

The process holds kernel locks rather than expiring leases. Process exit or a crash releases the guard automatically, so normal projection can resume without deleting a lock file. Keep the lock files themselves in place.

A failed backup retains `INCOMPLETE.json` and publishes no complete manifest. Use a new output directory for the next attempt. Inspect and remove the abandoned native archive through an operator procedure after confirming no server backup still uses it. The tool makes a bounded cancellation attempt after a reported native failure, but a process killed without cleanup can leave server-side work or partial files.

A restore creates `<db>.restore-incomplete` before changing ClickHouse. The application refuses to start while that marker exists. Failure can leave a partially restored database and a private `.restore-working` SQLite file. Do not remove the marker merely to admit traffic. Discard the isolated recovery target and repeat the restore into fresh storage, or use a separately reviewed manual recovery procedure that proves the same manifest checks. The tool does not drop partial target databases automatically.

The online protocol supports the current single-server `events_v2` and `schema_state` database. It fails closed on unsupported table layouts. It does not provide point-in-time recovery after the checkpoint, replication recovery, automatic scheduling, online schema upgrades, archive encryption, or cross-version compatibility guarantees. Native backup holds the projection guard through completion because asynchronous submission does not prove that ClickHouse has frozen its snapshot. Size the queue and backup timeout for that pause, and measure backup I/O impact on the deployment before scheduling it.

## Verify the recovery path locally

Run the operator failure-boundary tests without containers:

```sh
python3 -m unittest discover -s scripts -p test_online_backup.py -v
```

Run the separate Docker drills against disposable local storage:

```sh
python3 scripts/online_restore_drill.py
python3 scripts/default_restore_drill.py
```

The first drill exercises concurrent live ingestion, a finite receipt cache, pending work, native cold data, and native restore. The second proves that a restored permanent-ledger instance still starts and durably accepts a new event while ClickHouse is stopped. The [stopped-copy drill](restore.md) exercises a different recovery method. None of these commands deploys an instance or connects to production storage.

Restoring a checkpoint with interactive OAuth invalidates browser authorization requests, codes, refresh tokens and consent principals, including when machine access tokens are explicitly preserved. Users must approve a new grant. Import offsets, manifests and pending import rows come from the same SQLite checkpoint. Resume using that checkpoint’s reported offset.

When restoring an OAuth-enabled snapshot, keep an independent administrator credential available: the original bootstrap or a machine administrator with the client-credentials grant. The restore check excludes interactive registrations, email accounts and service keys when verifying that recovery access survives.
