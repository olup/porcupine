# Remote backups and offline recovery

Native backups use the two-database `porcupine-native-v2` format introduced in v0.15.0. Restore single-database v0.14.0 bundles with the v0.14.0 CLI and image. For local scheduling, see [local backups](native-backups.md).

## Choose a backup destination

The operator can select a private local directory or an independent S3 destination. Local data can use either. Analytical S3 requires an S3 backup destination; a metadata-only local copy is rejected.

Set these variables in the server's private deployment configuration:

```sh
PORCUPINE_BACKUP_S3_BUCKET=your-private-backup-bucket
PORCUPINE_BACKUP_S3_PREFIX=porcupine-recovery
PORCUPINE_BACKUP_S3_REGION=eu-west-3
PORCUPINE_BACKUP_INTERVAL=24h
PORCUPINE_BACKUP_TIMEOUT=30m
PORCUPINE_BACKUP_RETAIN=7
```

Leave `backup.directory` and its environment override unset. Put the S3 settings in `backup.s3` in the private [server configuration](configuration.md), or pass the variables directly to the server process.

Backup S3 settings use the same suffixes as [analytical S3](ducklake-s3.md), including `ENDPOINT`, `PATH_STYLE`, `CA_CERT_FILE`, `ACCESS_KEY_ID`, `SECRET_ACCESS_KEY` and `SESSION_TOKEN`. Empty explicit credentials use the AWS credential chain. The destination needs object read, write, list and delete permissions, including conditional creation. Keep its bucket and prefix disjoint from analytical data.

These are deployment settings. Project agents cannot change them. One live writer owns each source and backup namespace. A restored instance gets a new backup writer identity and must use a fresh backup prefix or directory. Raw deployment copies are not a multi-writer setup.

## Inspect a backup

An instance administrator can use HTTP, CLI or MCP `get-instance-status`:

```sh
porcupine get-instance-status
```

The administrator's `native_backup` report includes state, checkpoint, success, `latest_bundle` and `source_id`. Project-scoped callers do not receive it. This source ID selects the analytical backup namespace; it differs from an event's producer `source_id`.

S3 bundles live beneath `PREFIX/native-backups/SOURCE_ID/BACKUP_ID/`. Each completed bundle contains `control.sqlite`, `acceptance.sqlite`, project catalogs, data files, and `manifest.json`. Both SQLite files belong to the same coordinated recovery point. The manifest describes the bundle layout and original storage identity. It contains no source credentials or deployment environment files. Full-instance native backup is unavailable with shared PostgreSQL control or catalogs.

The server briefly coordinates projection and catalog capture. For remote data it then holds cleanup protection and project deletion pins while ingestion and queries resume. It checks current catalog references, streams data into the backup, verifies hashes by reading the destination back, and publishes the manifest last. Missing current references or failed transfers must not replace the previous good backup.

S3 backups are sequential full copies. Account for storage and transfer costs. Local backups retain Parquet reuse through hardlinks. Bounds are 50,000 files, a 16 MiB manifest and 5 GiB per S3 object. The configured total timeout bounds transfer work. After a crash, incomplete attempts are discarded and recaptured because their old snapshot protections no longer exist.

## Restore a local bundle

Use the standalone CLI version that matches `version` in the bundle manifest. The released CLI selects its matching server image. For a development build or an image pinned by digest, pass `--image` explicitly. Docker runs the native engine for this offline operation.

For builtin authentication, create a private file containing a new random bootstrap secret of at least 32 characters. It must differ from every archived credential. Keep it outside the repository. External authentication uses the mode-specific procedure below instead.

```sh
porcupine restore \
  --bundle /srv/porcupine-backups/pc_backup_YOUR_ID \
  --volume porcupine-recovered \
  --bootstrap-token-file /secure/porcupine-recovery-token \
  --reset-access
```

The volume must be new. The CLI puts the secret in private stdin, rather than Docker arguments. The one-shot container installs the journal at `/data/porcupine.db`, the control database at `/data/porcupine.db.control`, and analytics under `/data/ducklake`, then gives installed files to the normal nonroot server user. It leaves the HTTP service stopped. Before starting it, install the private server configuration in the recovered volume. Preserve the pipeline encryption key and match the restored storage paths and authentication mode. In builtin mode, keep the bootstrap secret supplied to restore. Failed attempts retain the volume for inspection. The incomplete marker remains until both SQLite files and the reconstructed analytical state are published; normal startup refuses an incomplete installation.

## Restore an S3 bundle

Configure `PORCUPINE_BACKUP_S3_*` for read access to the archive, then select its IDs:

```sh
porcupine restore \
  --backup-id pc_backup_YOUR_ID \
  --source-id YOUR_ANALYTICAL_SOURCE_ID \
  --volume porcupine-recovered \
  --bootstrap-token-file /secure/porcupine-recovery-token \
  --reset-access
```

To recover into S3, also set `PORCUPINE_DUCKLAKE_S3_*` for a fresh analytical bucket or prefix. Original analytical storage and archive storage are forbidden targets, including empty namespaces. Without target S3 settings, recovery uses the new volume's SSD. The CLI mounts custom CA and AWS credential files read-only.

The restorer verifies the inventory, checks application and DuckLake formats, and reconstructs current tables in new catalogs. Native `INSERT ... SELECT` transfers data without downloading the full S3 archive onto the SSD. Event IDs, revisions, identity links and tombstones survive. Physical snapshot history is not part of this recovery contract. The target can operate after the archive becomes unavailable.

## Restore with Parquet encryption

Set `PORCUPINE_DUCKLAKE_ENCRYPTED=true` in the CLI environment when restoring an encrypted DuckLake bundle. The same option restores a plaintext bundle into fresh encrypted catalogs. Before starting the restored server, set `ducklake.encrypted` to match the restore target. [DuckLake Parquet encryption](configuration.md#ducklake-parquet-encryption)

An encrypted native backup contains both the Parquet files and the catalogs with their encryption keys. Anyone who can read the bundle can recover the data. Protect access to the backup destination and all copies.

## Reconnect deliberately

Recovery resets old API keys, OAuth credentials, human sign-ins, and invitations in a builtin control database. It rotates public ingestion credentials, pauses pipelines, and cancels or clears pending imports, exports, and outbound work in the journal. The result provides new ingestion credentials. A replacement for a retrievable browser key is also available through [`get-client-ingestion-key`](projects.md#get-a-browser-ingestion-key). The bootstrap secret stays in your private file. Invite people and create service keys explicitly with the recovery administrator.

### Restore an external-authentication instance

For an archive created in [external mode](external-auth.md), set `--auth-mode external` and omit `--bootstrap-token-file`:

```sh
porcupine restore \
  --bundle /srv/porcupine-backups/pc_backup_YOUR_ID \
  --volume porcupine-recovered \
  --auth-mode external \
  --reset-access
```

The offline JSON request uses `auth_mode: "external"` and no `bootstrap_token`. A destination authentication configuration takes precedence; a conflicting request or archived mode is rejected before credential reset. An omitted mode defaults to builtin only when no destination mode is provided. Restore does not convert authentication modes.

External restore creates no local recovery administrator and does not prune builtin human, browser, or consent state. Reconfigure the trusted external issuer on the destination and recover administration with its operator JWT. Native machine keys are revoked, public ingestion keys are rotated, pipelines are paused, and exports and pending imports are canceled. Finite external execution snapshots are removed. Recreate native service keys explicitly.

Both modes preserve opaque references to unfinished export and pipeline artifacts before clearing checkpoints. Those references keep tenant deletion pending until reconciled, without contacting the source buckets. This reset/clone workflow is separate from the hosted PITR workflow that preserves secrets and restores shared operational state.

When replacing backend credentials, keep the producer's logical `source_id` so retries use the same event namespace.

Configured email-admin and OIDC provisioning stay disabled after reset. Review those bindings before explicitly enabling `PORCUPINE_RESTORE_IDENTITY_BOOTSTRAP=true`. Fresh API invitations work without this opt-in.

A checkpoint cannot infer event deletions made afterward. Review later deletion requests before reconnecting clients. Retention controls completed backups by count; it does not promise that every old copy disappears after a fixed number of days. Keep the archive until you have checked counts, identity, corrections, deletions and retries in the recovered deployment.
