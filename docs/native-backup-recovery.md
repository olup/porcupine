# Remote backups and offline recovery

Development after v0.13.0. The published v0.13.0 image supports [local recovery bundles](native-backups.md); it does not contain the S3 destination or CLI restore command described here. Build the development server and CLI together to try this workflow.

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

Leave `PORCUPINE_BACKUP_DIR` unset. Use `compose.native-backup-s3.yaml` with the development server build. Preserve the instance's analytical storage, sign-in and proxy overlays.

Backup S3 settings use the same suffixes as [analytical S3](ducklake-s3.md), including `ENDPOINT`, `PATH_STYLE`, `CA_CERT_FILE`, `ACCESS_KEY_ID`, `SECRET_ACCESS_KEY` and `SESSION_TOKEN`. Empty explicit credentials use the AWS credential chain. The destination needs object read, write, list and delete permissions, including conditional creation. Keep its bucket and prefix disjoint from analytical data.

These are deployment settings. Project agents cannot change them. One live writer owns each source and backup namespace. A restored instance gets a new backup writer identity and must use a fresh backup prefix or directory. Raw deployment copies are not a multi-writer setup.

## Inspect a backup

An instance administrator can use HTTP, CLI or MCP `get-instance-status`:

```sh
porcupine get-instance-status
```

The administrator's `native_backup` report includes state, checkpoint, success, `latest_bundle` and `source_id`. Project-scoped callers do not receive it. This source ID selects the analytical backup namespace; it differs from an event's producer `source_id`.

S3 bundles live beneath `PREFIX/native-backups/SOURCE_ID/BACKUP_ID/`. Each completed bundle contains SQLite, project catalogs, data files and `manifest.json`. The manifest describes the bundle layout and original storage identity. It contains no source credentials or deployment environment files.

The server briefly coordinates projection and catalog capture. For remote data it then holds cleanup protection and project deletion pins while ingestion and queries resume. It checks current catalog references, streams data into the backup, verifies hashes by reading the destination back, and publishes the manifest last. Missing current references or failed transfers must not replace the previous good backup.

S3 backups are sequential full copies. Account for storage and transfer costs. Local backups retain Parquet reuse through hardlinks. Bounds are 50,000 files, a 16 MiB manifest and 5 GiB per S3 object. The configured total timeout bounds transfer work. After a crash, incomplete attempts are discarded and recaptured because their old snapshot protections no longer exist.

## Restore a local bundle

Use the same server version that created the bundle. Development builds use the same development image for capture and recovery. The standalone CLI remains CGO-free; Docker runs the native engine for this offline operation.

Create a private file containing a new random bootstrap secret of at least 32 characters. It must differ from every archived credential. Keep it outside the repository.

```sh
porcupine restore \
  --bundle /srv/porcupine-backups/pc_backup_YOUR_ID \
  --volume porcupine-recovered \
  --bootstrap-token-file /secure/porcupine-recovery-token \
  --reset-access \
  --image porcupine-recovery:dev
```

The volume must be new. The CLI puts the secret in private stdin, rather than Docker arguments. The one-shot container reads the private bundle, then gives installed files to the normal nonroot server user. It does not start an HTTP service. Failed attempts retain the volume for inspection. An incomplete installation carries a marker that normal startup refuses.

## Restore an S3 bundle

Configure `PORCUPINE_BACKUP_S3_*` for read access to the archive, then select its IDs:

```sh
porcupine restore \
  --backup-id pc_backup_YOUR_ID \
  --source-id YOUR_ANALYTICAL_SOURCE_ID \
  --volume porcupine-recovered \
  --bootstrap-token-file /secure/porcupine-recovery-token \
  --reset-access \
  --image porcupine-recovery:dev
```

To recover into S3, also set `PORCUPINE_DUCKLAKE_S3_*` for a fresh analytical bucket or prefix. Original analytical storage and archive storage are forbidden targets, including empty namespaces. Without target S3 settings, recovery uses the new volume's SSD. The CLI mounts custom CA and AWS credential files read-only.

The restorer verifies the inventory, checks application and DuckLake formats, and reconstructs current tables in new catalogs. Native `INSERT ... SELECT` transfers data without downloading the full S3 archive onto the SSD. Event IDs, revisions, identity links and tombstones survive. Physical snapshot history is not part of this recovery contract. The target can operate after the archive becomes unavailable.

## Reconnect deliberately

Recovery resets old API keys, OAuth credentials, human sign-ins and invitations. It rotates public ingestion credentials, pauses pipelines and clears old outbound work. The result provides new ingestion credentials; the bootstrap secret stays in your private file. Invite people and create service keys explicitly with the recovery administrator.

When replacing backend credentials, keep the producer's logical `source_id` so retries use the same event namespace.

Configured email-admin and OIDC provisioning stay disabled after reset. Review those bindings before explicitly enabling `PORCUPINE_RESTORE_IDENTITY_BOOTSTRAP=true`. Fresh API invitations work without this opt-in.

A checkpoint cannot infer event deletions made afterward. Review later deletion requests before reconnecting clients. Retention controls completed backups by count; it does not promise that every old copy disappears after a fixed number of days. Keep the archive until you have checked counts, identity, corrections, deletions and retries in the recovered deployment.
