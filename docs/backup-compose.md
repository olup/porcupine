# Online backup with Docker Compose

This guide applies to the optional ClickHouse backend. For the default DuckLake backend, use [scheduled native backups](native-backups.md) or the [stopped-volume backup procedure](restore.md). These online tools do not create a DuckLake checkpoint.

The optional `compose.backup.yaml` overlay configures a named ClickHouse backup
disk and an on-demand Python operator container. The API uses its normal data
volume. Both the API and the operator must see the same SQLite database and lock
files at `/data/porcupine.db` on a filesystem that supports advisory locks.

On a Linux host, create two private directories on storage with enough room
for a complete checkpoint. The pinned ClickHouse image writes as UID/GID 101
and the operator runs as Porcupine's UID/GID 65532. For example:

```sh
export PORCUPINE_NATIVE_BACKUP_DIR=/srv/porcupine-backups/native
export PORCUPINE_CHECKPOINT_DIR=/srv/porcupine-backups/checkpoints
sudo install -d -m 0700 -o 101 -g 101 "$PORCUPINE_NATIVE_BACKUP_DIR"
sudo install -d -m 0700 -o 65532 -g 65532 "$PORCUPINE_CHECKPOINT_DIR"
```

Keep the directories outside the active data volumes. Preserve the two directory
settings in the deployment environment. Retain all other Compose overlays your
installation uses. For example, a new OAuth deployment using the base and backup
files can start with:

```sh
docker compose -f compose.yaml -f compose.oauth.yaml -f compose.release.yaml -f compose.backup.yaml up -d
```

Adding the disk changes the ClickHouse configuration. Plan the initial
configuration rollout separately from the recurring backup job. For the example
above, run a backup with a unique checkpoint name:

```sh
docker compose -f compose.yaml -f compose.oauth.yaml -f compose.release.yaml -f compose.backup.yaml \
  run --rm --no-deps backup backup \
  --db /data/porcupine.db --clickhouse-url http://clickhouse:8123 \
  --database porcupine --backup-disk backups \
  --output "/checkpoints/$(date -u +%Y%m%dT%H%M%SZ)"
```

The same one-shot command can run from a scheduler in the Compose project directory.
Record its exit status and alert on failure or a missing recent successful checkpoint.

The ClickHouse archive lands in `PORCUPINE_NATIVE_BACKUP_DIR`. The SQLite copy
and manifest land in `PORCUPINE_CHECKPOINT_DIR`. Keep both pieces on independent
durable storage or copy them together off the host after the command succeeds.
The SQLite copy contains private project and authentication state. The native
archive contains event data. Apply appropriate encryption and access controls to both.

During backup the API can accept events into SQLite within its existing queue
budgets. ClickHouse delivery waits at the checkpoint. Size the queue for the
expected ingestion rate and backup duration. Restore into a fresh deployment
using the [online backup and restore procedure](online-backup.md), with both
backup directories available and the API stopped on the restore target.
