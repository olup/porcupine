# Coordinated native backups

Porcupine schedules coordinated SQLite and DuckLake backups inside its existing server process. It does not need a backup sidecar or an external database backup script.

For a local directory destination, follow [Configure native backups](native-backups.md). For an independent S3 destination and the standalone offline restore command, follow [Remote backups and recovery](native-backup-recovery.md). Use v0.14.0 or later for S3 backup and CLI recovery.

A complete checkpoint includes the acceptance database, native registry, every project catalog, and the data files those catalogs reference. Projection and its SQLite acknowledgment are coordinated during capture. File and reader protections remain in place while the server copies and verifies the captured state.

**Unreleased two-database layout:** tenant-enabled development builds capture both `acceptance.sqlite`, the local journal, and `control.sqlite`, the accounts, projects, and policy database. Both are required in each `porcupine-native-v2` bundle. Use matching development server and CLI builds; they do not migrate the v0.14.0 single-database format. The offline restorer keeps its incomplete marker until both databases and the reconstructed analytical state are installed. Shared PostgreSQL deployments require a separate recovery procedure.

The manifest is published only after verification succeeds. A failed attempt does not replace the previous good backup. Restore uses fresh isolated storage and the matching server version; it is not an API operation over a running instance.

For a manual local alternative, see the [stopped-volume procedure](restore.md). Analytical S3 requires a coordinated copy of its referenced remote objects as well as the local volume.
