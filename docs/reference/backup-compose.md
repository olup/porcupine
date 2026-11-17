# Configure backups with Compose

Keep the instance's existing configuration and release overlay. Set one destination in the private [server configuration](configuration.md).

| Destination | Setting | Guide |
| --- | --- | --- |
| Private local directory, local analytical files | `backup.directory` | [Local backup schedule](native-backups.md#enable-a-schedule) |
| Independent S3 bucket or prefix | `backup.s3` | [S3 backup configuration](native-backup-recovery.md#choose-a-backup-destination) |

Use matching server and CLI versions.

The local scheduler runs in the server process and writes as container UID/GID 65532. Create its private host directory with the permissions in the local backup guide. A local backup directory must be outside the live analytical root. S3 backup namespaces must be separate from live analytical storage.

A local-only backup destination is rejected when analytical data lives on S3. It would otherwise omit remote event files. Do not substitute a copied metadata volume or a query-result export for a complete checkpoint.

Inspect progress with `porcupine get-instance-status` using an instance administrator credential. Keep completed backups off the source host and run an isolated [recovery drill](native-backup-recovery.md#restore-a-local-bundle) before relying on them.
