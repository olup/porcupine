# Configure backups with Compose

Keep the instance's existing proxy, sign-in, analytical storage, and release overlays. Add one backup overlay for the destination you need.

| Destination | Overlay | Guide |
| --- | --- | --- |
| Private local directory, local analytical files | `compose.native-backup.yaml` | [Local backup schedule](native-backups.md#enable-a-schedule) |
| Independent S3 bucket or prefix | `compose.native-backup-s3.yaml` | [S3 backup configuration](native-backup-recovery.md#choose-a-backup-destination) |

The S3 destination and offline restore command are development features after v0.13.0. Use a matching development server and CLI when following that guide.

The local scheduler runs in the server process and writes as container UID/GID 65532. Create its private host directory with the permissions in the local backup guide. A local backup directory must be outside the live analytical root. S3 backup namespaces must be separate from live analytical storage.

A local-only backup overlay is rejected when analytical data lives on S3. It would otherwise omit remote event files. Do not substitute a copied metadata volume or a query-result export for a complete checkpoint.

Inspect progress with `porcupine get-instance-status` using an instance administrator credential. Keep completed backups off the source host and run an isolated [recovery drill](native-backup-recovery.md#restore-a-local-bundle) before relying on them.
