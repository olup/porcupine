# Hosted profile composition

The hosted runtime is available to PostgreSQL and S3 integration fixtures. The
server rejects public startup with this profile while physical project and
tenant deletion and restore-safe object reclamation are incomplete. Standalone
startup and the existing owner-routed shared deployment retain their durable
SQLite acknowledgment.

## Candidate configuration

This profile uses the same server, API, CLI and MCP. To check a candidate
configuration, generate a private file with `porcupine init --hosted`, then
add the shared database and analytical bucket settings. Use
`porcupine config check` or `porcupine config install` to validate the file.
For example, these fields can be merged into the generated file.

```json
{
  "profile": "hosted",
  "acceptance": {"mode": "buffered"},
  "shared": {"control_database_url_file": "/run/secrets/porcupine-postgres"},
  "ducklake": {
    "s3": {"bucket": "example-analytics", "prefix": "porcupine", "region": "eu-west-1"},
    "cleanup_grace": "240h",
    "restore_window": "168h",
    "restore_max_reader_time": "1h",
    "restore_safety_margin": "24h"
  }
}
```

The shared PostgreSQL URL also configures DuckLake metadata. An explicit
`ducklake.postgres_url` must match it. The server checks the configured grace
against the sum of the three recovery durations. The provider's actual PITR
window and S3 retention require an independent check. See the
[restore drill](hosted-restore-drill.md).

Use a fresh PostgreSQL control database. The recorded deployment profile
prevents owner-routed workers and stateless workers from sharing the same
control state. The worker creates disposable analytical scratch space by
default. `storage.ducklake` can point to a private disposable directory.
Keep one consistent configuration and pipeline encryption key across
replicas. The candidate rejects local journal and control paths, receipt
retention, owner routing settings and local native backups.

## Verified and pending

- The fixture starts independent processes through the service startup used
  by the server, then serves the standard HTTP and MCP handlers. It checks
  cross-worker native deduplication, cookieless identity and usage. A fresh
  process reads the committed results after both earlier scratch directories
  are removed.
- The native ingestion schema and authenticated input contract report the
  instance's live acknowledgment. Buffered responses report memory admission.
  Their receipts include `message_id` and `uuid`. A process failure can lose
  pending events. The flush interval does not bound the age of that loss during
  an outage.
- The worker stops admission on shutdown and attempts to flush accepted
  batches. Shutdown reports an incomplete drain if the 30-second flush
  deadline expires. Work still in memory can then be lost. The HTTP shutdown
  has a separate 10-second deadline before the buffer drain.
- The candidate composes PostgreSQL-backed import uploads, mutation intents,
  queries, output jobs and usage with the shared native coordinator.
  Candidate startup does not yet schedule guarded compaction. Scheduled S3
  pipelines are unavailable. Project and tenant deletion fence access, with
  physical reclamation and finalization pending. Restore-safe reclamation
  remains open for several classes of immutable objects.
- The two-process fixture proves startup, steady-path acceptance and
  replacement in an isolated environment. It does not establish production
  capacity, a loss-age objective, managed-provider recovery or full teardown.

The [architecture and phase gates](hosted-architecture.md) apply before public
enablement.
