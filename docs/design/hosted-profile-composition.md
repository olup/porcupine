# Hosted profile composition

The opt-in hosted profile uses PostgreSQL and S3 for durable state. Standalone uses durable SQLite acknowledgment.

## Configure workers

This profile uses the same server, API, CLI and MCP. Generate a private file
with `porcupine init --hosted`, then add the shared database and analytical
bucket settings. Merge these fields into the generated file and keep its
bootstrap and pipeline secrets.

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
    "restore_safety_margin": "24h",
    "restore_floor_file": "/run/porcupine/restore-floor"
  }
}
```

The shared PostgreSQL URL also configures DuckLake metadata. An explicit
`ducklake.postgres_url` must match it. The server checks the configured grace
against the sum of the three recovery durations. The optional
`restore_floor_file` holds one operator-managed RFC3339 timestamp. It must
reflect the earliest PostgreSQL point the deployment can restore, including
secondary backups. Missing or invalid evidence keeps physical deletion
pending. See the [restore drill](hosted-restore-drill.md).

Run `porcupine config check --config porcupine.json`, then start each worker
with `porcupine --config porcupine.json`. Configure the listener and HTTPS
ingress for your deployment. Each worker needs its own writable scratch
directory. It can be discarded between starts.

Use a fresh PostgreSQL control database. The server records the deployment
profile and generates a new analytical source identity. Existing owner
registries and development catalogs require a separate migration plan. The
worker creates disposable analytical scratch space by default. `storage.ducklake` can point to a private disposable directory.
Keep one consistent configuration and pipeline encryption key across
replicas. The hosted profile rejects local journal and control paths, receipt
retention, owner routing settings and local native backups.

## Runtime behavior and verification

- The integration fixture launches independent copies of the compiled server
  from configuration files and uses the standard HTTP and MCP handlers. It checks
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
- The hosted profile connects PostgreSQL-backed import uploads, mutation intents,
  queries, output jobs and usage with the shared native coordinator.
  The worker schedules guarded compaction and runs scheduled S3 pipelines.
  It also schedules guarded [history cleanup](../reference/ducklake-cleanup.md#hosted-profile).
  That cleanup deletes obsolete files only when the configured grace and the
  recovery floor permit it.
  Project and tenant deletion fence access and can reclaim inventoried objects
  after the configured grace and recovery floor permit it. An unresolved
  scheduled S3 attempt keeps physical deletion pending until its destination
  ownership is verified.
- The two-process fixture proves startup, steady-path acceptance and
  replacement in an isolated environment. S3 validation may use the AWS
  Emulator. Emulator results describe the tested requests and observable
  storage behavior. The [buffered visibility experiment](hosted-buffered-visibility.md)
  records receipt-to-query time, worker kills, and S3 write interruption.
  Production capacity still needs separate measurements.

See the [hosted architecture](hosted-architecture.md) for the storage and
recovery protocols.
