# Clean up DuckLake history

Porcupine removes a deleted event from analytics after its deletion reaches DuckLake. Older snapshots and Parquet files can still contain the payload. Background cleanup reclaims that history using DuckLake's native rewrite, snapshot expiration and file cleanup operations.

This is separate from event retention. Events that are still active remain queryable regardless of their age.

## Deployment policy

Cleanup is enabled by default. These are instance settings, supplied by the deployment operator. Project credentials and agents cannot change them through the API.

| Environment variable | Default | Purpose |
| --- | --- | --- |
| `PORCUPINE_DUCKLAKE_CLEANUP_ENABLED` | `true` | Set `false` to stop snapshot expiration and obsolete-file cleanup. Normal small-file maintenance continues. |
| `PORCUPINE_DUCKLAKE_SNAPSHOT_RETENTION` | `168h` | Keep historical snapshots for seven days. |
| `PORCUPINE_DUCKLAKE_CLEANUP_GRACE` | `48h` | Wait two days after DuckLake schedules an obsolete file for deletion. |
| `PORCUPINE_DUCKLAKE_CLEANUP_INTERVAL` | `1h` | Minimum interval between successful cleanup cycles for each project catalog. |

Durations use Go syntax, such as `24h` or `30m`, and must be at least `1s`. The defaults leave time for readers and operational investigation. Lower values shorten the history available for recovery; they do not create a secure-erasure guarantee.

The maintenance scheduler considers one due catalog per pass, including registered catalogs that are not currently resident in memory. It uses the existing resident-engine and memory budgets. More projects can extend the effective interval. A failed or busy catalog becomes eligible again after one minute, or the configured interval if shorter. Other catalogs can still progress.

Each cleanup cycle flushes inlined rows, asks DuckLake to rewrite at most one group of files per table containing deleted rows, expires eligible snapshots, and removes eligible obsolete files. Rewriting uses a zero deletion-ratio threshold so a file with only a few deleted rows can eventually be reclaimed. A large backlog can require several cycles. The existing background pass has a 30-second deadline and uses the configured engine memory and thread limits; this is not a separate worker or service.

## Reads and interruption

Porcupine keeps a lease for the full lifetime of every query and export. A reader that began before the cleanup horizon delays expiration and file removal for that catalog until its lease ends. Other project catalogs can continue cleanup. An export can therefore retain old files longer than the default grace period.

Cleanup uses native transactional file replacement. It does not take Porcupine's event-projection write mutex. Ingestion still commits to the durable SQLite queue before acknowledging receipt. A native conflict or interrupted pass is reported and retried. On restart, an unfinished cycle becomes eligible for another attempt. Cleanup retains the minimal event tombstones needed to prevent old retries from resurrecting deleted events.

## Inspect progress

An instance administrator can read `ducklake_cleanup` in the existing status response:

```sh
porcupine get-instance-status
```

The HTTP route is `GET /api/v1/status`. In MCP, discover `get-instance-status` through `porcupine_docs`, then execute it through `porcupine_call`.

The report includes the configured retention and grace in seconds, registered/completed/failed/running catalog counts, and the latest attempt and success times. Counts include cold catalogs. `completed_catalogs` means that at least one cycle completed; it does not mean that every historical file has been reclaimed. A failed or interrupted attempt makes the administrator's overall status `degraded` until that catalog succeeds or is deleted. Project-scoped callers do not receive cleanup policy, counts or timestamps.

The report does not list filesystem paths, payloads, project IDs, or native error text. If cleanup stops succeeding, check the deployment logs, available disk, memory and the configured analytical directory before changing retention.

## What this does not erase

This cleanup removes obsolete native snapshots and managed Parquet files. It does not promise physical erasure within 30 days. Backlogs, long readers, repeated failures and disabled cleanup can delay reclamation.

Corrections preserve the original accepted snapshot privately in the current event for retry verification. That value remains live data until the event is deleted; expiring historical snapshots does not remove it. The minimal deleted-event tombstone contains no event payload.

SQLite free pages and WAL, DuckLake catalog free blocks, stopped-volume backups, filesystem snapshots, S3 object versions, and data already sent to external destinations have separate retention and deletion requirements. This feature does not perform forensic erasure of those copies. It also does not scan the entire analytical directory for orphan files.

Keep a [coordinated backup](restore.md), monitor [instance status](instance-status.md), and distinguish removal from current analytics from reclamation of historical storage.
