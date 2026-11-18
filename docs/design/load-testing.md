# Load testing

The early HTTP load driver was retired with the Python development tools. Its
results remain in the [historical validation report](ducklake-validation.md) and
[maintenance investigation](ducklake-maintenance.md).

Current load scenarios run against disposable fixtures using Go:

```sh
make ducklake-bench
make ducklake-batching-bench
make ducklake-architecture-test
```

The batching and architecture runners use Docker to keep their memory and CPU
limits reproducible. The native benchmark runs directly on the host. They do not
need credentials to an existing deployment or send events to production.

The [hosted flush economics fixture](archive/hosted-economics.md) exercises
PostgreSQL-backed flushes, queries, and compaction against disposable
object storage and records request counts and bytes.

## Standalone queue capacity during outages

The SQLite queue defaults to 100,000 pending original events and 256 MiB of
charged payload per instance. Set `queue.max_events` and `queue.max_bytes` in
`porcupine.json`, or use `PORCUPINE_QUEUE_MAX_EVENTS` and
`PORCUPINE_QUEUE_MAX_BYTES` on the server process. Set `queue.max_bytes` to
`67108864` for a 64 MiB test. [Configuration reference](../reference/configuration.md)

The byte charge is the stored UTF-8 length of the project ID, UUID, event name,
distinct ID, properties JSON, event and receipt timestamps, identity snapshot and
serialized ingestion metadata, plus 64 bytes per row. The service checks the charge after cookieless identity and
property processing. Both limits cover all projects sharing the SQLite queue.

If a new event would exceed either limit, ingestion returns HTTP 503 with
`Retry-After: 1`. A batch either commits completely or leaves the queue unchanged.
Retries with an already accepted stable UUID do not consume more capacity and
remain accepted while the queue is full. Delivery and queued-event deletion
release the corresponding byte charge transactionally.

SQLite triggers maintain count and byte totals in a single accounting row.
Existing pending rows are backfilled once when the accounting schema is first
created. Capture reads those totals without scanning the queue. If an existing
queue already exceeds a reduced limit, the service preserves its accepted rows
and rejects new unique events until enough capacity is released.

This payload budget is separate from SQLite file size. Reserve disk space for
indexes, WAL, durable deduplication receipts, mutation snapshots and other
metadata. During outage tests, use both limits to bound the backlog and inspect
HTTP 503 counts in the report. Increasing the event-count limit alone does not
increase the byte budget. [Hosted memory limits](hosted-architecture.md#live-admission-and-shutdown)
