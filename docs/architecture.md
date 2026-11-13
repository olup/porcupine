# Architecture

The default Porcupine deployment runs one Go server with embedded SQLite, DuckDB, and DuckLake. The server serves ingestion, the management API, and MCP. The standalone CLI is an HTTP client and needs no local database.

## From capture to a query

1. An input adapter validates and normalizes a native, PostHog, or Segment event. Credentials select the project.
2. SQLite commits the accepted event, its identity changes, and any webhook work before the server acknowledges capture. The queue has event-count and byte limits.
3. A worker projects accepted events into the project's DuckLake catalog. After the analytical transaction commits, a SQLite transaction records receipts and removes delivered queue payloads.
4. DuckDB executes validated read-only SQL against that project's public `events` view. Private originals, revision heads, and deletion tombstones remain inaccessible to client SQL.

Each project has its own local DuckLake catalog and Parquet directory. A bounded number of project engines can be resident at once. DuckLake manages transactional file replacement and snapshots. Porcupine schedules native maintenance and preserves its own stable-ID, identity, and event-revision rules.

SQLite remains authoritative for projects, grants, OAuth state, receipts, pending events, mutations, and destination jobs. DuckLake stores analytical history. Both are needed for recovery.

## Deployment and isolation

Run one application process per SQLite database and DuckLake directory. Keep the full `/data` volume on persistent local storage. A second writer, a replicated broker, and automatic host failover are outside this deployment model.

Project isolation applies to HTTP, CLI, MCP, interactive queries, exports, and destination pipelines. Native SQL validation rejects external sources, table functions, private tables, writes, and configuration changes. Each engine is restricted to its managed directory. Output, time, and concurrency limits also apply. DuckDB's memory setting is a shared engine budget, not a hard process-memory limit; use a container limit for containment.

## Background work

| Work | Data source | Control |
| --- | --- | --- |
| Event projection | Durable SQLite queue | Server worker |
| Identify, edits, and deletion | Accepted versioned changes | Server worker |
| Native file maintenance | Project DuckLake files | Server worker |
| Webhook delivery | Accepted original-event outbox | Project pipeline |
| One-time SQL export | Validated project SQL | Project export job |
| Scheduled S3 delivery | Closed receipt-time query windows | Project pipeline |

Webhooks and S3 destinations keep their own retry and publication state. Destination delivery does not change analytical storage retention. [Pipeline contract](pipelines.md)

## Storage and recovery

The default backend uses local SSD storage. This release does not provide analytical S3 tiering, remote DuckLake catalogs, online DuckLake checkpoints, or a physical-erasure deadline for historical snapshots. Use a [coordinated stopped-volume backup](restore.md). Query-result exports are separate from backups.

The optional ClickHouse backend uses a private ClickHouse service instead of embedded analytics. Enable it with `compose.clickhouse.yaml`; its SQL dialect, native cold-storage configuration, and online backup tools differ. The [storage guide](storage.md) identifies the supported paths. The pre-migration reference remains on `reference/clickhouse-before-ducklake`.
