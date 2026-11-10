# Architecture

A Porcupine deployment runs one Go server and one ClickHouse server. The Go server embeds SQLite and serves ingestion, the management API and MCP. The standalone CLI is an HTTP client. The [local benchmark](benchmarks.md) reports throughput and resource limits for a specific workload.

## From capture to query

1. A producer sends a native, PostHog or Segment event. Its credential identifies the project and, optionally, the logical source.
2. Porcupine validates the request and commits the accepted batch to SQLite before acknowledging it. Stable producer message IDs allow a client to retry a lost response. SQLite also holds project grants, identity links, import offsets, export jobs and pipeline delivery state.
3. A worker projects accepted events into ClickHouse. After ClickHouse acknowledges a batch, the worker records receipts and removes the delivered payloads in one SQLite transaction. During an outage, capture can continue while queue capacity and the configured receipt policy allow it. See [durability](reliability.md) and [deduplication](receipts.md).
4. An agent discovers event names and properties, then runs a validated SQL query against the project's `events` table. Cached queries can reuse a result for up to 60 seconds. Set `use_cache: false` to read the latest projected rows. The query response reports pending delivery. See [analytics](analytics.md).

The SQLite queue runs on the same persistent volume as the control data. This keeps a single-host deployment independent of a separate broker. Deploy one application process per SQLite database. When ClickHouse is disabled, the HTTP queue inspector can show accepted events, and SQL analytics require enabling ClickHouse.

## Project and query boundaries

Huma registers the management operations and their permissions. The HTTP API, standalone CLI and MCP use that contract. The MCP server exposes two tools for authorized operation discovery and calls by operation ID. Each call invokes the same HTTP handler with the caller's credential. See [access and interfaces](security-and-interfaces.md).

Each project has a restricted ClickHouse reader and an invoker-security view. Row policies on the physical table also enforce project scope. Interactive reads have fixed limits. One-time exports and scheduled S3 runs use separate reader settings and share a background execution slot.

The physical event table uses `ReplacingMergeTree(revision,is_deleted)`. An original row starts at revision zero. Corrections and deletion tombstones get higher revisions. The view uses `FINAL` so queries see the latest projected live row before background merges. Identity fields are resolved at read time. [Event mutations](events.md) and [identity resolution](data-and-identity.md) explain these operations.

Renaming a project preserves its ID and grants. Deleting a project drains its work, removes its local state and checkpoints external cleanup. The server can resume deletion after a restart. See the [management API](management-api.md#operations).

## Where S3 fits

| Use | Data | Who configures it |
| --- | --- | --- |
| [Cold storage](storage.md) | ClickHouse moves eligible parts to a cold volume. SQL continues to read them. The default hot period when enabled is 30 days. | Deployment operator |
| [One-time exports](exports.md) | A project reader requests a SQL result and receives temporary download links to private gzip files. | The operator configures the export store. The reader supplies the query. |
| [Destination pipelines](pipelines.md) | A scheduled SQL query writes a manifest and files for each time window. Webhooks can also send individual events or batches. | A caller with project pipeline permissions supplies the destination. |

The [hosting guide](hosting.md) covers the Compose services, persistent volumes and optional object storage. The [backup guide](online-backup.md) describes how to capture a consistent recovery point across SQLite and ClickHouse.
