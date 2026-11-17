# Architecture

The default Porcupine deployment runs one Go server with embedded SQLite, DuckDB, and DuckLake. The server serves ingestion, the management API, and MCP. The standalone CLI is an HTTP client and needs no local database.

## From capture to a query

1. An input adapter validates and normalizes a native, PostHog, or Segment event. Credentials select the project.
2. SQLite commits the accepted event, its identity changes, and any webhook work before the server acknowledges capture. The queue has event-count and byte limits.
3. A worker projects accepted events into the project's DuckLake catalog. After the analytical transaction commits, a SQLite transaction records receipts and removes delivered queue payloads.
4. DuckDB executes validated read-only SQL against that project's public `events` view. Private originals, revision heads, and deletion tombstones remain inaccessible to client SQL.

Each project has its own local DuckLake catalog and either a local Parquet directory or an isolated analytical S3 prefix. A bounded number of project engines can be resident at once. DuckLake manages transactional file replacement and snapshots. Porcupine schedules native maintenance and preserves its own stable-ID, identity, and event-revision rules.

SQLite remains authoritative for projects, grants, OAuth state, receipts, pending events, mutations, and destination jobs. DuckLake stores analytical history. Both are needed for recovery.

## Unreleased control and ingestion separation

Local development builds use distinct SQLite databases for control metadata and the ingestion journal. `Service.control` owns accounts, credentials, tenants, project definitions, budget policy, and aggregate usage. `Service.db` owns event acceptance, receipt history, identity state, exact local acceptance counters, imports, mutations, and delivery jobs. Journal project and actor rows are uncredentialed foreign-key anchors, not authorization records.

A local acceptance batch refreshes authoritative policy under the standalone admission lock, then atomically accepts events and consumption in the journal. Policy changes use the same lock. Usage reads replay the journal's high-water counters into control without charging twice. Native backups capture both SQLite databases and analytical state; file identities prevent pairing unrelated snapshots. The separate local layout is unreleased and requires a fresh data directory.

## Unreleased shared deployment

The same server can optionally use [shared PostgreSQL metadata](shared-deployment.md) and S3 while keeping a durable SQLite volume on each worker. This configuration is in development and is not included in v0.14.0.

PostgreSQL holds control metadata, authorization, policies, usage reports, worker ownership, query reservations, and DuckLake catalogs. It does not hold event payloads. PostgreSQL-backed DuckLake catalogs disable data inlining, so payloads move directly from the owner's SQLite queue into S3 Parquet files.

Each project has one durable owner for its acceptance queue, receipt ledger, identities, imports, mutations, and jobs. Any public worker can authenticate a request and forward owner-bound work internally. SQL can read delivered data on any worker through the shared catalog and S3. A non-owner query reports `delivery.pending_unknown: true`; schema discovery routes to the owner for authoritative delivery status.

Workers share tenant query reservations and publish cumulative monthly usage with a ten-second budget refresh. Auth revocation is checked against shared control independently of this cache. Owner-local SQLite commits remain the acceptance boundary. Adding workers distributes projects and analytical requests, while a project's writes stay with its owner.

Native PostgreSQL metadata uses bounded connection, statement, lock, and TCP settings, but not every native network call stops immediately on cancellation. Shared interactive execution has a fail-stop watchdog: if native work exceeds its SQL deadline, both I/O grace allowances, and a 15-second margin, the process exits with status `70` before its shared permit becomes reusable. Operators must supervise restart with the same owner volume. [Timeout and supervision contract](shared-deployment.md#native-timeouts-and-worker-supervision)

There is no automatic owner failover without its volume. An unavailable owner produces `503` for owner-bound requests. Recovery requires the shared PostgreSQL metadata, S3 data, and every owner volume; native whole-instance backup and restore are unsupported in shared mode. [Failure and recovery contract](shared-deployment.md#failure-and-recovery-boundary)

## Deployment and isolation

Run one application process per SQLite database and DuckLake directory. Keep the full `/data` volume on persistent local storage. The default deployment has one writer. The unreleased shared deployment has one writer per project owner, with a separate persistent volume per worker. A replicated event queue and automatic owner failover are outside both models.

Project isolation applies to HTTP, CLI, MCP, interactive queries, exports, and destination pipelines. Native SQL validation rejects external sources, table functions, private tables, writes, and configuration changes. Each engine is restricted to its managed local directory and, when configured, its own S3 prefix. Output, time, and concurrency limits also apply. DuckDB's memory setting is a shared engine budget, not a hard process-memory limit; use a container limit for containment.

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

## Code layout

| Package | Owns |
| --- | --- |
| `internal/porcupine` | HTTP, MCP and management handlers, authorization, acceptance, imports, delivery workers, pipelines and exports. |
| `internal/lake` | The DuckLake engine: per-project catalogs, projection, SQL execution, catalog and profile reads, maintenance, checkpoints and the native backup format. It has no HTTP or `Service` dependency. |
| `internal/nativequery` | SQL policy: AST validation, function inventory and parameter binding. |
| `internal/apierr`, `internal/syncx` | The shared API error shape and small locking primitives. |
| `internal/exportstore` | S3 object storage for export files. |

Tests follow the same split: engine tests live in `internal/lake` and run without a server.

## Storage and recovery

The default backend uses local SSD storage. Fresh deployments can keep analytical data files on [S3](ducklake-s3.md), with local SQLite and DuckLake catalogs. Storage identity is pinned to prevent accidental bucket or prefix changes. There is no automatic migration from an existing SSD archive or age-based native S3 tiering. Local archives support [scheduled coordinated backups](native-backups.md); that local-only backup mode is rejected for analytical S3. No physical-erasure deadline is guaranteed. Query-result exports are separate from backups.

Version v0.14.0 uses DuckDB/DuckLake exclusively. The [native boundary design](native-only-architecture.md) records transaction ownership, guards, contract generation, and the staged modularization. Historical comparison code remains on `reference/clickhouse-before-ducklake`.
