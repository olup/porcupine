# Architecture

Porcupine is an open source, headless product analytics service for small self-hosted deployments. It runs one Go process with embedded SQLite and a separate ClickHouse server. Low operating cost is a design goal; no representative performance or cost benchmark is claimed.

[The roadmap](roadmap.md) separates implemented behavior from proposed capabilities.

## Deployment boundaries

The Go process owns ingestion, authentication, project management, queue processing, SQL validation, and query coordination. A pure-Go SQLite driver keeps local builds free of a C toolchain requirement.

SQLite stores authoritative projects, credential hashes and grants, cookieless daily keys, the query-reader derivation secret, the durable ingestion queue, delivery receipts, mutation heads, metadata audit, the bounded mutation outbox, and delivery metadata. ClickHouse stores revisioned event facts, private original snapshots, and deletion tombstones. SQLite also stores encrypted pipeline credentials, configuration, delivery
state, and scheduled-run checkpoints. SQLite stores identified-person links and current profile traits. The ingestion worker projects accepted identity controls into ClickHouse for project-scoped reads. Optional S3-compatible cold storage uses a global operator-configured ClickHouse disk. [Webhook and scheduled S3 pipelines](pipelines.md) run in the same process.

Docker Compose starts Porcupine and ClickHouse 26.8.36.3 with separate volumes. ClickHouse has no published host port. The application can also run without ClickHouse when `PORCUPINE_CLICKHOUSE_URL` is empty. In that mode, accepted events remain in the bounded SQLite queue, and analytical queries are unavailable.

One instance supports multiple projects. The operator bootstraps its first administrative credential. Clients use HTTP or MCP. Email verification or upstream OIDC enable browser consent. Agents upload local PostHog export files through the resumable HTTP API.

MCP runs in the Go process. Two tools expose a permission-filtered operation catalog, on-demand OpenAPI contracts, and calls by operation ID. Huma registrations define the callable operations and their access policies. Calls dispatch to local API handlers with the caller's credential and reuse validation and authorization.

## Why no broker

The bounded SQLite queue lets a single process commit accepted events before acknowledging them. It avoids operating Redis, PostgreSQL, or a message broker. A ClickHouse outage does not immediately prevent ingestion while queue capacity remains.

A broker becomes a separate decision if independent ingestion nodes, distributed workers, or a larger failure domain become necessary. Those requirements do not currently justify another required service.

## Data flow

1. A client sends events with a project-scoped ingestion credential.
2. The process authenticates, validates, and commits the batch to SQLite.
3. The process acknowledges acceptance after that commit.
4. A worker inserts batches into ClickHouse synchronously.
5. After ClickHouse acknowledgment, one SQLite transaction records project-and-UUID receipts and deletes delivered payloads.
6. SQL reads use a project-specific ClickHouse account, row policy, and deduplicated view.

Acceptance means local durable storage, not immediate analytical visibility. A lost acknowledgment can leave an event visible in ClickHouse while SQLite still reports it pending. Query results and delivery status do not share a cross-database snapshot.

`GET /api/v1/projects/{id}/events` inspects queued payloads. It does not retain delivered history. [The analytics API](analytics.md) provides schema discovery, observed event and property discovery, and read-only SQL against delivered events.

## Query boundaries

Every request targets one authorized project. A parser limits SQL to supported reads against `events` and local CTEs. The process provisions a database account and an invoker-security view for each project. A row policy on the physical event table enforces that project's scope even if the view is bypassed. Client SQL never executes as the provisioning account.

Each view applies `FINAL` to the `events_v2` table using
`ReplacingMergeTree(revision,is_deleted)`. Originals use revision zero; corrections
and deletions use positive SQLite audit sequence numbers. Queries return the
latest projected live row before background merges. Reader settings force final
reads and prevent prewhere optimization from exposing an older revision.
Column-specific grants exclude private original snapshots and tombstone internals. Fixed resource settings and an instance-wide concurrency bound limit analytical work. [Access and interfaces](security-and-interfaces.md) explains these controls.

## Decision status

The accepted design uses one Go process, SQLite as authoritative local state, ClickHouse for analytics, and project-scoped access. Redis, PostgreSQL, and a broker are not required. S3-compatible storage is optional and configured only by the deployment operator.

When tiering is enabled, the default hot period is 30 days for the whole
deployment. A native MOVE TTL transfers eligible ClickHouse parts to the cold
volume asynchronously. The same SQL queries read both volumes. No DELETE TTL
or total-history expiration is installed. Storage settings are not project
configuration or agent-editable management state. See [storage](storage.md) for
configuration and migration, and [event mutations](events.md) for logical deletion.
[One-off SQL exports](exports.md) and [project delivery pipelines](pipelines.md)
are separate from native cold storage.

Identified capture supports project-local anonymous-to-user links and profile traits. See [identity](data-and-identity.md) and [reliability](reliability.md). SDK compatibility is bounded by [the tested profile](compatibility.md).
