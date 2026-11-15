# Native architecture boundaries

This design records the native-only runtime and transaction boundaries released in v0.14.0 after historical import PR #40.

## What changes

DuckDB/DuckLake is the only analytical implementation. Removing ClickHouse also removes remote query readers, its SQL parser, result cache, finite receipt fallback, storage TTL configuration, and old operating scripts. The native archive format and operational SQLite database stay in place. A SQLite database bound to an unsupported analytical source is rejected rather than silently rebound to a new archive.

The pre-change application had 276 `Service` methods across 63 production files at `97ba6cf`. Code deletion reduces that count, but does not establish ownership. The remaining work is judged by narrower dependencies, whole transactional operations, and tests through normal interfaces.

## Transaction owners

| Operation | Owner and required atomic unit |
| --- | --- |
| Live acceptance | One SQLite transaction commits the event, applicable identity state, queue accounting, and webhook jobs before ACK. |
| Historical upload | One SQLite transaction commits accepted rows, invalid-row savepoints, hash/tail state, offsets, diagnostics, and delivery cursor. No live webhook delivery. |
| Projection | One DuckLake transaction commits revisions and identity projections; a subsequent SQLite transaction records receipts and removes accepted payloads. A lost second commit must be replayable. |
| Mutation | Serialized CAS acceptance commits revision, audit, immutable outbox and deletion effects together. A late original must not resurrect a tombstone. |
| Project deletion | The application drains project activity and records cleanup intent and managed-object obligations. Native deletion retains its durable registry fence until completion. |
| Backup | The coordinator captures SQLite and native catalogs under the projection guard; native file pins remain alive through local or remote copying. |

Product identity belongs to event processing. Human accounts, service credentials, OAuth grants and permissions belong to access control. They do not share an identity model merely because both contain IDs.

## Guards that remain

The lifetime writer lock excludes offline restoration. A shared projection lock spans native writes and SQLite acknowledgments; an exclusive lock protects checkpoint capture. These locks existed before the native migration and remain required.

Projection delivery takes catalog read exclusion before its shared projection guard. Mutation delivery additionally takes the mutation mutex. Native engine operations then obtain a project lease and, for writes, the project's write lock. Deletion drains request activity before taking the catalog write lock and starting SQLite cleanup. It must not wait for request completion while holding a transaction those requests need.

The native registry lock protects short engine/opening/deletion transitions. Remote object work occurs outside it. Cleanup has its own exclusion and reader-bound cutoffs; it does not acquire a blanket application mutation lock. A checkpoint returns a live handle whose close operation releases pins, rather than a path with an implicit lifetime.

## Contract and tooling boundary

Typed Huma registrations remain authoritative. The live registry contains access policies and HTTP-only metadata used by MCP. HTTP and MCP calls still pass through actual authorization.

Native `genapi` builds the full optional-route schema without opening a database. It writes the generated `internal/apischema/management.openapi.json`. The independent, pure-Go `gencli` reads that snapshot and emits the standalone command catalog. The CLI and catalog generator do not import the server or DuckDB. CI checks the snapshot against a served all-options schema and verifies both generated artifacts.

The snapshot is an offline build artifact. Its serialized OpenAPI cannot replace runtime authorization metadata.

## Choosing module boundaries

Two designs were compared: a native archive plus SQLite journal package, and smaller concrete transaction owners inside the application. The archive boundary is useful only if engines, registry, leases, checkpoint pins and native restore internals move together. Exporting the store's mutex or raw database to preserve old callers would defeat it.

The implementation keeps existing persisted event/projection values and narrows SQLite ownership incrementally:

- `eventRules` owns the clock and queue budgets shared by live capture and historical imports.
- `eventAcceptance` receives an authorized, prepared batch and parsed peer data. It owns the live SQLite commit and calls one transaction-scoped webhook enqueue participant.
- `importManager` owns manifests, upload state, byte/hash checkpoints and historical commits. It calls `eventRules.AcceptHistoricalTx` within its existing transaction.
- The HTTP boundary retains authentication, project activity, request admission, body handling and response selection. Neither acceptance nor import state holds a `Service` pointer.

Webhook delivery remains in the application. `webhookAcceptance` retains only pipeline configuration and the shared rules. It uses the acceptance transaction without retaining the application or opening another transaction. This change does not extract destination workers.

`internal/nativequery` owns AST validation, source and function policy, named parameter binding and the discoverable function inventory. It uses an already scoped SQL connection and imports only the standard library. Query and export adapters convert its neutral errors into API errors. The engine, result encoding, checkpoint and restore stay together in the application. This is a SQL-policy boundary, not a claim that archive lifecycle ownership has already moved. A whole-store extraction would need to move that lifecycle as one tested unit.

## Native resource semantics

Interactive queries have two instance-wide admission slots, an execution deadline, and row and output-size bounds. Each resident project engine has its own DuckDB memory setting and threads. That setting is neither a per-query quota nor a hard process RSS ceiling. Multiple resident projects multiply the engine budgets, and native allocations can exceed the buffer-manager setting.

Physical scan quotas and per-query temporary-disk caps are not implemented. API limits report them as unsupported instead of carrying ClickHouse settings that DuckDB does not enforce. Output, bandwidth, file size, concurrency and queue-pressure limits still protect managed exports. Permanent SQLite receipts preserve retry identity and therefore grow with accepted history.

## Verification

Independent native reference results cover hourly trends, dimensions, nearest-rank percentiles, sessions, funnels, retention, timezones, and recomputation after edits/deletions. Equality with a second engine is no longer the oracle.

The retained gates exercise official SDKs, API/CLI/MCP permissions, source-scoped retries, interrupted historical uploads, lost projection acknowledgments, crash recovery, concurrent cleanup, reader-pinned local/S3 backups, offline restoration, project cleanup and destination publication revocation. Structural checks prove that offline tooling has no native runtime dependency. Module extraction must retain these gates and remove the old callers in the same stage.
