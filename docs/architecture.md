# Architecture

Porcupine runs ingestion, queries, and management in one Go server. The standalone CLI is an HTTP client with no native database dependency. MCP and HTTP use the same operation contracts and authorization.

## From an event to a query

1. A native, PostHog, or Segment adapter validates and normalizes the input. The credential selects the project.
2. SQLite commits the event, stable-ID receipt, identity changes, and planned webhook work before capture succeeds.
3. A background worker projects queued work into DuckLake, then confirms delivery in SQLite. Failures leave durable work to retry.
4. DuckDB executes validated read-only SQL against the project's public `events` view. Private revision and deletion state cannot be queried directly.

DuckLake owns transactional catalog/file changes and snapshots. Porcupine schedules maintenance and enforces event identity, revisions, deletion, and retry semantics. Queries see delivered data, so durable acceptance and analytical visibility are separate states.

Each project has its own catalog and managed data namespace. A bounded number of project engines stay resident. More projects do not each receive a permanent process, but active engines and native memory still need sizing.

## Storage responsibilities

The published v0.14.0 server uses one operational SQLite database and local DuckLake catalogs, with local SSD or S3 analytical files.

**Unreleased:** local mode separates operational state into two SQLite files:

- **Control** owns humans, credentials, tenant memberships, projects, policies, and reported usage.
- **Journal** owns accepted payloads, receipts, identity state, acceptance counters, mutations, imports, and delivery jobs.

The two files have matching persistent identities and must be restored together. Optional shared mode replaces control and DuckLake catalog metadata with PostgreSQL and keeps journal SQLite local to each worker. Complete event rows go from that journal directly to S3 Parquet without a PostgreSQL event queue; data inlining is disabled. DuckLake catalog metadata can still hold column statistics containing derived event values.

Each project has a durable owner. Public workers forward owner-bound requests; delivered SQL can run against shared metadata and files. Adding workers does not recover an unavailable owner's queue. See [shared deployment](reference/shared-deployment.md) for supervision, timeout, and failure behavior.

## Boundaries in the code

| Module | Responsibility |
| --- | --- |
| `internal/porcupine` | Service composition, HTTP/MCP handlers, durable transactions, jobs, and cross-module coordination |
| `internal/lake` | Project engines, analytical projection/query, maintenance, and native backup state |
| `internal/nativequery` | SQL AST policy, allowed functions, and bindings |
| `internal/auth` | Principals, grant/consent policy, OAuth validation, provider verification, browser OTP/OIDC sign-in, temporary sessions, and login/email admission |
| `internal/event` | Canonical event model and IDs, independent of storage and adapters |
| `internal/ingest` | Native/Segment live input decoding, normalization, traits, and privacy filtering |
| `internal/importer` | Historical native/PostHog/Segment decoding, manifests, and bounded JSONL parsing |
| `internal/delivery` | Durable webhook enqueueing, batching, attempts, retries and history; webhook destination/DNS policy and HTTP delivery; CSV/JSONL row encoding |
| `internal/s3client` | Common S3 client construction, with separate credentials and transports for each destination |
| `internal/apierr`, `internal/syncx` | Error representation and locking utilities |
| `internal/exportstore` | Export object storage |

This table describes the unreleased stabilization work. The service owns analytical delivery, exports and S3 pipeline jobs, checkpoint commits, quotas, and the atomic acceptance transaction. The webhook queue owns its attempt and retry transactions. Its enqueuer and event-cancellation function instead use the caller's SQLite transaction, so event acceptance and mutation remain atomic with their webhook effects. A three-method host contract supplies current authorization, destination decryption, and the project activity lease. See the [webhook module record](design/webhook-module.md) for the ownership map and verification.

Authentication policy and provider verification live in `internal/auth`; the service still owns control-database identity linking, consent issuance, revocation transactions, and login-version checks. No authentication mode was removed. See the [simplification proposal](design/auth-simplification.md) for decisions still open. The [native boundary design](design/native-only-architecture.md) records these invariants.

## Isolation and limits

Resolve tenant/project scope from authenticated access, never an event's claimed project ID. SQL validation rejects writes, external sources, private tables, and configuration changes. Engines are restricted to managed local storage and the configured S3 namespace.

Bound queues, query concurrency, execution time, returned rows, and result bytes. Unreleased tenant budgets subdivide operator ceilings; they cannot raise them. Per-engine memory is not a hard process RSS limit. A worker/container limit provides additional containment.

Shared usage publication can lag, so monthly quotas permit some overshoot. Concurrent query admission instead reserves a shared place before execution. Auth revocation is checked independently of the usage cache.

## Recovery and deployment

Run one process per local journal and DuckLake root. Preserve the complete volume, control state, private configuration, and matching version. A query export or S3 bucket listing is not an instance checkpoint. Use [coordinated backups](backup-and-restore.md).

The same server can support self-hosting and a future commercial deployment. The commercial controller owns prices and billing and uses an authenticated management API. It does not need a private fork or direct access to the operational database. Separate compute roles and automatic owner failover remain design work, not current deployment guarantees.
