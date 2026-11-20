# Architecture

Porcupine runs ingestion, queries, and management in one Go server. The standalone CLI is an HTTP client with no native database dependency. MCP and HTTP use the same operation contracts and authorization.

## From an event to a query

In a SQLite-journal deployment, the following steps take an event from ingestion to query.

1. A native, PostHog, or Segment adapter validates and normalizes the input. The credential selects the project.
2. SQLite commits the event, stable-ID receipt, identity changes, and planned webhook work before capture succeeds.
3. A background worker projects queued work into DuckLake, then confirms delivery in SQLite. Failures leave durable work to retry.
4. DuckDB executes validated read-only SQL against the project's public `events` view. Private revision and deletion state cannot be queried directly.

DuckLake owns transactional catalog/file changes and snapshots. Porcupine schedules maintenance and enforces event identity, revisions, deletion, and retry semantics. Queries see delivered data, so durable acceptance and analytical visibility are separate states.

Each project has its own catalog and managed data namespace. A bounded number of project engines stay resident. More projects do not each receive a permanent process, but active engines and native memory still need sizing.

## Storage responsibilities

Standalone stores analytical data on local SSD or S3 and keeps operational state in two SQLite files.

- **Control** owns humans, credentials, tenant memberships, projects, policies, and reported usage.
- **Journal** owns accepted payloads, receipts, identity state, acceptance counters, mutations, imports, and delivery jobs.

The two files have matching persistent identities and must be restored together.

The opt-in [hosted profile](reference/shared-deployment.md#configuration) uses PostgreSQL control and DuckLake catalogs and S3 analytical storage. Live requests acknowledge bounded memory admission. Shared upload checkpoints and mutation intents survive worker replacement. Worker crashes can lose acknowledged live data that has yet to commit.

## Boundaries in the code

This map follows the source checkout. See [storage profiles](storage.md) for deployment behavior.

| Module | Responsibility |
| --- | --- |
| `internal/porcupine` | Service composition, HTTP and MCP handlers, SQLite acceptance transactions, and coordination across stores |
| `internal/lake` | Project engines, native query execution, analytical projection, maintenance, and native backup state |
| `internal/query` | Admission and execution lifetime for interactive SQL, catalog, profile, and hosted event-list reads. Owns hosted permit renewal and worker capacity shared with exports and scheduled S3 queries. |
| `internal/catalog` | Materialized event catalog persistence and refresh, description storage, and documentation review rules |
| `internal/nativequery` | SQL AST policy, allowed functions, and bindings |
| `internal/auth` | Principals, grant/consent policy, OAuth validation, provider verification, browser OTP/OIDC sign-in, temporary sessions, and login/email admission |
| `internal/event` | Canonical event model and IDs, independent of storage and adapters |
| `internal/ingest` | Native/Segment live input decoding, normalization, traits, and privacy filtering |
| `internal/acceptance` | Validated live batches and acknowledgment types. Owns the hosted memory buffer, flush scheduling, retries, and drain. |
| `internal/importer` | Historical decoding, manifests, bounded JSONL parsing, and shared import row decisions |
| `internal/delivery` | Webhook destination and DNS policy, HTTP delivery, and CSV/JSONL row encoding |
| `internal/identity` | Parse and evaluate event identity controls against the current links and profiles |
| `internal/output` | SQLite and PostgreSQL job stores, export and pipeline orchestration, webhook queues, and artifact cleanup |
| `internal/usage` | Durable counters, execution observations, storage samples, and report snapshots |
| `internal/observability` | Process-local Prometheus counters and histograms, text encoding, and shared flush metrics |
| `internal/s3client` | Common S3 client construction, with separate credentials and transports for each destination |
| `internal/apierr`, `internal/syncx` | Error representation and locking utilities |
| `internal/exportstore` | Export object storage |

In standalone, the service attaches webhook enqueue and cancellation to the caller's SQLite transaction. In the hosted profile, the service commits buffered batches to DuckLake and reconciles retained proofs into shared output jobs and usage. The output stores own their attempt and retry transactions. [Hosted flush protocol](design/hosted-architecture.md#flush-and-publication)

Authentication policy and provider verification live in `internal/auth`; the service still owns control-database identity linking, consent issuance, revocation transactions, and login-version checks. No authentication mode was removed. See the [simplification proposal](design/auth-simplification.md) for decisions still open. The [native boundary design](design/native-only-architecture.md) records these invariants.

## Isolation and limits

Resolve tenant/project scope from authenticated access, never an event's claimed project ID. SQL validation rejects writes, external sources, private tables, and configuration changes. Engines are restricted to managed local storage and the configured S3 namespace.

Bound queues, query concurrency, execution time, returned rows, and result bytes. Tenant budgets subdivide operator ceilings; they cannot raise them. Per-engine memory is not a hard process RSS limit. A worker/container limit provides additional containment.

Shared usage publication can lag, so monthly quotas permit some overshoot. Concurrent query admission instead reserves a shared place before execution. Auth revocation is checked independently of the usage cache.

## Recovery and deployment

Run one process per local journal and DuckLake root. Preserve the complete volume, control state, private configuration, and matching version. A query export or S3 bucket listing is not an instance checkpoint. Use [coordinated backups](backup-and-restore.md).

The same server can support self-hosting and a future commercial deployment. The commercial controller owns prices and billing and uses an authenticated management API. It does not need a private fork or direct access to the operational database. Hosted replicas share durable state and can be replaced independently. [Hosted architecture](design/hosted-architecture.md)
