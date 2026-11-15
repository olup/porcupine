# Scope and roadmap

## Available in v0.14.0

- **Capture.** Native JSON, supported PostHog SDK profiles and Segment ingestion. Native, PostHog, and Segment history can be imported from JSONL through one resumable CLI command. PostHog persons files provide profile snapshots. Stable producer IDs support retries. [Inputs](collect-events.md)
- **Analytics.** Project-scoped DuckDB SQL through API, CLI and MCP. Discover event names, properties, activity, recent examples and sampled property coverage with event profiles. Identify links anonymous activity to a stable actor. [Queries](query-events.md) · [Event profiles](event-discovery.md)
- **Delivery.** One-off query exports, individual or batched event webhooks, and scheduled SQL-to-S3 pipelines. [Pipelines](pipelines.md)
- **Management.** Projects, scoped credentials, human email/OIDC sign-in and service accounts, through the shared API contract and agent tools. [Projects](projects.md) · [Permissions](security-and-interfaces.md)
- **Operations.** Local DuckLake by default, optional [analytical S3](ducklake-s3.md) for fresh deployments, native history cleanup and administrator-visible status. [Scheduled backups](native-backups.md) support local data files; [independent S3 backups and offline recovery](native-backup-recovery.md) support both local and analytical S3 data.

## Added in v0.13.0

- Analytical S3 storage with local metadata, pinned storage identity, project prefixes and scoped native credentials. No automatic migration from an existing local archive or age-based native tiering.
- Event profiles with occurrence-based activity, separate receipt times, bounded examples and sampled property statistics.
- Native maintenance no longer pins every resident project at once. Cold project admission and true resident-budget exhaustion have separate error handling.
- Scheduled snapshot expiration, deleted-row rewrites and obsolete-file cleanup, including cold catalogs and reader protection.
- Optional coordinated local recovery bundles, verified inventory, configurable retention, Parquet reuse and a tested isolated restore.
- Release publication depends on the validation workflow and tests the actual native images against HTTPS object storage without runtime extension downloads.

## Added in v0.14.0

- One-command historical imports for native, PostHog and Segment files or directories, including gzip streaming, automatic resume, original occurrence times and stable-ID deduplication. [Import guide](imports.md)
- Published native schemas and `get-event-input-contract` through API, CLI and MCP. [Event contract](native-event-contract.md)
- Smaller analytical transactions after a typed native out-of-memory error. Unfinished work stays queued. [Large-event delivery](ducklake-large-batches.md)
- Complete backups to an independent S3 destination and offline recovery through the standalone CLI. [Backup and recovery](native-backup-recovery.md)
- DuckDB/DuckLake as the only analytical runtime. Explicit acceptance/import/webhook transaction owners, an independent SQL-policy module, and offline CLI contract generation. [Architecture boundaries](native-only-architecture.md)

## Other planned improvements

- Expand historical adapter coverage from real-world export variants while preserving the common acceptance rules.
- Define a separate native historical person-snapshot encoding against the [event contract](native-event-contract.md). Whether additional official SDK connectors or a plugin framework are useful remains an open question.
- Scheduled SQL results delivered to webhooks, with optional alert conditions, cooldowns and repeat suppression. Reuse durable runs and destination credentials.
- Operational metrics and measured hosting profiles. Add an external queue or multiple writers only when a deployment need justifies the shared state and failure-model changes.

## Deferred

Saved parameterized SQL queries and report collections remain on hold. Agents write SQL and use tested [DuckDB recipes](duckdb-recipes.md). A server-side graph renderer is not a current priority; the client agent can generate graphs where its tools support it.

General aliasing, merging identified users, unlinking, remote DuckLake catalogs, native result caching and finite native receipt history remain outside this release. The [compatibility matrix](../compat/matrix.json) records the exact supported SDK behaviors. Feature flags and replay are outside the current product scope.
