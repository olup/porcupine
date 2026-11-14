# Scope and roadmap

## Available in v0.13.0

- **Capture.** Native JSON, supported PostHog SDK profiles and Segment ingestion. Historical PostHog events and persons can be imported from JSONL. Stable producer IDs support retries. [Inputs](collect-events.md)
- **Analytics.** Project-scoped DuckDB SQL through API, CLI and MCP. Discover event names, properties, activity, recent examples and sampled property coverage with event profiles. Identify links anonymous activity to a stable actor. [Queries](query-events.md) · [Event profiles](event-discovery.md)
- **Delivery.** One-off query exports, individual or batched event webhooks, and scheduled SQL-to-S3 pipelines. [Pipelines](pipelines.md)
- **Management.** Projects, scoped credentials, human email/OIDC sign-in and service accounts, through the shared API contract and agent tools. [Projects](projects.md) · [Permissions](security-and-interfaces.md)
- **Operations.** Local DuckLake by default, optional [analytical S3](ducklake-s3.md) for fresh deployments, native history cleanup and administrator-visible status. [Scheduled backups](native-backups.md) currently support local data files; analytical S3 requires a separate consistent recovery procedure.

## Added in v0.13.0

- Analytical S3 storage with local metadata, pinned storage identity, project prefixes and scoped native credentials. No automatic migration from an existing local archive or age-based native tiering.
- Event profiles with occurrence-based activity, separate receipt times, bounded examples and sampled property statistics.
- Native maintenance no longer pins every resident project at once. Cold project admission and true resident-budget exhaustion have separate error handling.
- Scheduled snapshot expiration, deleted-row rewrites and obsolete-file cleanup, including cold catalogs and reader protection.
- Optional coordinated local recovery bundles, verified inventory, configurable retention, Parquet reuse and a tested isolated restore.
- Release publication depends on the validation workflow and tests the actual native images against HTTPS object storage without runtime extension downloads.

## Development after v0.13.0

Large-event delivery now retries smaller native transactions when DuckDB reports a typed out-of-memory error. Healthy batches keep their existing limits. The acceptance queue retains unfinished work; resource failures do not quarantine events. The [delivery notes](ducklake-large-batches.md) explain partial analytical visibility and memory limits. This change is not included in the published v0.13.0 image.

## Next work, in order

1. **Complete backup operations.** S3-aware backups and off-host replication, then an offline restore command. Retain source/project isolation, safe credential rollback and outbound-delivery controls. Never publish a metadata-only copy as a complete S3 backup.
2. **Simpler historical imports.** One resumable file-or-directory command for native, PostHog and Segment formats, preserving occurrence times and retry identity. Show validation, upload, acceptance and actual analytical availability without requiring a custom Python script.

Before this release, the agreed sequence was analytical S3, release publication, large batches, then backup follow-ups. S3 and publication are the first milestones of that sequence; later items remain development work until released.

## Other planned improvements

- Shared import processing with format-specific adapters, explicit timestamp requirements and original identity/profile semantics. Keep imported history from triggering live destinations by default.
- A clearer native input/import contract for people writing their own adapters. Whether additional official SDK connectors or a plugin framework are useful remains an open question.
- Scheduled SQL results delivered to webhooks, with optional alert conditions, cooldowns and repeat suppression. Reuse durable runs and destination credentials.
- Operational metrics and measured hosting profiles. Add an external queue or multiple writers only when a deployment need justifies the shared state and failure-model changes.

## Deferred

Saved parameterized SQL queries and report collections remain on hold. Agents write SQL and use tested [DuckDB recipes](duckdb-recipes.md). A server-side graph renderer is not a current priority; the client agent can generate graphs where its tools support it.

General aliasing, merging identified users, unlinking, remote DuckLake catalogs, native result caching and finite native receipt history remain outside this release. The [compatibility matrix](../compat/matrix.json) records the exact supported SDK behaviors. Feature flags and replay are outside the current product scope.
