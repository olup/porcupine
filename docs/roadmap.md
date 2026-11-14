# Scope and roadmap

## Unreleased

- `get-event-profile` exposes one event's historical activity, occurrence and receipt times, recent examples, and sampled property coverage through HTTP, the generated CLI, and MCP. It shares project permissions and analytics budgets. [Inspect an event](event-discovery.md)

## Available in v0.12.1

- **Capture.** Send events through the [native or Segment interfaces](native-and-segment-ingestion.md), or point a supported [PostHog SDK profile](compatibility.md) at Porcupine. Stable producer IDs support retries. Historical PostHog events and persons can be [imported from JSONL](posthog-import.md).
- **Analytics.** Discover event names, observed properties and the latest example UUID. Run read-only DuckDB SQL with bound parameters by default. The [query reference](analytics.md) describes the supported grammar, limits, and optional ClickHouse dialect. [Identified capture](data-and-identity.md) links anonymous activity to a user. Cookieless capture estimates daily visitors.
- **Delivery.** [Export a query once](exports.md), send events to a webhook individually or in batches, or [schedule a query pipeline](pipelines.md) to publish time windows to S3.
- **Management.** Manage projects, including rename and deletion, through [HTTP](management-api.md), the [CLI](../cli/README.md) or [MCP](mcp.md). Humans can authorize tools using email verification or configured OIDC. Service accounts can use scoped credentials.
- **Operations.** Inspect the [instance status](instance-status.md) through HTTP, CLI, or MCP. The single-host deployment uses [bounded durable queues](reliability.md), local DuckLake files, and a [stopped-volume backup procedure](restore.md). Optional ClickHouse deployments retain their separate cold-storage and online-checkpoint tools.

## Added in v0.12.1

- Embedded light and dark Porcupine icons in MCP server and tool metadata. Icon display depends on the client.

## Added in v0.12.0

- Structured DuckDB query errors with recovery hints.
- Property discovery filters and cursor pagination, independent of event-name pagination.
- Updated MCP descriptions and tested DuckDB query recipes.

## Unreleased reliability fixes

- Maintenance keeps only its current project resident and skips candidates evicted or deleted before their turn. Idle projects remain evictable so maintenance does not exhaust the resident-engine budget when a query opens another project.
- A fully occupied resident-engine budget returns `429` with `active_project_limit` for queries, catalog reads, and export validation. Storage failures remain `503`.

## Unreleased DuckLake operations

- Scheduled native snapshot expiration, deleted-row rewrites and obsolete-file cleanup, including cold project catalogs. Policies are deployment-wide and protect active readers. [Cleanup controls](ducklake-cleanup.md)
- Administrator-only cleanup progress through the existing status API, CLI and MCP.

Coordinated automatic native backups, private status and an isolated restore drill are implemented in the source build. [Backup controls](native-backups.md)

Priority order: analytical S3 storage, publish the next release, validate large-event delivery batches, then complete backup operations. A dedicated offline restore command and automatic off-host backup replication remain future work; native restore currently follows the isolated manual procedure. Saved queries and report collections remain deferred; chart rendering is not a priority.

## Areas for future work

### Next tasks

- [ ] Complete DuckLake operations first: snapshot expiration and storage cleanup after logical deletion, automated coordinated backup and restore, then analytical S3 storage. Define and test physical-erasure guarantees before promising a deadline. Storage policy remains an operator-controlled deployment setting.
- [ ] Validate the cold-project maintenance fix proposed in [PR #33](https://github.com/olup/porcupine/pull/33). Its regression test reproduces one cause of transient `503` responses; it does not establish the cause of every historical storage failure.
- [ ] Check large-event projection batches against the native memory budget. A direct `Apply` fixture with roughly 15 KB of properties per event exceeded the 512 MiB engine budget with 100-row batches, before profiling ran. Ten-row batches loaded the same 1,000 events. Reproduce through the ingestion worker before selecting batch limits or adaptive behavior.
- [ ] After those tasks, make historical imports a single resumable file-or-directory command. Support native, PostHog, and Segment formats with original occurrence timestamps, stable retry IDs, progress, and a final distinction between accepted and queryable events.

The tasks above remain planned. Event profiles are available in development builds as described in the unreleased section.

### Deferred: saved queries and report collections

Saved parameterized SQL queries and collections of queries for reusable reports are on hold. Revisit their need after the operational work. No saved-query endpoints, new permissions, report runner, or UI are being implemented for now. Agents continue to write SQL and use the tested query recipes.

Named metrics, built-in session definitions and live-presence rules remain candidates for future work. Funnels and retention can already be expressed in the supported SQL dialect when the events carry the identities and timestamps the analysis needs.

A replicated queue and multiple ingestion workers would change the failure model and deployment requirements. The current architecture targets a single application process with local durable storage.

The [compatibility matrix](../compat/matrix.json) records the supported PostHog SDK behaviors. General aliasing, merging two identified users, unlinking identities, feature flags and replay are outside that contract. The default query API uses DuckDB SQL. Analytical S3 storage, remote DuckLake catalogs, result caching on DuckLake, finite native receipts, and online native checkpoints remain outside this release. Native snapshot and obsolete-file cleanup are described in the [operations guide](ducklake-cleanup.md).

## Deferred: chart rendering for agents

- [x] Keep a server-side chart renderer out of the current priorities. In the agent workflow exercised with Codex, the agent queried Porcupine through MCP and produced an hourly chart with an application selector in the client. Porcupine supplied the data without a rendering service.
- [ ] Revisit rendering when users need images from a client without visualization support, or unattended reports that must deliver chart files. Client capabilities and display behavior vary; this decision does not assume that every LLM client can render charts.
- [ ] If that need appears, evaluate an optional API/MCP renderer with a declarative chart specification, project authorization, and resource limits. Do not execute model-generated code on the server.

For now, prioritize DuckLake operations and reliable queries, followed by simpler imports. Rendering remains a deferred option, not a commitment to add a UI.
