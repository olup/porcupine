# Scope and roadmap

## Available in v0.11.0

- **Capture.** Send events through the [native or Segment interfaces](native-and-segment-ingestion.md), or point a supported [PostHog SDK profile](compatibility.md) at Porcupine. Stable producer IDs support retries. Historical PostHog events and persons can be [imported from JSONL](posthog-import.md).
- **Analytics.** Discover event names, observed properties and the latest example UUID. Run read-only DuckDB SQL with bound parameters by default. The [query reference](analytics.md) describes the supported grammar, limits, and optional ClickHouse dialect. [Identified capture](data-and-identity.md) links anonymous activity to a user. Cookieless capture estimates daily visitors.
- **Delivery.** [Export a query once](exports.md), send events to a webhook individually or in batches, or [schedule a query pipeline](pipelines.md) to publish time windows to S3.
- **Management.** Manage projects, including rename and deletion, through [HTTP](management-api.md), the [CLI](../cli/README.md) or [MCP](mcp.md). Humans can authorize tools using email verification or configured OIDC. Service accounts can use scoped credentials.
- **Operations.** Inspect the [instance status](instance-status.md) through HTTP, CLI, or MCP. The single-host deployment uses [bounded durable queues](reliability.md), local DuckLake files, and a [stopped-volume backup procedure](restore.md). Optional ClickHouse deployments retain their separate cold-storage and online-checkpoint tools.

## Unreleased improvements

- Structured DuckDB query errors with recovery hints.
- Property discovery filters and cursor pagination, independent of event-name pagination.
- Updated MCP descriptions and tested DuckDB query recipes.

## Areas for future work

Agents currently write the SQL for each question. Named metrics, a saved and parameterized query catalog, built-in session definitions and live-presence rules are candidates for future work. Funnels and retention can already be expressed in the supported SQL dialect when the events carry the identities and timestamps the analysis needs.

A replicated queue and multiple ingestion workers would change the failure model and deployment requirements. The current architecture targets a single application process with local durable storage.

The [compatibility matrix](../compat/matrix.json) records the supported PostHog SDK behaviors. General aliasing, merging two identified users, unlinking identities, feature flags and replay are outside that contract. The default query API uses DuckDB SQL. Analytical S3 storage, remote DuckLake catalogs, result caching on DuckLake, finite native receipts, online native checkpoints, and automatic snapshot expiration remain outside this release.

## Open question: chart rendering for agents

- [ ] Check which target LLM clients provide code execution, plotting and file delivery. Capabilities depend on the client, tools and plan, not just the model.
- [ ] Decide whether query results and downloadable exports suffice, or an optional API/MCP chart renderer would help clients without an execution environment.
- [ ] If a renderer is useful, compare a declarative chart specification and a small set of chart types. Evaluate image delivery, project authorization and resource limits without executing model-generated code.

This is a research question, not a commitment to add rendering or a UI.
