# Scope and roadmap

## Available in v0.9.0

- **Capture.** Send events through the [native or Segment interfaces](native-and-segment-ingestion.md), or point a supported [PostHog SDK profile](compatibility.md) at Porcupine. Stable producer IDs support retries. Historical PostHog events and persons can be [imported from JSONL](posthog-import.md).
- **Analytics.** Discover event names, observed properties and the latest example UUID. Run read-only ClickHouse SQL with typed parameters. The [query reference](analytics.md) describes the supported SQL and the 60-second cache. [Identified capture](data-and-identity.md) links anonymous activity to a user. Cookieless capture estimates daily visitors.
- **Delivery.** [Export a query once](exports.md), send events to a webhook individually or in batches, or [schedule a query pipeline](pipelines.md) to publish time windows to S3.
- **Management.** Manage projects, including rename and deletion, through [HTTP](management-api.md), the [CLI](../cli/README.md) or [MCP](mcp.md). Humans can authorize tools using email verification or configured OIDC. Service accounts can use scoped credentials.
- **Operations.** The single-host deployment uses [bounded durable queues](reliability.md), optional [cold storage](storage.md), and a [backup and restore procedure](online-backup.md).

## Areas for future work

Agents currently write the SQL for each question. Named metrics, a saved and parameterized query catalog, built-in session definitions and live-presence rules are candidates for future work. Funnels and retention can already be expressed in the supported SQL dialect when the events carry the identities and timestamps the analysis needs.

A replicated queue and multiple ingestion workers would change the failure model and deployment requirements. The current architecture targets a single application process with local durable storage.

The [compatibility matrix](../compat/matrix.json) records the supported PostHog SDK behaviors. General aliasing, merging two identified users, unlinking identities, feature flags and replay are outside that contract. The query API uses ClickHouse SQL.
