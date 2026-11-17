# Porcupine documentation

Porcupine collects product events and makes them queryable through DuckDB SQL by default. Connect an agent through MCP, use the standalone CLI, or call the HTTP API. Your agent generates the analysis and charts. Porcupine has no analytics dashboard.

Version v0.14.0 supports DuckDB/DuckLake only. Match the CLI, server, and documentation version.

## Start here

If you are trying Porcupine for the first time, follow the [local quickstart](../README.md#quickstart). It starts an instance, sends a test event, and queries it.

If your team already has an instance, [connect your tools](connect-tools.md), then [query your events](query-events.md). You need the instance URL and access from its administrator.

To let an agent guide you, give it the [agent guide](https://olup.github.io/porcupine/llms.txt) and tell it whether you want to deploy an instance or connect to an existing one. Every guide also has a **View Markdown** link.

## Collect and use data

| Task | Guide |
| --- | --- |
| Send events from a browser or backend | [Choose an input](collect-events.md) |
| Keep using a PostHog SDK | [Configure PostHog capture](compatibility.md) |
| Link anonymous activity to a signed-in user | [Users, identity, and sessions](data-and-identity.md) |
| Bring your existing event history | [Import files with resume](imports.md) |
| Discover events and answer a question | [Query your events](query-events.md) |
| Download a large query result | [Export query results](exports.md) |
| Deliver events or scheduled query results elsewhere | [Webhooks and S3 pipelines](pipelines.md) |
| Fix or remove stored events | [Event correction and deletion](events.md) |

## Run an instance

Start with [hosting](hosting.md). Then [set up team sign-in](connect-your-team.md), [manage projects](projects.md), and [invite people](email-accounts.md). For a backend, cron job, or shared agent, create a [service key](service-keys.md).

The deployment operator chooses [storage](storage.md) and configures [backups](restore.md). DuckLake uses local SSD storage by default; new deployments can select [analytical S3](ducklake-s3.md) for their data files. These settings apply to the deployment and are separate from project permissions. You can also read an authenticated [instance status report](instance-status.md). Operators can configure and monitor [DuckLake history cleanup](ducklake-cleanup.md) and [scheduled native backups](native-backups.md).

Version v0.14.0 includes [remote backups and offline recovery](native-backup-recovery.md).

**Unreleased:** [tenants and usage budgets](tenants.md) group projects under a monthly ingestion allowance and shared interactive query limits. After connecting the instance administrator, [choose a tenant, create its projects, and invite people](connect-your-team.md#choose-a-tenant-for-your-projects-unreleased). A fresh instance already has the `default` tenant for a single team. Invitations grant project permissions; tenant membership and tenant administrators are not implemented. This feature requires a fresh data directory and is not included in v0.14.0.

Optional [shared PostgreSQL deployment](shared-deployment.md) is also unreleased: one binary can use PostgreSQL control/catalog metadata, owner-local SQLite queues, and S3 Parquet. Any worker can serve as a public entry point; accepted events and jobs stay with their durable project owner. This does not provide automatic owner failover or native whole-instance backup/restore.

## Look up a contract

Use the [HTTP API reference](management-api.md), [SQL reference](analytics.md), or [MCP reference](mcp.md) for request fields, permissions, and limits. The [native event contract](native-event-contract.md) describes producer fields and historical message semantics. The running instance serves its OpenAPI contract at `/openapi.json`. In MCP, `porcupine_docs` describes the operations available to your credential.

For deployment constraints and design decisions, read the [architecture](architecture.md), [durability contract](reliability.md), and [current scope](roadmap.md).
