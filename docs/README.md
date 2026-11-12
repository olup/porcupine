# Porcupine documentation

Porcupine collects product events and makes them queryable through ClickHouse SQL. Connect an agent through MCP, use the standalone CLI, or call the HTTP API. Your agent generates the analysis and charts. Porcupine has no analytics dashboard.

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
| Bring your existing PostHog history | [Import files with resume](posthog-import.md) |
| Discover events and answer a question | [Query your events](query-events.md) |
| Download a large query result | [Export query results](exports.md) |
| Deliver events or scheduled query results elsewhere | [Webhooks and S3 pipelines](pipelines.md) |
| Fix or remove stored events | [Event correction and deletion](events.md) |

## Run an instance

Start with [hosting](hosting.md). Then [set up team sign-in](connect-your-team.md), [manage projects](projects.md), and [invite people](email-accounts.md). For a backend, cron job, or shared agent, create a [service key](service-keys.md).

The deployment operator configures [S3 cold storage](storage.md) and [backups](online-backup.md). These settings apply to the deployment and are separate from project permissions. Development builds also expose an authenticated [instance status report](instance-status.md).

## Look up a contract

Use the [HTTP API reference](management-api.md), [SQL reference](analytics.md), or [MCP reference](mcp.md) for request fields, permissions, and limits. The running instance serves its OpenAPI contract at `/openapi.json`. In MCP, `porcupine_docs` describes the operations available to your credential.

For deployment constraints and design decisions, read the [architecture](architecture.md), [durability contract](reliability.md), and [current scope](roadmap.md).
