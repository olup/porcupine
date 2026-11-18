# Porcupine documentation

Porcupine stores product events and lets you query them through SQL, MCP, or a standalone CLI. It has no analytics dashboard. Your agent can explore the data and turn query results into a report or chart.

This guide covers **v0.15.1**. Start with the [hosting guide](hosting.md) for deployment and upgrade steps. Use a matching server and CLI; the running instance's contracts tell you which operations it supports.

## Get started

- **New instance:** follow the [quickstart](../README.md#quickstart), then [host it on your server](hosting.md).
- **Existing instance:** [connect your agent or CLI](connect-tools.md), then [query or export events](query-events.md).
- **Let an agent guide you:** give it [llms.txt](https://olup.github.io/porcupine/llms.txt) and your goal. Keep credentials out of the prompt.

Every guide has a **View Markdown** link. It opens the source text in the browser, including on nested reference pages.

## Use your data

| I want to… | Guide |
| --- | --- |
| Send events from my application | [Collect events](collect-events.md) |
| Understand users and sessions | [Identity](data-and-identity.md) |
| Bring historical data | [Import history](imports.md) |
| Discover events, run SQL, or download a large result | [Query or export events](query-events.md) |
| Fix or remove events | [Edit and delete](events.md) |
| Send data to another system | [Pipelines](pipelines.md) |

## Run an instance

[Hosting](hosting.md) covers a laptop or VM. [People and permissions](access.md) covers invitations, tool connections, and service credentials. [Storage](storage.md) explains local files, S3, and the hosted profile with PostgreSQL and S3. Configure [backups](backup-and-restore.md) before storing important data.

Detailed request fields and configuration options live in the [reference](reference/README.md). Development decisions and benchmark records live in [design and validation](design/README.md). The [roadmap](roadmap.md) separates completed work from proposals.
