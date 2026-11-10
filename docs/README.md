# Porcupine documentation

Start with the [local quickstart](hosting.md#laptop-with-docker-compose) to run the released image, send an event, and query it. For an existing instance, give your agent the [plain-text agent guide](https://olup.github.io/porcupine/llms.txt).

| Task | Guide |
| --- | --- |
| Choose an image or get a checksummed offline archive | [Release downloads](https://github.com/olup/porcupine/releases/tag/v0.6.0) · [Loading an archive](hosting.md#load-a-release-image-without-registry-access) |
| Deploy on a server | [Single VM with HTTPS](hosting.md#single-linux-vm-with-https) · [Separate ClickHouse](hosting.md#application-container-with-a-separate-clickhouse-service) |
| Ingest application events | [PostHog SDK compatibility and configuration](compatibility.md) |
| Invite people or connect a team agent | [Team setup](connect-your-team.md) · [SMTP configuration](email-transport.md) · [Scoped service keys](service-keys.md) |
| Connect and query a project | [OAuth](oauth.md) · [MCP](mcp.md) · [SQL](analytics.md) |
| Send data to a destination | [Per-event or batched webhooks and scheduled S3 queries](pipelines.md) |
| Operate the instance | [Storage](storage.md) · [Backup and restore](online-backup.md) |

[Native and Segment ingestion](native-and-segment-ingestion.md) documents the HTTP endpoints, source bindings, retry receipts, and cross-SDK analytics.

[Import a local PostHog export](posthog-import.md) · [Invite an MCP client](local-invitations.md) · [Configure OIDC login](mcp-interactive-oauth.md)
