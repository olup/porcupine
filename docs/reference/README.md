# Reference

Use the running server's `/openapi.json` for its installed HTTP contract. MCP `porcupine_docs` returns the operations available to the connected credential. Use a matching server and CLI. The CLI's operation help is generated from the same API definition.

## Data

- [SQL, parameters, errors, and limits](analytics.md)
- [DuckDB recipes](duckdb-recipes.md)
- [Event discovery and profiles](event-discovery.md)
- [Native event schema](native-event-contract.md)
- [Native and Segment HTTP inputs](native-and-segment-ingestion.md)
- [PostHog SDK compatibility](compatibility.md)
- [Historical upload protocol](posthog-import.md) and [PostHog export format](posthog-import-format.md)
- [Export API](export-api.md) and [pipeline API](pipeline-api.md)

## Access and management

- [Management API](management-api.md) and [project lifecycle](projects.md)
- [MCP tools](mcp.md)
- [Permissions](security-and-interfaces.md) and [service credentials](service-keys.md)
- [Team sign-in setup](connect-your-team.md) and [email invitations](email-accounts.md)
- [OAuth clients](oauth.md) and [Google/OIDC configuration](mcp-interactive-oauth.md)
- [Email transport](email-transport.md)
- [External authentication](external-auth.md)
- [Tenant access](tenant-access.md), [budgets](tenants.md), and [identity recovery](identity-recovery.md)

## Operations

- [Server configuration keys](configuration.md) and [detailed hosting configuration](hosting-options.md)
- [Analytical S3](ducklake-s3.md) and [hosted runtime](shared-deployment.md)
- [Instance status](instance-status.md), [durability](reliability.md), and [retry receipts](receipts.md)
- [Operator monitoring and the visibility probe](observability.md)
- [Numerical usage and Parquet storage sampling](usage.md)
- [History cleanup](ducklake-cleanup.md)
- [Local backup scheduler](native-backups.md), [S3 backups and offline restore](native-backup-recovery.md), and [Compose backup setup](backup-compose.md)
- [Backup consistency](online-backup.md) and [stopped-volume recovery](restore.md)
