# Porcupine documentation

To try Porcupine on your laptop, [send your first event and query it](../README.md#quickstart). For an existing instance, [connect an agent](mcp.md) or [install the standalone CLI](../cli/README.md). Give an agent the [plain-text guide](https://olup.github.io/porcupine/llms.txt).

| I want to… | Start here |
| --- | --- |
| Self-host | [Deployment options](hosting.md) · [Release downloads](https://github.com/olup/porcupine/releases/tag/v0.9.0) · [Architecture](architecture.md) |
| Give someone access | [Team setup](connect-your-team.md) · [Human accounts](email-accounts.md) · [Email delivery](email-transport.md) · [Interactive OAuth and OIDC](mcp-interactive-oauth.md) |
| Connect a service or shared agent | [Service keys](service-keys.md) · [OAuth resources and scopes](oauth.md) · [Authorization boundaries](security-and-interfaces.md) |
| Capture or identify | [Tested PostHog SDK profiles](compatibility.md) · [Native and Segment inputs](native-and-segment-ingestion.md) · [Cookieless and identified capture](data-and-identity.md) |
| Import historical events | [Resumable PostHog uploads](posthog-import.md) · [Supported file formats](posthog-import-format.md) |
| Answer a product question | [Schema discovery and SQL examples](analytics.md) · [MCP tools](mcp.md) · [CLI usage](../cli/README.md) |
| Manage projects and events | [Management API, rename and delete](management-api.md#operations) · [Event corrections and deletion](events.md) |
| Send data elsewhere | [One-off SQL exports](exports.md) · [Webhooks and scheduled S3 pipelines](pipelines.md) |
| Operate and recover | [Queryable cold storage](storage.md) · [Durability and limits](reliability.md) · [Deduplication receipts](receipts.md) · [Online backups](online-backup.md) · [Recovery drills](restore.md) |
| Size a deployment | [Benchmark results](benchmarks.md) · [Run a load test](load-testing.md) |

The running instance serves its HTTP contract at `/openapi.json`. MCP clients discover the operations available to their credential with `porcupine_docs`.
