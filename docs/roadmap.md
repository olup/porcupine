# Scope and roadmap

Porcupine provides headless ingestion and project-scoped SQL analytics. This document separates implemented capabilities from future work; it is not a release schedule or a production-readiness claim.

## Implemented foundation

The Go server uses a pure-Go SQLite driver for bootstrap, projects, technical credentials, cookieless daily keys, a bounded ingestion queue, delivery receipts, mutation heads, metadata audit,
and a bounded mutation outbox. JSON capture, batches, bounded gzip transport, and opt-in browser ingestion follow [the SDK compatibility profile](compatibility.md).

[The management API](management-api.md) uses Huma request types and generated OpenAPI. Central policies govern project creation, scoped key creation, metadata listing, and revocation. Revocation cannot remove the last active instance administrator. Key expiry and audit history remain future work.

Cookieless capture provides project-scoped daily pseudonyms with a real SDK no-storage contract. These estimate daily network-and-browser combinations, not exact people. Cookieless sessions remain unsupported. Identified capture can link a persisted anonymous browser ID to one identified user within a project.

## Implemented analytics and agent access

A retrying worker delivers to ClickHouse, then atomically records delivery receipts and deletes queued payloads. Operators can keep all receipts in SQLite or enable [bounded receipts](receipts.md). Project views deduplicate at read time before merges. Delivered event history is available through [schema discovery, event discovery, and read-only SQL](analytics.md). The local `/events` endpoint inspects the remaining queue. Revision-checked
[event corrections and deletions](events.md) are implemented with durable
acceptance, idempotent retries, mutation status, and logical deletion tombstones.

Clients and agents write SQL for their question. Supported functions include numeric aggregates, percentiles, `windowFunnel`, and `retention`. There are no named prebuilt metrics, saved SQL catalogs, or HogQL compatibility. SQL has a documented subset, typed scalar parameters, fixed quotas, project-specific readers, invoker views, and physical-table row policies.

[MCP](mcp.md) exposes two tools for permission-filtered documentation and API calls. Schema, event catalog, and SQL operations use the same handlers and policies as HTTP. Static custom Bearer headers are supported. Source builds support [interactive MCP authorization](mcp-interactive-oauth.md) with pre-registered clients and an operator-configured OIDC provider.

`make integration` runs real ClickHouse and real Go SDK MCP clients in an isolated test deployment. It covers discovery, queries, project boundaries, read-only enforcement, quotas, retry deduplication before merges, outages, backpressure, and restart. `make check` does not start ClickHouse. These tests do not establish benchmarks or backup recovery.

## Further analytical capabilities

Add named metrics or saved query catalogs only when an explicit product requirement warrants them. Agent-written SQL already supports aggregation and business-property grouping. Dedicated session analytics, paths, live heartbeats, and reusable views still need definitions, permission rules, time semantics, and bounded results.

General aliasing, unlinking and merging identified users remain outside the supported identity contract. Session support needs a tested SDK contract for `$session_id` propagation.

## Storage and operations

Native [S3 tiering](storage.md) is implemented as global operator configuration,
using a ClickHouse S3 disk and MOVE TTL. The default enabled hot period is 30 days.
Cold history remains queryable through the same API. No project or agent can edit
storage configuration. There is no DELETE TTL or automatic age-based history
expiration. Private asynchronous SQL result exports are implemented separately
from native cold storage. [Webhook and scheduled S3 pipelines](pipelines.md)
provide repeated project delivery.

Revisions replace logical event state, and tombstones prevent resurrection.
Permanent SQLite receipts, mutation audit metadata, and tombstones currently
grow without expiry. Physical cleanup of old parts, S3 versions, and backups is
asynchronous and separate from logical deletion.

The original-event queue has count and byte budgets. Row-specific rejections can be quarantined for operator review. A [coordinated restore drill](restore.md) exercises stopped copies of SQLite, ClickHouse and cold object storage. The [online operator tool](online-backup.md) coordinates backups while capture continues within the queue limits. Scheduling, off-host retention, and long-term storage sizing remain operator responsibilities. Publish performance measurements only with reproducible workloads and deployment settings. Revisit a broker or multiple application processes when measured requirements justify distributed operation.

## Exports and pipelines

[SQL result exports](exports.md) provide durable job creation, polling,
cancellation, gzip CSV or JSONL files, and short-lived S3 download links. A
separate restricted reader has operator-controlled budgets; a delivery barrier
waits for accepted work without promising a snapshot. Publication waits for
complete query validation. Retries rerun the whole query and clean prior
attempts. See [export verification](exports.md#verification-status) for the
recorded local checks and limitations.

[Project pipelines](pipelines.md) support original-event webhooks in event or
closed-window batch mode and scheduled receipt-window SQL delivery to S3. Reads
require `pipelines:read` plus `events:read`; configuration and retries require
`pipelines:write` plus `events:read`. Credentials are encrypted, metadata omits
secrets, and supported restores pause every destination. Receivers deduplicate
stable webhook IDs; S3 consumers use committed window manifests. External
delivery is at least once.

Arbitrary cron schedules, consumer-specific adapters, change-data-capture
streams for corrections, and automatic reopening of completed windows are
outside this implementation. No external integration or production destination
is established by synthetic local checks.

## Documentation index

- [Architecture](architecture.md) explains deployment, data flow, and decision status.
- [Analytics API](analytics.md) documents schema, discovery, SQL, examples, and quotas.
- [SQL result exports](exports.md) documents jobs, formats, links, budgets, and lifecycle requirements.
- [Project pipelines](pipelines.md) documents webhooks, scheduled windows, egress grants, and restore safety.
- [Event mutations](events.md) documents revisions, corrections, deletions, and idempotency.
- [Storage](storage.md) explains operator-only S3 tiering, schema migration, and recovery limits.
- [Events, identity, and sessions](data-and-identity.md) describes cookieless capture, anonymous-to-user linking, and current person properties.
- [Access and interfaces](security-and-interfaces.md) describes permissions and query isolation.
- [Management API](management-api.md) lists endpoints, authorization, and response codes.
- [MCP reference](mcp.md) describes discovery, calls, credentials, and limits.
- [Durability and recovery](reliability.md) states acknowledgment, retry, storage, and verification boundaries.
