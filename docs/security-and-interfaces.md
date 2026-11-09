# Access and interfaces

This document describes current access rules and planned interfaces. The
[management API reference](management-api.md) lists implemented operations.

## Technical access

An operator bootstraps the deployment and creates its first technical access. Operators can configure an upstream OIDC provider and explicit human-to-project bindings for [interactive MCP](mcp-interactive-oauth.md). Each technical credential has an explicit scope, one or more projects, or the whole instance, and a set of permissions.

[OAuth client credentials](oauth.md) authorize machine API and MCP access when
the operator configures an issuer. Clients exchange an ID and secret through
HTTP Basic at `/oauth/token`; only the resulting short-lived access token goes
in Bearer authorization. SQLite stores separate secret and token hashes.
Creation returns a client secret once. Logs must not contain raw credentials.
HTTP and MCP share operation policies, with resource-bound tokens for each.

Each request checks token expiry, parent revocation, and current grants. Effective
authority intersects token scope and issued projects with current permissions
and projects. A later expansion of parent grants cannot expand an old token.
Client revocation invalidates subsequent uses of its opaque access tokens. Interactive MCP uses rotating refresh tokens and per-consent grants when OIDC is configured.

A configured issuer rejects legacy API and MCP Bearer keys by default. Explicit
legacy allowance supports migration. Without OAuth configuration, legacy mode
remains available for offline use. Public single-project PostHog ingestion keys
stay separate, write-only browser credentials and are not OAuth clients.

Huma provides typed management requests, validation, and generated OpenAPI.
SQLite stores client credentials, token hashes, and project grants. Explicit permission checks
run inside the Go process; there is no policy server or role inheritance.

Instance scope is an explicit administrative grant. A project credential cannot create an instance credential, target another project, or grant permissions outside its own authority. Credential creation must validate both requested scope and permissions. An event payload cannot override the project determined by authentication.

## Permission vocabulary

The accepted permission names are:

| Permission | Intended authority |
| --- | --- |
| `instance:admin` | All instance and project operations, including access revocation. Requires `*` project scope. |
| `events:read` | Read events and analytics within scope. |
| `imports:write` | Create and resume historical uploads within a project, together with `events:write`. |
| `events:write` | Ingest events within scope. |
| `events:edit` | Queue a revision-checked full event correction within scope. |
| `events:delete` | Queue a revision-checked deletion tombstone within scope. |
| `management:read` | Inspect permitted project or instance configuration. |
| `management:write` | Change permitted configuration and manage access without escalation. |
| `identity:write` | Reserved for future identity administration. |
| `pipelines:read` | Inspect project pipeline and run metadata, also requiring `events:read`. |
| `pipelines:write` | Configure project destinations and retries, also requiring `events:read`. |

[Event editing and deletion](events.md) are implemented through native mutation
operations. They require their explicit permissions; ingestion authority alone
does not grant either capability. `identity:write` remains reserved, for future administrative operations. Capturing a supported
`$identify` or `$set` uses the project's `events:write` permission. Public
capture credentials let callers supply analytics IDs and traits. Application
authentication and authorization must use a separate source of authority.

## Ingestion compatibility

Porcupine accepts [a tested PostHog ingestion subset](compatibility.md), including
JSON capture, batches, and bounded gzip transport. The matrix names pinned SDKs
and supported settings. It does not claim every endpoint, default SDK behavior,
payload encoding, session behavior, or identity operation. Unsupported identity
control events are rejected.

Backend capture can use an API-resource token with `events:write`. It must
resolve to one project, either through its single-project grant or a validated
public ingestion-key selector already within token scope. The OAuth client ID
never becomes the visitor's `distinct_id`.

Porcupine's own query API serves its native analytics model. HogQL compatibility is not required.

## Analytics and MCP

[The analytics API](analytics.md) provides SQL schema discovery, observed event
and sampled property discovery, and read-only ClickHouse queries. Every request
targets one project and requires `events:read`. Agents write their own SQL.
There are no named prebuilt metrics or saved SQL catalogs. Statistical meaning
still depends on the event model, especially for daily cookieless identities.

[MCP](mcp.md) exposes two tools for documentation and API calls. Discovery filters
operations by the caller's grants. Calls use the same local API handlers,
including SQL validation, body validation, project checks, and anti-escalation
rules. Event names, properties, and SQL results are untrusted data for agents.

MCP uses stateless Streamable HTTP and MCP-resource OAuth tokens in Bearer
headers. RFC 8414 authorization-server metadata and RFC 9728 protected-resource
metadata advertise the machine grant and pinned resource URLs. Tokens for the
API resource cannot authenticate MCP. This does not implement a generic
interactive MCP OAuth login flow. The endpoint rejects cross-origin browser
requests and retains the SDK's localhost DNS rebinding protection. Ingestion
CORS grants do not apply to MCP.

## Restricted SQL

A parser accepts one supported `SELECT` or `WITH` query against `events` and
local CTEs. The exact function allowlist comes from `get-event-schema`. Sequential
CTEs, joins, subqueries, `UNION`, aggregation, and typed scalar parameters are
supported. Unsupported syntax fails closed. Writes, qualified source tables,
system metadata, external sources, table functions, `FORMAT`, `SETTINGS`,
windows, lambdas, `CAST`, and recursive CTEs are excluded.

The configured ClickHouse account provisions schema and project readers. Client
SQL executes as a restricted project-specific account, through an invoker-security
view with a project predicate. A physical-table row policy also restricts that
reader to the same project. The view applies `FINAL` for latest live revision
selection. Reader settings enforce `final=1` and disable prewhere movement for
final reads. Column-only physical-table grants exclude private originals and
tombstone internals. Clients do not receive those database credentials. Even
instance administrators execute SQL through these read-only project readers.

Reader settings fix read-only mode and disable DDL and introspection. They
limit execution to five seconds, memory to 256 MiB, scanned rows to five million,
read bytes to 1 GiB, and results to 1,000 rows and 1 MiB. The request timeout is
ten seconds, with at most two concurrent analytical requests instance-wide.
Supplied SQL cannot change these settings. Parameters stay separate from SQL
and require exact declared names and safe scalar types.

Public statistics include elapsed time, not physical scan rows or bytes.
Delivery status reports only the requested project's pending original and
mutation counts and last success. A generic worker error appears only when that
project has pending work. Internal ClickHouse error messages and credentials are not returned.

`make integration` exercises the real ClickHouse boundaries, including a direct
physical-table read as the project reader. This complements parser restrictions;
SQL parsing alone is not the database isolation boundary.

## Operator storage boundary

[S3 tiering](storage.md) is global deployment configuration. Bucket endpoint,
AWS credentials, ClickHouse policy, cold volume, and hot duration have no HTTP
or MCP management operation. S3 credentials go only to the ClickHouse container.
Project credentials and agent clients cannot change those settings. Native cold
storage preserves query access; it is separate from export pipelines.

## Pipeline boundaries

[Project pipelines](pipelines.md) deliver webhook events and scheduled S3
results. `pipelines:read` or `pipelines:write` alone is insufficient; the caller
also needs `events:read` for the same project. Native storage policy remains
operator-only. One-off export configuration remains separate.

Destination credentials are encrypted with an operator key and are never
returned by metadata APIs or MCP. Webhook URLs and headers are encrypted too.
Workers check the creating principal's current authority. Permission to read
events alone does not authorize configuring a new outbound destination.

Webhook transport permits public HTTPS destinations, checks all DNS answers
at connection time, dials validated addresses, and rejects redirects and
private-network targets. Custom S3 endpoints require an exact origin in the
operator allowlist. These are distinct destination controls; allowing an S3
origin does not permit private webhook traffic.

Supported restores pause every destination. A restore cannot reverse a request
or object write already accepted elsewhere. Stable delivery identifiers and
receiver deduplication remain part of the contract. See the pipeline reference
for limits, credential replacement, and retry behavior.

## Ingestion under load

Each process accepts up to 32 concurrent capture requests across ingestion routes,
and eight concurrent requests to the OAuth token endpoint in an independent pool.
Excess requests receive HTTP 503 with `Retry-After: 1` before body decoding or
SQLite access. Use stable event UUIDs when retrying. Bodies are limited to
1 MiB before and after decompression, and the JSON decoder stops growing a batch
at 1,000 events. These limits bound concurrent work in the process. Configure
rate limits at your reverse proxy if you also need quotas per caller or IP.

For records used in billing or trusted reporting, create a `server_only` project
and use an OAuth writer scoped to that project. A `mixed` project's public key
allows any holder to submit any supported event name. Keep that distinction
when interpreting SQL results. See [OAuth ingestion](oauth.md).
