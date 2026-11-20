# Access and interfaces

The HTTP API, CLI and MCP use the same project grants. The [management API reference](management-api.md) lists operations and their required permissions.

## Technical access

An operator bootstraps the deployment and creates its first technical access. Operators can enable [email accounts](email-accounts.md). Configured Google or OIDC sign-in authenticates the same invited human accounts and memberships, alongside email verification. See [sign-in configuration](mcp-interactive-oauth.md). Each technical credential has an explicit scope, one or more projects, or the whole instance, and a set of permissions.

[OAuth client credentials](oauth.md) authorize machine API and MCP access when
the operator configures an issuer. Clients exchange an ID and secret through
HTTP Basic at `/oauth/token`; only the resulting short-lived access token goes
in Bearer authorization. The control database stores separate secret and token hashes.
Creation returns a client secret once. Logs must not contain raw credentials.
An administrator can also issue a [static service key](service-keys.md) for a single audience. HTTP and MCP share operation policies.

Each request checks token expiry, parent revocation, and current grants. Effective
authority intersects token scope and issued projects with current permissions
and projects. A later expansion of parent grants cannot expand an old token.
Client revocation invalidates subsequent uses of its opaque access tokens. Interactive MCP uses rotating refresh tokens and per-consent grants with either login method.

A configured issuer rejects legacy API and MCP Bearer keys by default. Explicit
legacy allowance supports migration. Without OAuth configuration, legacy mode
remains available for offline use. Public single-project PostHog ingestion keys
stay separate, write-only browser credentials and are not OAuth clients.

Huma provides typed management requests, validation, and generated OpenAPI.
The control database stores client credentials, token hashes, memberships, and project grants. It uses local SQLite by default or PostgreSQL in hosted. Explicit permission checks run inside the Go process; there is no external policy server.

Instance scope is an explicit administrative grant. A project credential cannot create an instance credential, target another project, or grant permissions outside its own authority. Credential creation must validate both requested scope and permissions. An event payload cannot override the project determined by authentication.

<a id="tenant-roles-and-project-permissions-unreleased"></a><!-- Legacy URL fragment. -->

## Tenant roles and project permissions

Instance and tenant administrators have distinct authority.

| Assignment | Authority |
| --- | --- |
| Instance operator, `instance:admin` | Deployment settings, tenant creation, hard budgets, suspension, and recovery. The operator can administer every tenant. |
| Tenant administrator | That tenant's projects, memberships, and team service accounts. Can inspect its configuration and usage. Cannot change operator budgets or another tenant's resources. |
| Tenant member | Project capabilities granted by that membership. Membership alone does not grant every project. |

Memberships use `project_scope: "all"` or `"selected"`. `all` includes current and future projects in the same tenant. `selected` names projects explicitly. Neither is the deployment-wide `*` grant. An existing analyst connection keeps its approved project selection; a broader membership does not silently widen that connection.

Project roles are fixed permission presets:

| Project role | Permissions |
| --- | --- |
| `reader` | `events:read`, `management:read` |
| `contributor` | Reader permissions plus `events:write`, `identity:write`, and `imports:write` |
| `manager` | Every project permission listed below, including event correction, deletion, and pipelines |

A grant can use a role preset or an explicit permission list. The same person can be a reader of project A and a contributor to project B. Operations check the required capability on the requested project, rather than a union of capabilities across projects. Tool consent can further restrict those rights. Ordinary CLI login requests read scopes. A write-capable member requests explicit project permissions with `porcupine login --scopes`, or authorizes a registered MCP client for those scopes. Custom project scopes cannot be combined with an administrative login mode. Tenant administrators have fixed authority on their own tenant; their membership does not accept project restrictions or custom permission bundles.

The human-only OAuth scope `tenant:admin` authorizes a fixed administrative role in one explicitly selected tenant. A dedicated client and consent page keep it separate from ordinary project analysis and deployment administration. Team machine accounts receive fixed permissions for one project and cannot receive `tenant:admin` or `instance:admin` through tenant provisioning.

Membership changes invalidate affected human-tool connections. Removing a membership preserves access from other memberships and the lifecycle of independent team service accounts. The last active tenant administrator is protected from removal or demotion by another tenant administrator. An instance operator retains recovery authority. See [tenant access](tenant-access.md) and [service keys](service-keys.md).

## Permission vocabulary

The accepted permission names are:

| Permission | Intended authority |
| --- | --- |
| `instance:admin` | All instance and project operations, including access revocation. Requires `*` project scope. |
| `tenant:admin` | Human-only consent scope for administration of one verified tenant. It is never a deployment-wide grant. |
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

[Event editing and deletion](../events.md) are implemented through native mutation
operations. They require their explicit permissions; ingestion authority alone
does not grant either capability. `identity:write` remains reserved, for future administrative operations. Capturing a supported
`$identify` or `$set` uses the project's `events:write` permission. Public
capture credentials let callers supply analytics IDs and traits. Application
authentication and authorization must use a separate source of authority.

## Ingestion compatibility

[Native and Segment inputs](native-and-segment-ingestion.md) accept product events with stable producer message IDs. Public capture uses the [project's browser ingestion key](projects.md#get-a-browser-ingestion-key). The control database stores its public value. Browser keys authorize live capture into a single mixed project. A server can use an API-audience service key or access token scoped to that project.

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
and sampled property discovery, and read-only project SQL queries. Every request
targets one project and requires `events:read`. Agents write their own SQL.
There are no named prebuilt metrics or saved SQL catalogs. Statistical meaning
still depends on the event model, especially for daily cookieless identities.

[MCP](mcp.md) exposes four tools. `porcupine_schema` and `porcupine_query` are
read-only and require `events:read` on the selected project. `porcupine_docs`
discovers operations allowed by the caller's grants. `porcupine_call` can run
read and write operations. Tool execution uses the API handlers, validation,
and permission checks. Agents must treat event names, properties, and SQL
results as untrusted data.

MCP uses stateless Streamable HTTP with a Bearer token or service key for the
MCP audience. RFC 8414 authorization-server metadata and RFC 9728 protected-resource
metadata advertise OAuth grants and resource URLs. Tokens for the API resource
cannot authenticate MCP. With builtin sign-in, people can approve pre-registered
MCP clients with email verification or upstream OIDC. The endpoint rejects
cross-origin browser requests and retains the SDK's localhost DNS rebinding
protection. Ingestion
CORS grants do not apply to MCP.

## Restricted SQL

The default backend parses native DuckDB SQL and checks its AST before execution. Queries can read `events` and local CTEs using allowed functions. Aggregates, joins, subqueries, `UNION`, windows, and scalar casts are supported. Writes, qualified tables, private columns, system metadata, external sources, table functions, recursive CTEs, time travel, and configuration changes are rejected.

Each project has a separate DuckLake catalog and engine context. The public view excludes private originals and tombstone internals. The engine's filesystem permissions are restricted to its managed directory and external access is disabled after initialization. Query parameter values are bound separately, with exact name matching. Even instance administrators use these project-scoped read-only paths.

The [query limits](analytics.md#query-limits) describe execution deadlines, result bounds, and the worker-local capacity pool. The per-project native memory setting is shared by its work and does not cap process RSS. DuckLake enforces no physical scanned-row or byte quota.

Public statistics expose elapsed time without physical scan totals. Delivery status reports only the requested project's work. Storage errors omit raw SQL, server messages, and credentials. Native tests exercise the actual storage boundary in addition to parser rejection.

## Operator storage boundary

Local data paths and engine resource settings belong to deployment configuration. Project credentials cannot change them through HTTP or MCP. Analytical S3 is configured before initializing a fresh archive.

[Analytical S3](ducklake-s3.md) uses operator-controlled bucket endpoints, credentials, and project prefixes. These settings remain outside the management API. Destination pipelines and one-time query exports use separate credentials and retention policies.

<a id="unreleased-shared-deployment-boundary"></a><!-- Legacy URL fragment. -->

### Hosted deployment boundary

[Hosted workers](shared-deployment.md) authenticate against the same control database and accept public traffic through a configured ingress. They derive scope from credentials and check current project and tenant authority. Protect the shared PostgreSQL credentials, deployment secrets, and analytical S3 permissions.

Hosted live input resides in bounded memory before flush. DuckLake writes analytical payloads to S3 and uses PostgreSQL for catalog metadata. Column statistics can include derived event values, so apply analytical access and retention rules to the catalog too.

Native I/O can outlast an HTTP deadline. Hosted workers hold execution-scoped query reservations until calls return and quarantine affected interactive engines when execution is uncertain. Automatic expiry can temporarily permit physical concurrency above the configured limit during a partition. [Hosted execution lifecycle](shared-deployment.md#native-timeouts-and-worker-supervision)

## Pipeline boundaries

[Project pipelines](../pipelines.md) deliver webhook events and scheduled S3
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

Manual full-volume restores require the operator to pause every destination before allowing egress. The [offline native restore](native-backup-recovery.md) resets archived access and pauses destinations automatically. A restore cannot reverse a request
or object write already accepted elsewhere. Stable delivery identifiers and
receiver deduplication remain part of the contract. See the pipeline reference
for limits, credential replacement, and retry behavior.

## Ingestion under load

Each process accepts up to 32 concurrent capture requests across ingestion routes,
and a separate pool of eight for token exchange and revocation.
Excess requests receive HTTP 503 with `Retry-After: 1` before body decoding or
SQLite access. Retry with the original event UUID or producer message ID. The general request limit is 1 MiB before and after decompression and 1,000 events per batch. [Segment](native-and-segment-ingestion.md) also enforces a 32 KiB message limit and a 500 KiB batch limit. These limits bound concurrent work in the process. Configure
rate limits at your reverse proxy if you also need quotas per caller or IP.

For records used in billing or trusted reporting, create a `server_only` project
and use an OAuth writer scoped to that project. A `mixed` project's public key
allows any holder to submit any supported event name. Keep that distinction
when interpreting SQL results. See [OAuth ingestion](oauth.md).
