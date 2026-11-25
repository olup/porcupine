# Management API reference

This reference lists HTTP operations, required permissions, and response codes. For a task walkthrough, start with [projects](projects.md), [people](email-accounts.md), [queries](../query-events.md), or [pipelines](../pipelines.md).

The running server publishes its OpenAPI contract at `GET /openapi.json`. The CLI and MCP use the same operation IDs and authorization rules. The [analytics API](analytics.md) describes the project schema, discovery, and SQL queries.

## Authentication and scope

[`list-usage`](usage.md) at `GET /api/v1/usage` exposes replayable numerical consumption by tenant/project and UTC day/month. It requires the instance operator; tenant administrators retain their separate `get-tenant-usage` allowance view.

Send a Bearer credential in the Authorization header. A builtin standalone
server without OAuth accepts static access keys. With builtin OAuth, use an
access token or a service key for the `/api/v1` audience. A person can authorize
the CLI with email sign-in. A machine can exchange its client credentials for a
short-lived token. See the [CLI setup](../../cli/README.md), [OAuth token
exchange](oauth.md#token-exchange), and [service keys](service-keys.md).

[Externally issued access JWTs](external-auth.md) use the configured API audience. Native service keys remain available. Local token exchange, human invitations, memberships, and consent management are absent. Machine-account operations remain available, but return no local OAuth secret in this mode.

On first builtin startup, `PORCUPINE_BOOTSTRAP_TOKEN` creates an `instance:admin`
principal with `*` project scope. Without OAuth, the original secret is a static
Bearer credential. With OAuth, exchange the secret through the client ID
`bootstrap`. The alias stays pinned to the first administrator. Subsequent
startups keep the stored credential.

Each principal has permissions and project IDs. `*` includes all projects.
`instance:admin` grants all operations and requires `*` scope. An access token's
issued scope and project snapshot intersect current parent grants on every
request. Operators can configure [email accounts](email-accounts.md) for interactive API and MCP access. [Google or another trusted OIDC provider](mcp-interactive-oauth.md) can authenticate those same invited accounts, with email OTP available alongside it when SMTP is configured. Both login methods use the same current permissions. Authentication
runs before request validation. HTTP and [MCP](mcp.md) share operation policies
and project checks, with separate API and MCP token resources. The CLI generates its commands from the same OpenAPI operations.

## Operations

`query-events` executes fresh DuckDB SQL against delivered state. See [query consistency](analytics.md#fresh-queries).

| Operation | Required authority | Success |
| --- | --- | --- |
| `GET /api/v1/status` | `management:read`; no project data or secrets in the [instance report](instance-status.md) | `200`, including a degraded report |
| `GET /api/v1/projects` | `management:read`; results restricted to permitted projects | `200` |
| `POST /api/v1/projects` | Instance operator or administrator of the selected tenant | `201` |
| `GET /api/v1/projects/{id}/client-ingestion-key` | `management:read` for this project | `200` with the active public key, or `404` if absent |
| `POST /api/v1/projects/{id}/client-ingestion-key` | Instance operator or administrator of the owning tenant | `200` with the active public key, or `409` for `server_only` |
| `PATCH /api/v1/projects/{id}` | `management:write` for this project | `200` with the updated project |
| `DELETE /api/v1/projects/{id}` | Instance operator or administrator of the owning tenant | `204` |
| `GET /api/v1/access-keys` | `management:read`; results restricted to keys whose entire project scope the caller holds | `200` |
| `POST /api/v1/access-keys` | `management:write`; every requested permission and project must be held by the caller | `201` |
| `DELETE /api/v1/access-keys/{id}` | Instance operator or administrator of the tenant that owns the key | `204` |
| `POST /api/v1/oauth-clients` | `management:write`; requested grants cannot exceed the caller | `201` |
| `GET /api/v1/oauth-clients` | `management:read`; complete project scope required for each returned client | `200` |
| `DELETE /api/v1/oauth-clients/{id}` | `instance:admin`, with [recovery administrator protection](#project-and-credential-details) in builtin mode | `204` |
| `POST /api/v1/oauth-clients/{id}/api-keys` | `instance:admin`; service key must fit the machine client's grants | `201` |
| `GET /api/v1/oauth-grants` | `instance:admin` | `200` |
| `DELETE /api/v1/oauth-grants/{id}` | `instance:admin` | `204` |
| `POST /api/v1/humans` | `instance:admin` on an email-enabled instance | `201` |
| `GET /api/v1/humans` | `instance:admin` on an email-enabled instance | `200` |
| `PUT /api/v1/humans/{id}` | `instance:admin` on an email-enabled instance | `200` |
| `DELETE /api/v1/humans/{id}` | `instance:admin` on an email-enabled instance | `204` |
| `GET /api/v1/projects/{id}/events?limit=100` | `events:read` and access to the project | `200` |
| `GET /api/v1/projects/{id}/quarantined-events` | `events:read` and access to the project | `200` |
| `POST /api/v1/projects/{id}/quarantined-events/{event_id}/retry` | `events:edit` and access to the project | `202` |
| `GET /api/v1/projects/{id}/schema` | `events:read` and access to the project | `200` |
| `GET /api/v1/projects/{id}/catalog` | `events:read` and access to the project | `200` |
| `GET /api/v1/projects/{id}/event-profile` | `events:read` and access to the project | `200` with [activity and sampled properties](event-discovery.md) |
| `GET /api/v1/projects/{id}/input-contract` | `events:read` and access to the project | `200` with the [native producer contract](native-event-contract.md) |
| `POST /api/v1/projects/{id}/query` | `events:read` and access to the project | `200` |
| `GET /api/v1/projects/{id}/events/{uuid}` | `events:read` and access to the project | `200` |
| `PATCH /api/v1/projects/{id}/events/{uuid}` | `events:edit` and access to the project | `202` |
| `DELETE /api/v1/projects/{id}/events/{uuid}` | `events:delete` and access to the project | `202` |
| `GET /api/v1/projects/{id}/event-mutations/{mutation_id}` | `events:read` and access to the project | `200` |
| `POST /api/v1/projects/{id}/event-deletions` | `events:read`, `events:delete`, and access to the project | `200` preview, `202` accepted |
| `GET /api/v1/projects/{id}/event-deletions/{mutation_id}` | `events:read` and access to the project | `200` |

For managed SQL exports, see the [export job operations](export-api.md#api-operations). The [pipeline operations](pipeline-api.md#permissions-and-api) configure recurring deliveries. [Historical imports](posthog-import.md) use HTTP or the CLI for the entire import, including binary chunk uploads.

### Tenant operations

[Tenant administrators](tenant-access.md) manage their own tenant with `tenant:admin`. Instance operators retain deployment-wide authority, including [recovery of upstream sign-in identities](identity-recovery.md). HTTP, CLI, and MCP use the same operation registry and permission checks. Project readers can inspect configured `tenant_budgets`, `deployment_limits`, and effective `limits` through `get-event-schema`, including the configured ingestion allowance but no tenant-wide consumption.

| Operation ID | Route | Authority | Success |
| --- | --- | --- | --- |
| `list-tenants` | `GET /api/v1/tenants` | Administered tenants only; operators see all | `200` |
| `create-tenant` | `POST /api/v1/tenants` | Instance operator | `201` |
| `get-tenant` | `GET /api/v1/tenants/{tenant_id}` | Administrator of this tenant or instance operator | `200` |
| `update-tenant` | `PATCH /api/v1/tenants/{tenant_id}` | Tenant administrator for names; instance operator for ingestion suspension | `200` |
| `update-tenant-budgets` | `PUT /api/v1/tenants/{tenant_id}/budgets` | Instance operator | `200` |
| `get-tenant-usage` | `GET /api/v1/tenants/{tenant_id}/usage` | Administrator of this tenant or instance operator | `200` |

`list-tenants` returns `tenants` and an optional `next_cursor`. Its `limit` defaults to 100 and accepts 1–200. Continue with `cursor=NEXT_CURSOR`. `create-tenant` accepts `name` and an optional complete `budgets` object. `get-tenant`, creation, and updates return `{"tenant":{...}}`.

`update-tenant` accepts `name`, `ingestion_allowed`, or both. Suspension prevents new acceptance while allowing retries and delivery of already accepted events. `update-tenant-budgets` replaces the complete budget object without resetting usage:

| Budget field | Accepted value |
| --- | --- |
| `ingestion_monthly_events` | `null` for unlimited, or a nonnegative integer; zero prevents new acceptance |
| `ingestion_monthly_exceptions` | Omit or pass `null` for no additional limit, or set a nonnegative integer to cap `$exception` rows within the shared allowance |
| `hot_query_max_concurrent` | 1–64 |
| `hot_query_timeout_ms` | 1–60,000 |
| `hot_query_max_rows` | 1–100,000 |
| `hot_query_max_result_bytes` | 256–67,108,864 |

Query limits are further bounded by the deployment's configured ceilings. Creating a tenant without `budgets` selects unlimited ingestion and deployment-default interactive limits.

`get-tenant-usage` accepts an optional `period=YYYY-MM`, defaulting to the current UTC calendar month. It returns `tenant_id`, `period_start`, `period_end`, decimal-string `accepted_events`, nullable decimal-string `remaining_events`, and effective `ingestion_allowed`. It also returns decimal-string `accepted_exceptions`, nullable decimal-string `remaining_exceptions`, and effective `exception_ingestion_allowed`. The exception count is part of `accepted_events`. The exception balance applies only to the exception quota. The effective flag also checks the shared quota and the tenant ingestion switch. Exception accounting begins at upgrade, so the first UTC month can be partial.

Usage counts new unique canonical rows at durable acceptance, including imports and identity controls. Rejected, ignored, duplicate, and rolled-back rows consume nothing. Deletes do not refund usage. Reading an earlier month applies the current budget to that month's recorded counter; it does not recover historical budget settings. Deletes do not reset retained usage.

`create-project` accepts optional `tenant_id`, defaulting to `default`, and requires administration of that tenant or instance administration. The same tenant authority permits project deletion and deletion retries. Project responses include immutable `tenant_id`. `list-projects` accepts an optional tenant filter but still returns only authorized projects. Explicit project grants may span tenants; `*` remains deployment-wide.

Tenant membership operations invite, list, update, and revoke people under `/api/v1/tenants/{tenant_id}/members`. Role presets are discoverable with `get-tenant-project-roles`. [Team access](tenant-access.md) describes read-only, mixed project grants, future-project access, and last-administrator protection. [Team service accounts](service-keys.md) have separate account and key lifecycles under the tenant. Tenant administrators can manage their tenant's public ingestion keys through the access-key operations. They cannot create instance administration credentials or manage global OAuth registrations.

Operators can list upstream identity bindings with `list-human-identities` and unlink one with `unlink-human-identity`. See [identity recovery](identity-recovery.md) for revocation effects and the HTTP, CLI, and MCP workflow.

The [tenant guide](tenants.md) explains shared allowances, resumable imports, quota response codes, and the standalone and hosted deployment boundaries.

### Project and credential details

Project creation accepts `{"name":"Example"}` and returns a `mixed` project plus
a public `ingest_token` for live capture into that project. With OAuth
enabled, send `{"name":"Trusted service","ingestion_mode":"server_only"}` to
require authenticated backend writes. The response omits the public key. The
creation operation requires the same permissions for both modes. See the
[ingestion policy](oauth.md#project-ingestion-policy).

`get-client-ingestion-key` and `ensure-client-ingestion-key` both take a project ID and return `{"key":"..."}`. Ensure takes no request body and creates a key only when the mixed project has no active client ingestion key. Existing hash-only keys retain their scopes and continue working.

`rename-project` accepts `{"name":"Research"}`. Names must contain 1–200 UTF-8 bytes after trimming whitespace. The response contains the updated `project`. Existing SDKs, dashboards and pipelines continue to address the project by ID.

`delete-project` accepts the project ID in the path and takes no request body. An authorized administrator can use it to remove a project's queued and analytical events, metadata, pipeline definitions and Porcupine-managed exports. Copies already delivered to customers' webhook or S3 destinations and existing backups are outside the deletion scope. Bucket lifecycle rules control retention of noncurrent object versions. See [export cleanup](export-api.md#bucket-permissions-lifecycle-and-cleanup). If a storage dependency is unavailable and the request returns `503`, retry the same ID. The server also retries pending cleanup every minute and on restart. A repeat request for a completed deletion returns `204`. An unknown ID returns `404`.

Deleting a project removes it from access grants. Changing a parent grant also revokes its derived keys and sessions under the usual credential rules. A user or service with access to a surviving project may need to reconnect or obtain a replacement key.

OAuth client creation accepts this body:

```json
{
	"name": "Analytics reader",
	"permissions": ["events:read", "management:read"],
	"projects": ["PROJECT_ID"]
}
```

The response contains `client_id`, one-time `client_secret`, `name`,
`permissions`, `projects`, and `created_at`. The secret is for token exchange,
not direct API authorization. Names contain 1 to 200 UTF-8 bytes and cannot be whitespace only.
`create-oauth-client` requires `instance:admin`. Instance administrators can also use `create-access-key`. A tenant administrator with `management:write` and `events:write` can use `create-access-key` to issue an ingestion key for exactly one project in their own tenant. Use [team service accounts](service-keys.md) for private team access.

Project credentials with `management:write` can still rename their project. Credential creation through either operation returns `403` for callers without the required administrative authority. Permissions and projects must each contain at least one entry. Project IDs must exist unless the scope is `*`.

OAuth client listing returns `{"enabled":true,"oauth_clients":[...]}` without
secrets or hashes. Items contain `client_id`, name, grants, creation time, and
nullable revocation time. The bootstrap alias is not listed as a registered
client. See [OAuth client management](oauth.md#bootstrap-and-registered-clients).

With a builtin or external issuer, access-key creation requires
`permissions: ["events:write"]` and exactly one project. The one-time `token`
can authorize capture for a `mixed` project or select a project for an
authenticated API writer. See [ingestion credentials](oauth.md#browser-and-backend-ingestion).

A builtin standalone instance without OAuth also lets instance administrators
issue scoped static Bearer access keys for API and MCP operations.

Key listing returns `{"access_keys":[...]}`. Each item contains `id`, `kind`, `name`,
`permissions`, `projects`, `created_at`, and `revoked_at`. Active keys have
`revoked_at: null`. Revoked keys remain visible within scope. A key spanning
projects A and B is omitted entirely from a caller restricted to A. Use `kind: "client_ingestion"` to identify a project's browser key for revocation.

Client or principal revocation disables future authentication, including all
access tokens issued to it. Ordinary already-authenticated requests are not
automatically canceled. Exports also recheck creator authority while running.
In builtin mode, revoking the last independent instance administrator returns
`409`. With builtin OAuth, only the bootstrap administrator and machine clients
with `instance:admin` and the `client_credentials` grant qualify. Without OAuth,
the bootstrap administrator and other independently issued static keys with
`instance:admin` qualify. The check and update share a control database
transaction.

Access tokens expire; public ingestion keys and client secrets have no expiry
or dedicated rotation endpoint. Rotate a client by creating a replacement and
revoking the old principal.

The event-read operation returns `{"events":[...]}` from the pending SQLite
queue, newest first. `limit` defaults to 100 and accepts 1 through 1,000.
Acknowledged delivery removes payloads from that queue, so this endpoint is
queue inspection rather than historical event access.

## Quarantined delivery repair

`list-quarantined-events` lists stored quarantined originals for operator repair. It returns metadata only. Event payloads, properties, visitor IDs, and raw engine errors are absent. Native allocation and transient storage failures retain pending work; they do not automatically quarantine it.

`GET /api/v1/projects/{id}/quarantined-events?limit=100&after_event_id=0`
returns `events`, `limit`, `after_event_id`, and an optional
`next_after_event_id`. Each event contains `event_id`, `uuid`, `error_code`,
`quarantined_at`, and `received_at`. Event IDs and cursors are decimal strings
to preserve 64-bit precision. Error codes are diagnostic metadata, not an authorization or retry policy.

Pages sort by increasing event ID. `limit` defaults to 100 and accepts 1 through
1,000. Pass `next_after_event_id` as the next request's `after_event_id`.
Pagination uses an event-ID cursor, without offsets. The listing reflects current
quarantine state rather than a snapshot. Restart from cursor `0` to include older
originals that entered quarantine after an earlier page.

After repairing the downstream constraint or deployment configuration,
`retry-quarantined-event` restores the original's delivery eligibility:

```text
POST /api/v1/projects/PROJECT_ID/quarantined-events/EVENT_ID/retry
```

The request has no body. A `202` response contains
`{"event_id":"EVENT_ID","requeued":true}`. Retrying an original already eligible
for delivery returns `requeued:false`. A missing or already acknowledged original
returns `404`, including an event ID belonging to a different project.

Retry preserves the original event ID, UUID, payload, revision, and receipt time.
It does not acknowledge analytical delivery, remove the accepted payload, or free
queue capacity. The normal delivery worker processes the event after retry. No
service restart or pause is needed. A remaining row-specific rejection can
quarantine the event again. Inspect project delivery status and quarantine
metadata after repair. Corrections and deletions retain their existing mutation
acknowledgement requirements.

Both operations are available through `porcupine_call` with the same operation
IDs, project checks, and permissions as HTTP. `porcupine_docs` exposes listing to
readers and retry to callers with `events:edit`.

The [analytics API reference](analytics.md) describes `get-event-schema`,
`get-event-catalog`, and `query-events`, including SQL examples, fixed quotas,
and eventual consistency. All three require one permitted project per request.
Use [set-catalog-description](event-discovery.md#document-an-event-or-property)
to edit optional event and property descriptions in the catalog.

`delete-events` accepts a SQL `where` predicate, optional `parameters` and
`time_range`, and a required `idempotency_key`. Send `dry_run: true` for the
matching count and a UUID sample. Confirmation requires `expected_count` and
returns `409` on count mismatch. The maximum is 1,000 matching events. A `202`
confirms durable acceptance. `get-event-deletion` returns the projection state
and counts. [Event mutation reference](../events.md)

Storage tiering is deployment-only and has no management endpoint.

## Historical imports

The [import guide](../imports.md) covers one-command native, PostHog, and Segment imports. The [HTTP protocol](posthog-import.md) covers manifest creation, binary chunk upload, status, resume, cancellation and import permissions. These operations use the API audience.

## Delivery pipeline management

Project routes under `/api/v1/projects/{id}/pipelines` configure webhooks and
scheduled S3 delivery. Reads require `pipelines:read` plus `events:read`; all
writes and retries require `pipelines:write` plus `events:read`. These are explicit
egress grants, separate from management permissions and event-reading authority.

The [pipeline reference](pipeline-api.md#permissions-and-api) lists all ten operation
IDs and routes. Lists contain bounded metadata; a detail read includes the
nonsecret rule. Destination endpoint URLs, credentials, event payloads, and
remote response bodies are never returned. The operator must configure the
encryption key before creation is available. Restore pauses destinations until
an authorized caller explicitly resumes them after review.

## Errors and limits

Error responses use `{"error":"message"}`. Domain and authentication errors
use `application/json`. Huma validation errors use `application/problem+json`
with the same envelope.

| Status | Meaning |
| --- | --- |
| `400` | Malformed JSON, invalid permission or project, or a domain validation failure |
| `401` | Missing, invalid, expired, revoked, or wrong-resource access token |
| `403` | Missing permission, project outside scope, or attempted privilege escalation |
| `404` | Revocation target or retained event does not exist, or the target is already revoked |
| `409` | Last administrator protection, stale mutation revision or deletion count, pending mutation, or idempotency conflict |
| `410` | Requested event has been deleted |
| `413` | Request body exceeds 1 MiB |
| `415` | Unsupported body encoding or content type |
| `422` | Request schema validation failed or the query deletion match limit was exceeded |
| `429` | Analytics concurrency or database resource limit exceeded |
| `500` | Storage or internal error |
| `503` | Authentication dependency, analytics storage, reader provisioning, or delivery-status storage unavailable; authentication failures include `Retry-After` and do not mark the credential invalid |

Management requests with a body use uncompressed JSON, except for binary PostHog import chunks. Browser CORS configuration
applies to ingestion endpoints only. `events:edit` and `events:delete` authorize
implemented mutation endpoints. `identity:write` remains a reserved permission.
The [access design](security-and-interfaces.md) describes analytics, SQL, and
MCP boundaries.

### Source-bound ingestion credentials

Ingestion credentials accept an optional `source_id` for [producer identity and retry deduplication](native-and-segment-ingestion.md). A bound writer needs `events:write` for one concrete project. The source is case sensitive, starts with a letter or digit, and accepts letters, digits, dots, underscores, colons, and hyphens up to 128 ASCII characters. The exact value `posthog` is reserved for legacy PostHog capture. The source appears in credential metadata. A source-bound parent issuing another writer delegates its own source.
