# Management API reference

This reference lists HTTP operations, required permissions, and response codes. For a task walkthrough, start with [projects](projects.md), [people](email-accounts.md), [queries](query-events.md), or [pipelines](pipelines.md).

The running server publishes its OpenAPI contract at `GET /openapi.json`. The CLI and MCP use the same operation IDs and authorization rules. The project analytics schema has [its own endpoint](analytics.md#event-schema).

## Authentication and scope

Send a Bearer credential issued for `ISSUER/api/v1`. A person can authorize the [CLI](../cli/README.md) with email sign-in. A machine can use an [API-audience service key](service-keys.md) or exchange its client credentials for a short-lived [OAuth access token](oauth.md).

On first startup, `PORCUPINE_BOOTSTRAP_TOKEN` creates an `instance:admin`
principal with `*` project scope. The OAuth client ID `bootstrap` exchanges its
original secret. Subsequent startups preserve state; changing the environment
does not reset credentials. The alias stays pinned to that first administrator.

Each principal has permissions and project IDs. `*` includes all projects.
`instance:admin` grants all operations and requires `*` scope. An access token's
issued scope and project snapshot intersect current parent grants on every
request. Operators can choose [email accounts](email-accounts.md) for interactive API and MCP access, or [upstream OIDC](mcp-interactive-oauth.md) for read-only MCP. Each deployment selects one interactive login method. Authentication
runs before request validation. HTTP and [MCP](mcp.md) share operation policies
and project checks, with separate API and MCP token resources. The CLI generates its commands from the same OpenAPI operations.

With an issuer configured, legacy API and MCP Bearer keys are rejected by
default. Explicit legacy-key allowance is a migration setting. Without any OAuth
configuration, the server retains legacy authentication for offline use. Public
PostHog ingestion keys are separate and remain valid for capture.

## Operations

`query-events` accepts an optional `use_cache` Boolean. Omitted or `true` allows
a 60-second ClickHouse result cache. `false` bypasses both cache directions and
reads currently projected data. See [the analytics reference](analytics.md#cached-and-fresh-queries)
for mutation invalidation and delivery consistency.

| Operation | Required authority | Success |
| --- | --- | --- |
| `GET /api/v1/status` | `management:read`; no project data or secrets in the [instance report](instance-status.md) | `200`, including a degraded report |
| `GET /api/v1/projects` | `management:read`; results restricted to permitted projects | `200` |
| `POST /api/v1/projects` | `management:write` and `events:write`, both with `*` scope | `201` |
| `PATCH /api/v1/projects/{id}` | `management:write` for this project | `200` with the updated project |
| `DELETE /api/v1/projects/{id}` | `instance:admin` | `204` after cleanup |
| `GET /api/v1/access-keys` | `management:read`; results restricted to keys whose entire project scope the caller holds | `200` |
| `POST /api/v1/access-keys` | `management:write`; every requested permission and project must be held by the caller | `201` |
| `DELETE /api/v1/access-keys/{id}` | `instance:admin` | `204` |
| `POST /api/v1/oauth-clients` | `management:write`; requested grants cannot exceed the caller | `201` |
| `GET /api/v1/oauth-clients` | `management:read`; complete project scope required for each returned client | `200` |
| `DELETE /api/v1/oauth-clients/{id}` | `instance:admin`; last administrator protected | `204` |
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
| `POST /api/v1/projects/{id}/query` | `events:read` and access to the project | `200` |
| `GET /api/v1/projects/{id}/events/{uuid}` | `events:read` and access to the project | `200` |
| `PATCH /api/v1/projects/{id}/events/{uuid}` | `events:edit` and access to the project | `202` |
| `DELETE /api/v1/projects/{id}/events/{uuid}` | `events:delete` and access to the project | `202` |
| `GET /api/v1/projects/{id}/event-mutations/{mutation_id}` | `events:read` and access to the project | `200` |

For managed SQL exports, see the [export job operations](exports.md). The [pipeline operations](pipelines.md#permissions-and-api) configure recurring deliveries. [Historical imports](posthog-import.md) use HTTP or the CLI for the entire import, including binary chunk uploads. The running server publishes its enabled operations at `/openapi.json`.

Project creation accepts `{"name":"Example"}` and returns a `mixed` project plus
a one-time public `ingest_token` with `events:write` for that project. With OAuth
enabled, send `{"name":"Trusted service","ingestion_mode":"server_only"}` to
require authenticated backend writes. The response omits the public key. The
creation operation requires the same permissions for both modes. See the
[ingestion policy](oauth.md#project-ingestion-policy).

`rename-project` accepts `{"name":"Research"}`. Names must contain 1–200 UTF-8 bytes after trimming whitespace. The response contains the updated `project`. Existing SDKs, dashboards and pipelines continue to address the project by ID.

`delete-project` accepts the project ID in the path and takes no request body. An instance administrator can use it to permanently remove a project's queued and analytical events, metadata, pipeline definitions and Porcupine-managed exports. Copies already delivered to customers' webhook or S3 destinations and existing backups are outside the deletion scope. Bucket lifecycle rules control retention of noncurrent object versions. See [export cleanup](exports.md#bucket-permissions-lifecycle-and-cleanup). If a storage dependency is unavailable and the request returns `503`, retry the same ID. The server also retries pending cleanup every minute and on restart. A repeat request for a completed deletion returns `204`. An unknown ID returns `404`.

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
Creating independent access keys or OAuth clients also requires an instance administrator or an eligible machine credential. A project-scoped human session or static service key with `management:write` can rename its project but cannot issue independent credentials. Permissions and projects must each contain at least one entry. Project IDs
must exist unless the scope is `*`.

OAuth client listing returns `{"enabled":true,"oauth_clients":[...]}` without
secrets or hashes. Items contain `client_id`, name, grants, creation time, and
nullable revocation time. The bootstrap alias is not listed as a registered
client. See [OAuth client management](oauth.md#bootstrap-and-registered-clients).

In OAuth mode with legacy keys disabled, access-key creation is restricted to
`permissions: ["events:write"]` and exactly one project. It returns the one-time
`token` used by public PostHog ingestion. Use an OAuth machine client or a [service key](service-keys.md) for service and agent access. Legacy mode retains the older scoped-key behavior.

Key listing returns `{"access_keys":[...]}`. Each item contains `id`, `name`,
`permissions`, `projects`, `created_at`, and `revoked_at`. Active keys have
`revoked_at: null`. Revoked keys remain visible within scope. A key spanning
projects A and B is omitted entirely from a caller restricted to A.

Client or principal revocation disables future authentication, including all
access tokens issued to it. Ordinary already-authenticated requests are not
automatically canceled. Exports also recheck creator authority while running. Revoking the last active instance administrator
returns `409`. The check and update share a SQLite write transaction.
Access tokens expire; public ingestion keys and client secrets have no expiry
or dedicated rotation endpoint. Rotate a client by creating a replacement and
revoking the old principal.

The event-read operation returns `{"events":[...]}` from the pending SQLite
queue, newest first. `limit` defaults to 100 and accepts 1 through 1,000.
Acknowledged delivery removes payloads from that queue, so this endpoint is
queue inspection rather than historical event access.

## Quarantined delivery repair

`list-quarantined-events` lists accepted originals retained after confirmed
row-specific ClickHouse rejection. It returns metadata only. Event payloads,
properties, visitor IDs, and raw ClickHouse errors are absent.

`GET /api/v1/projects/{id}/quarantined-events?limit=100&after_event_id=0`
returns `events`, `limit`, `after_event_id`, and an optional
`next_after_event_id`. Each event contains `event_id`, `uuid`, `error_code`,
`quarantined_at`, and `received_at`. Event IDs and cursors are decimal strings
to preserve 64-bit precision. Confirmed rejection codes are `321` and `469`.

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
It does not acknowledge ClickHouse delivery, remove the accepted payload, or free
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

The [event mutation reference](events.md) describes current-event reads, full
replacement corrections, deletion tombstones, revision checks, idempotency, and
mutation status. A `202` confirms durable acceptance, not analytical visibility.
Storage tiering is deployment-only and has no management endpoint.

## Historical PostHog imports

The [import reference](posthog-import.md) covers manifest creation, binary chunk upload, status, resume, cancellation and import permissions. These operations use the API audience.

## Delivery pipeline management

Project routes under `/api/v1/projects/{id}/pipelines` configure webhooks and
scheduled S3 delivery. Reads require `pipelines:read` plus `events:read`; all
writes and retries require `pipelines:write` plus `events:read`. These are explicit
egress grants, separate from management permissions and event-reading authority.

The [pipeline reference](pipelines.md#permissions-and-api) lists all ten operation
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
| `409` | Last administrator protection, stale revision, pending mutation, or idempotency conflict |
| `410` | Requested event has been deleted |
| `413` | Request body exceeds 1 MiB |
| `415` | Unsupported body encoding or content type |
| `422` | Request schema validation failed, including wrong types, required fields, or invalid limits |
| `429` | Analytics concurrency or ClickHouse resource limit exceeded |
| `500` | Storage or internal error |
| `503` | Analytics storage, reader provisioning, or delivery-status storage unavailable |

Management requests with a body use uncompressed JSON, except for binary PostHog import chunks. Browser CORS configuration
applies to ingestion endpoints only. `events:edit` and `events:delete` authorize
implemented mutation endpoints. `identity:write` remains a reserved permission.
The [access design](security-and-interfaces.md) describes analytics, SQL, and
MCP boundaries.

### Source-bound ingestion credentials

Ingestion credentials accept an optional `source_id` for [producer identity and retry deduplication](native-and-segment-ingestion.md). A bound writer needs `events:write` for one concrete project. The source is case sensitive, starts with a letter or digit, and accepts letters, digits, dots, underscores, colons, and hyphens up to 128 ASCII characters. The exact value `posthog` is reserved for legacy PostHog capture. The source appears in credential metadata. A source-bound parent issuing another writer delegates its own source.
