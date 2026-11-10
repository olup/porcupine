# MCP reference

Porcupine exposes two tools over stateless Streamable HTTP at `/mcp`. The
implementation uses the [official Go SDK](https://github.com/modelcontextprotocol/go-sdk)
v1.8.0 in the existing Go process. It requires no additional service.

## Connection and credentials

| Setting | Value |
| --- | --- |
| Local endpoint | `http://127.0.0.1:8080/mcp` |
| Transport | Streamable HTTP, stateless, JSON responses |
| Authentication header | `Authorization: Bearer MCP_ACCESS_TOKEN` for the MCP resource |
| Permissions | Same project and operation permissions as the native API |
| Supported client setup | Machine OAuth, audience-scoped service keys, or interactive OAuth for people |

For email OTP, Codex setup, and the optional deployment admin mailbox, see [Connect your team](connect-your-team.md). An operator configures each hosted client once. For upstream OIDC, see the [OIDC setup](mcp-interactive-oauth.md).

Agents and applications use separate registered [OAuth clients](oauth.md).
A reader client typically has `management:read` and `events:read` for selected
projects. Exchange its ID and secret at `/oauth/token` with
`resource=ISSUER/mcp`, then put the returned token value in the Authorization
header. Client secrets are not MCP Bearer credentials. An API-resource token
cannot authenticate MCP, and an MCP token cannot authenticate direct API calls.

Access tokens expire after 15 minutes by default. Machine clients repeat the client-credentials exchange. Interactive clients can use rotating refresh tokens within the consent grant lifetime. The server
revalidates expiry, resource, parent revocation, and effective grants on every
HTTP request. Ordinary in-flight calls are not canceled solely by that check.
Export jobs additionally recheck creator authority while running and before
publication. MCP session IDs grant no authority.

OAuth mode provides authorization-server and protected-resource metadata.
Unauthorized MCP responses point to
`ISSUER/.well-known/oauth-protected-resource/mcp`. URLs come from the pinned
operator issuer, not the request Host. [The OAuth reference](oauth.md) describes
the client-credentials exchange, Basic authentication, scopes, and challenges.

Machine clients can also use a [service key](service-keys.md) issued for MCP. Interactive clients support email OTP or an upstream OIDC provider. The MCP transport is streamable HTTP.

Configured OAuth rejects legacy MCP Bearer keys by default. Explicit legacy-key
allowance is a migration option. Entirely unconfigured OAuth retains the older
static-key mode for offline use; it exposes no OAuth endpoints. Public browser
ingestion credentials do not grant MCP access.

Cross-origin browser requests are rejected. Ingestion CORS configuration does
not apply to MCP. The SDK's localhost DNS rebinding protection remains enabled.
With a reverse proxy forwarding to a loopback listener, the upstream Host header
must also name that loopback host. External clients use the public HTTPS URL.

## Discovery with `porcupine_docs`

An empty argument object returns summaries of operations allowed by the current
token. It includes the token's effective `projects`, a sorted `operations` array, and
`next_offset`. It does not return operation schemas.

| Argument | Meaning |
| --- | --- |
| `query` | Optional case-insensitive word filter over operation IDs, summaries, and paths. Every word must match. Maximum 200 UTF-8 bytes. |
| `limit` | Catalog page size, default 10, maximum 20. Zero selects the default. |
| `offset` | Catalog offset, default 0. `next_offset: null` marks the last page. |
| `operation_id` | Full contract for one operation. Used alone, without filters or pagination. Maximum 200 UTF-8 bytes. |

Example search arguments:

```json
{"query":"SQL"}
```

Example detail arguments:

```json
{"operation_id":"query-events"}
```

The detail result contains one operation and its `contract`, an OpenAPI document
with only that operation and the component schemas it references. Contracts
come from the same Huma registrations as `/openapi.json`.

Discovery checks the token's effective permissions and instance-wide grants. A project
manager without the instance-wide grants needed to create projects does not
see that operation. The detailed event-read contract still describes a project
parameter; the actual project ID is checked when the call runs.

## Execution with `porcupine_call`

The call tool accepts an operation ID and the arguments described by its
contract. It does not accept a URL, method, credential, or custom headers.

```json
{
	"operation_id": "query-events",
	"path": {"id": "PROJECT_ID"},
	"body": {"sql": "SELECT event, count() AS total FROM events GROUP BY event ORDER BY total DESC LIMIT 20"}
}
```

| Argument | Meaning |
| --- | --- |
| `operation_id` | Required native API operation ID from discovery. |
| `path` | Optional string map of documented path parameters. IDs contain 1 through 128 ASCII letters, digits, underscores, or hyphens. |
| `query` | Optional string map of documented query parameters. |
| `body` | Optional JSON object for operations that accept a body. |

The result includes `status`, the native API HTTP status, and `body`, the JSON
response or `null` for an empty response. API failures set MCP `isError: true`
and retain the status and error envelope. Tool argument validation errors also
set `isError: true`, with an explanatory text result. Successful results include
structured content and a JSON text fallback.

Calls dispatch inside the process to the registered API route. Huma validates
the request and the API rechecks the credential and project access. The
adapter rejects unknown operations, unknown path or query parameters, and path
traversal. The adapter dispatches locally without an arbitrary URL or shell
command. Calling `query-events` executes validated SQL through the analytics
handler and its project-specific ClickHouse reader.

Mutating operations are available to suitably scoped access tokens. Project and key
creation return one-time secrets, just as their HTTP counterparts do. Event
corrections require `events:edit`, and deletions require `events:delete`. Their
`body` includes an idempotency key and expected revision, including for DELETE. Key
listing returns metadata only. There is no separate MCP permission system or
MCP-only permission vocabulary. Registered OAuth client creation, listing,
and revocation are also available through the same tools when permitted.

## Limits and annotations

MCP requests are limited to 1 MiB without compression. Captured API response
bodies are limited to 2 MiB. An oversized result returns a tool error with status
`413`, rather than truncated JSON. Lowering the event-read `limit` can reduce
the result. The structured result and text fallback increase the final wire
size beyond the captured API body size.

`porcupine_docs` is annotated as read-only and idempotent. `porcupine_call` is
annotated as potentially destructive and non-idempotent because it can create
or revoke credentials. Both have `openWorldHint: false`. These hints inform
clients; server-side token scopes and project grants enforce access independently.

The available operations include project and key management, queue inspection,
SQL schema discovery, observed event and property discovery, read-only
ClickHouse SQL, current-event reads, corrections, deletions, and mutation status.
Export creation, listing, polling, cancellation, and file downloads add five
operations to the same discovery catalog; the tool count remains two. [Project pipelines](pipelines.md) add scoped webhook and scheduled S3 delivery
operations through the same tools. Identity mutation remains unimplemented.

For analysis, request the contracts for `get-event-schema`, `get-event-catalog`,
and `query-events`. Schema discovery lists supported functions and quotas. The
event catalog returns observed names and sampled property types without raw
property values. Its latest-event UUID pointers can be inspected through a
parameterized SQL query. See [the analytics API examples](analytics.md).

The `query-events` body accepts the same optional `use_cache` Boolean as HTTP.
Omitting it or setting it to `true` allows a 60-second ClickHouse result cache.
Set it to `false` to bypass cache reads and writes and read currently projected
data. Fresh queries do not wait for delivery. Pending edits and deletions
automatically bypass caching; applied mutations use a new cache generation.
See [cached and fresh queries](analytics.md#cached-and-fresh-queries).

SQL is written by the agent. There are no named prebuilt metrics or saved SQL
catalogs. Event names and property content are untrusted data. HTTP and MCP
enforce the same `events:read` scope, SQL restrictions, and result limits. Read
`delivery.pending` and `delivery.pending_mutations` when interpreting results
because delivery is eventual. `get-event` reflects pending accepted corrections;
SQL and catalog results show projected state. Read the [mutation contract](events.md)
before calling `edit-event` or `delete-event`.

Native ClickHouse S3 tiering, its bucket endpoint and AWS credentials, and hot
duration have no MCP operations. They are operator deployment configuration.
Project delivery pipelines use separate scoped destination inputs. Administrator
access tokens still execute read-only SQL and cannot change storage through it.

## SQL result exports

[Exports](exports.md) use `events:read` for the selected project. Discover
`create-event-export`, `list-event-exports`, `get-event-export`,
`cancel-event-export`, and `list-event-export-files`, then use `porcupine_call`
with their generated contracts. Creation requires SQL, an idempotency key,
optional typed parameters, and optional `jsonl` or `csv` format. Cancellation
requires no body and is restricted to the creator credential or an administrator.

The tool returns job metadata or bounded file pages, never a large result body.
Files become available only after the entire query succeeds. Their presigned
URLs last at most five minutes and cannot be revoked through Porcupine once
issued. Bucket, prefix, endpoint, credentials, and budgets are operator-only
configuration. Export SQL remains read-only and uses the same project isolation
as interactive analytics. The delivery barrier is not a point-in-time snapshot.

The [export reference](exports.md#mcp-examples) includes create, poll, and cancel
arguments. Export-specific validation is in progress; this reference does not
claim that its real-storage integration suite has passed yet.

## Delivery pipelines

Use `porcupine_docs` to discover pipeline operations, then call them through
`porcupine_call`. Reads require `pipelines:read` and `events:read`; writes and
retries require `pipelines:write` and `events:read`, all for the selected project.
An event reader alone cannot configure egress. Destination credentials are
write-only inputs; stored credential values never appear in returned metadata.

The [pipeline reference](pipelines.md) describes creation, pause, resume,
credential replacement, run inspection, and retry. Pausing is not a recall of
requests already in flight. Supported restores pause destinations before any
resumption. These calls cannot change the operator's encryption key, custom S3
origin allowlist, native ClickHouse policy, or export resource budgets.

## Verification

`go test ./internal/porcupine -run TestMCP -count=1 -v` runs real Go SDK clients
over HTTP against temporary SQLite databases. The tests cover protocol versions
`2026-07-28` and `2025-11-25`, two-tool discovery, selected schemas, API execution,
project isolation, delegation, revocation, last-admin protection, invalid inputs,
cross-origin rejection, transport limits, and bounded responses.

`make integration` adds an isolated real ClickHouse test with real Go SDK MCP
clients for analytical calls, catalog discovery, schemas, and UUID pointers.
Ordinary `make check` does not start ClickHouse.

OAuth-specific deployment and final verification status are tracked in the
[OAuth reference](oauth.md#verification-status).
