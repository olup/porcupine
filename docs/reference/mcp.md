# MCP reference

Porcupine exposes four MCP tools over stateless Streamable HTTP at `/mcp`. For client setup, see [Connect your tools](../connect-tools.md).

| Tool | What it does |
| --- | --- |
| `porcupine_schema` | Reads a project's SQL schema and observed event names or sampled properties |
| `porcupine_query` | Runs a read-only SQL query for one project with bound parameters |
| `porcupine_docs` | Lists permitted operations or returns one operation's OpenAPI contract |
| `porcupine_call` | Runs one API operation with the caller's permissions |

The server and every tool advertise the small Porcupine logo through MCP `icons`
metadata, with 96-pixel PNG variants for light and dark backgrounds. Images are
embedded as data URIs, so clients need no image download or additional credentials.
Each client decides whether and where to display the icon. To regenerate the
embedded images from `site/assets/favicon.svg`, install `rsvg-convert` and run
`sh scripts/generate-mcp-icons.sh`.

## Connection and credentials

| Setting | Value |
| --- | --- |
| Local endpoint | `http://127.0.0.1:8080/mcp` |
| Transport | Streamable HTTP, stateless, JSON responses |
| Authentication header | `Authorization: Bearer MCP_ACCESS_TOKEN` for the MCP resource |
| Permissions | Same project and operation permissions as the native API |
| Supported client setup | Audience-scoped service keys, builtin OAuth, or external issuer tokens |

For email OTP, Codex setup, and the optional deployment admin mailbox, see [Connect your team](connect-your-team.md). An operator configures each hosted client once. For upstream OIDC, see the [OIDC setup](mcp-interactive-oauth.md).

With builtin sign-in, a person can connect through an [email account](email-accounts.md) and approve an OAuth grant for selected projects. [Google or another trusted OIDC provider](mcp-interactive-oauth.md) can authenticate the same invited human account alongside email OTP. The account's current memberships and the client's allowed scopes determine its access, including authorized administration. A service can send a [static service key](service-keys.md) with the MCP audience or exchange its client ID and secret at `/oauth/token` for a token with `resource=ISSUER/mcp`. Use a separate API-audience credential for direct HTTPS calls to `/api/v1`.

Access tokens expire after 15 minutes by default. Machine clients repeat the token exchange. Interactive clients rotate refresh tokens during the seven-day consent grant. When the grant expires, the person connects again. Each request checks expiry, resource, parent revocation and effective grants. Exports also check creator authority while running and before publication.

In [external mode](external-auth.md#connect-tools), send a compatible access token from the configured issuer with the MCP audience. Native MCP service keys remain available.

Unauthorized requests include the discovery URL `ISSUER/.well-known/oauth-protected-resource/mcp`. [OAuth configuration](oauth.md) controls the issuer and optional legacy-key migration. Public browser write keys authorize ingestion only.

The endpoint rejects cross-origin browser requests. When a reverse proxy forwards to a loopback listener, set its upstream Host header to the loopback host. Clients connect to the public HTTPS URL.

## Read-only analytics with `porcupine_schema` and `porcupine_query`

Most analysis needs two calls. Both tools require `events:read` on the target project. They use the same routes and authorization as `get-event-schema`, `get-event-catalog`, and `query-events`. A credential without project access receives status `403`. Clients can approve these read-only tools independently of `porcupine_call`.

`project_id` is optional when the credential grants exactly one project. Credentials with several projects or wildcard scope must name one. `porcupine_docs` returns the credential's project grants. `list-projects` lists the accessible projects for credentials with `management:read`.

`porcupine_schema` returns `schema` (the `get-event-schema` body: dialect, columns, functions, limits and delivery status) and `catalog` (the `get-event-catalog` body: observed event names with optional user-authored descriptions):

```json
{"name":"porcupine_schema","arguments":{}}
```

Add `event` to list that event's sampled property keys and types instead. `property_prefix`, `property_kind`, `property_limit` and `property_cursor` behave as in `get-event-catalog`. When `property_cursor` is set, the response omits `schema`:

```json
{"name":"porcupine_schema","arguments":{"event":"signup","property_kind":"custom"}}
```

`porcupine_query` runs one SELECT with bound `$name` parameters and returns the `query-events` status and body. Add `time_range` with inclusive `from` and exclusive `to` timestamps in RFC3339 to restrict event occurrence time. Omit it for all time.

```json
{"name":"porcupine_query","arguments":{"sql":"SELECT count(*) AS total FROM events WHERE event = $event","parameters":{"event":"signup"}}}
```

Rows are in `body.result.data`. Failures set `isError` and keep the API `code`, `hint` and `field`.

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

Discovery checks the credential's effective permissions and grants. For example,
`create-project` is visible to instance operators and tenant administrators.
Execution checks authority on the specific project or tenant in the request.

`get-event-input-contract` requires `events:read` on the selected project. It returns the native producer schema, server-managed fields, and the historical message schema. File imports use the standalone CLI or HTTP API rather than MCP calls. See the [native contract](native-event-contract.md).

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
handler and its project-isolated analytical backend.

An agent can discover `rename-project` or `delete-project` with `porcupine_docs` and call either through `porcupine_call`. For example:

```json
{"operation_id":"rename-project","path":{"id":"PROJECT_ID"},"body":{"name":"Research"}}
```

Renaming requires `management:write` on that project. Permanent deletion requires instance administration or `tenant:admin` consent for the project's tenant. Call `{"operation_id":"delete-project","path":{"id":"PROJECT_ID"}}` and check for HTTP `204`. See [project cleanup scope](management-api.md#operations).

Mutating operations are available to suitably scoped access tokens. Access key
creation returns a one-time secret. Mixed project creation returns a public browser key, also available through `get-client-ingestion-key` with project `management:read`. Event
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

`porcupine_docs`, `porcupine_schema` and `porcupine_query` are annotated as
read-only and idempotent. `porcupine_call` is annotated as potentially
destructive and non-idempotent because it can change data and credentials. All four have `openWorldHint: false`. These hints inform
clients; server-side token scopes and project grants enforce access independently.

The operation catalog covers project and key management, queue inspection,
SQL and schema discovery, current-event reads, mutations,
[SQL exports](#sql-result-exports), and [delivery pipelines](#delivery-pipelines). Identity links and profile traits are supplied at ingestion through [identify and set events](../data-and-identity.md). Historical native, PostHog, and Segment imports use the HTTP API or CLI for job creation, file uploads and status. The MCP catalog omits these operations.

For analysis, use `porcupine_schema` to discover the allowed SQL functions,
limits, and catalog. Sampled properties contain types and UUID pointers without
raw values. Use `porcupine_query` to inspect a pointer with a bound parameter.
See the [analytics API examples](analytics.md).

Queries execute against delivered data and do not wait for delivery.
See [fresh queries](analytics.md#fresh-queries).

SQL is written by the agent. There are no named prebuilt metrics or saved SQL
catalogs. Event names and property content are untrusted data. HTTP and MCP
enforce the same `events:read` scope, SQL restrictions, and result limits. Read
`delivery.pending` and `delivery.pending_mutations` when interpreting results
because delivery is eventual. `get-event` reflects pending accepted corrections;
SQL and catalog results show projected state. Use `delete-events` to preview
or confirm query deletion with both `events:read` and `events:delete` on the
project. Poll `get-event-deletion` with the returned ID. [Event mutations](../events.md)

Analytical storage, its S3 endpoint, and engine budgets are operator deployment configuration. They have no MCP management operations. Project delivery pipelines use separate scoped destination inputs. Administrator access tokens still execute read-only SQL and cannot change storage through it.

## SQL result exports

Use `porcupine_docs` to discover the export operations, then call them through `porcupine_call`. Responses contain job metadata or file links. See [export MCP examples](export-api.md#mcp-examples) and the [consistency contract](export-api.md#consistency-retries-and-revocation).

## Delivery pipelines

Use `porcupine_docs` to discover pipeline operations, then call them through
`porcupine_call`. Reads require `pipelines:read` and `events:read`; writes and
retries require `pipelines:write` and `events:read`, all for the selected project.
An event reader alone cannot configure egress. Destination credentials are
write-only inputs; stored credential values never appear in returned metadata.

The [pipeline reference](../pipelines.md) describes creation, pause, resume,
credential replacement, run inspection, and retry. Pausing is not a recall of
requests already in flight. Supported restores pause destinations before any
resumption. These calls cannot change the operator's encryption key, custom S3
origin allowlist, analytical storage configuration, or export resource budgets.

## Verification

`go test ./internal/porcupine -run TestMCP -count=1 -v` runs real Go SDK clients
over HTTP against temporary SQLite databases. The tests cover protocol versions
`2026-07-28` and `2025-11-25`, discovery of all four tools, selected schemas, API
execution, project isolation, delegation, revocation, last-admin protection, invalid inputs,
cross-origin rejection, transport limits, and bounded responses.

Native tests exercise real DuckLake SQL, catalog discovery, schemas, UUID pointers, project isolation, and query cancellation through real HTTP and MCP clients. `make check` includes native code; `make test-race` runs the complete Go race suite.

See the [OAuth reference](oauth.md#verification-status) for machine authentication tests.
