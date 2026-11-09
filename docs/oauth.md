# OAuth client credentials

Porcupine provides machine-to-machine OAuth for the native API and MCP. A service exchanges its client ID and secret at `/oauth/token`, then sends the returned access token as Bearer authorization. Client secrets are token-exchange credentials, never API Bearer tokens or visitor identities.

The authorization server runs in the existing Go process and stores state in
SQLite. It requires no separate identity application, external authorization
server, or human account system. Interactive browser login requires a canonical HTTPS origin (lowercase scheme and host, default port omitted). Operator configuration pins the issuer and
resources; project grants remain Porcupine's own authority.

This page covers machine credentials. Porcupine v0.4.0 also supports [interactive MCP authorization](mcp-interactive-oauth.md) when an upstream OpenID Connect provider is configured. That optional flow uses pre-registered clients, an exact redirect URI, explicit user/project grants, and authorization codes with PKCE S256. The v0.3.0 release supports machine credentials only.

## Operator configuration

| Environment variable | Default | Meaning |
| --- | --- | --- |
| `PORCUPINE_OAUTH_ISSUER` | Empty | Fixed public origin; enables OAuth when set |
| `PORCUPINE_OAUTH_TOKEN_LIFETIME_SECONDS` | `900` | Access-token lifetime, 60 through 3,600 seconds |
| `PORCUPINE_OAUTH_ALLOW_LEGACY_KEYS` | `false` when enabled | Explicit migration allowance for old API and MCP Bearer keys |

The issuer is an origin with no path, credentials, query, or fragment. HTTPS is required except for HTTP on localhost or a loopback IP. A trailing slash normalizes away. Set the real external HTTPS origin behind a reverse proxy. Discovery URLs use this pinned issuer, never the request Host header.

[compose.oauth.yaml](../compose.oauth.yaml) adds these settings to the base deployment. Auth configuration is deployment-only; no HTTP or MCP operation changes it. Preserve SQLite, because it stores client grants, secret hashes, access-token hashes, and bootstrap alias metadata.

A configured issuer disables legacy API and MCP Bearer keys by default. Leaving OAuth entirely unconfigured retains legacy authentication for offline use and migration; it does not enable OAuth endpoints. Set `PORCUPINE_OAUTH_ALLOW_LEGACY_KEYS=true` only for an explicit migration period. Public PostHog ingestion keys remain separate and continue to work without OAuth for `mixed` projects. Projects with `server_only` ingestion always require OAuth.

## Bootstrap and registered clients

The original bootstrap administrator is available at the token endpoint as client ID `bootstrap`, using the original `PORCUPINE_BOOTSTRAP_TOKEN` as its exchange secret. This alias is pinned to that first administrator. It does not switch to a later administrator. Changing an environment value does not reset an existing database credential.

In OAuth mode, exchange the bootstrap secret before calling management APIs. Its raw secret is not accepted as API or MCP Bearer authorization unless legacy migration mode is explicitly enabled. Create separate clients for services and agents, then use their limited grants rather than distributing bootstrap access.

| Operation ID | Endpoint | Authority |
| --- | --- | --- |
| `create-oauth-client` | `POST /api/v1/oauth-clients` | `management:write`, with every requested permission and project held by the caller |
| `list-oauth-clients` | `GET /api/v1/oauth-clients` | `management:read`, restricted to complete project scope |
| `revoke-oauth-client` | `DELETE /api/v1/oauth-clients/{id}` | `instance:admin` |

Creation accepts this body through HTTP or `porcupine_call`:

```json
{
	"name": "Analytics agent",
	"permissions": ["events:read", "management:read"],
	"projects": ["PROJECT_ID"]
}
```

The `201` response contains `client_id`, one-time `client_secret`, `name`, `permissions`, `projects`, and `created_at`. Save the secret privately. SQLite stores a separate secret hash; the client secret does not become a legacy Bearer key or ingestion key. Names contain 1 through 200 bytes and cannot be blank. Project IDs must exist, with `*` reserved for instance scope. `instance:admin` requires `*` scope. Delegation cannot exceed the calling token's effective grants.

Listing returns `enabled` and `oauth_clients`. Items contain client ID, name, permissions, projects, creation time, and nullable revocation time. They contain no secret or hash. A client spanning projects A and B is omitted from a caller restricted to A. The bootstrap alias is not a registered client and does not appear in this list.

Revocation returns `204` and disables the principal and all its access tokens for subsequent authentication. The last active instance administrator remains protected with `409`. There is no secret recovery or dedicated rotation endpoint. Create a replacement client and revoke the old one to rotate access.

## Token exchange

`POST /oauth/token` accepts an uncompressed `application/x-www-form-urlencoded` body of at most 16 KiB. Authenticate with one HTTP Basic header. Body client secrets, assertions, duplicate form parameters, and query-string parameters are rejected.

| Field | Meaning |
| --- | --- |
| `grant_type` | Required, exactly `client_credentials` |
| `scope` | Optional space-separated permission names, restricted to client grants |
| `resource` | Optional exact resource URI; defaults to the issuer plus `/api/v1` |

The two resources are `ISSUER/api/v1` and `ISSUER/mcp`. API tokens cannot authenticate MCP requests, and MCP tokens cannot authenticate direct API or backend ingestion requests. An MCP call dispatching to a local API handler keeps the MCP audience. An omitted scope uses all currently allowed permissions. An explicitly empty scope is invalid. Request only the permissions a service needs.

Keep Basic credentials in a private file outside the repository, for example `$HOME/.config/porcupine/client.curl`, with mode `600`. For a registered client its content has this shape:

```text
user = "CLIENT_ID:CLIENT_SECRET"
```

For bootstrap, use `bootstrap` in place of `CLIENT_ID` and its original secret. Do not put the secret in a URL, request body, process argument, shell history, or committed configuration.

With `OAUTH_CLIENT_CONFIG` pointing to that file, request an API token and save
the response privately. For initial bootstrap administration, replace the example
read scope with `instance:admin` before creating projects or clients:

```sh
umask 077
TOKEN_RESPONSE_FILE="$HOME/.config/porcupine/api-token.json"
curl --fail-with-body --config "$OAUTH_CLIENT_CONFIG" "$PORCUPINE_URL/oauth/token" \
	-H 'Content-Type: application/x-www-form-urlencoded' \
	--data-urlencode 'grant_type=client_credentials' \
	--data-urlencode 'scope=events:read management:read' \
	--data-urlencode "resource=$PORCUPINE_URL/api/v1" > "$TOKEN_RESPONSE_FILE"
ACCESS_TOKEN="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["access_token"])' "$TOKEN_RESPONSE_FILE")"
```

`PORCUPINE_URL` must equal the configured issuer in this example. The response contains `access_token`, `token_type: "Bearer"`, `expires_in`, normalized `scope`, and `resource`. It is not cacheable. No refresh token is returned. Obtain another access token before expiry; applications should reuse a valid token rather than exchange credentials for every API call.

Call the API with the returned token:

```sh
curl --fail-with-body "$PORCUPINE_URL/api/v1/projects" \
	-H "Authorization: Bearer $ACCESS_TOKEN"
```

For MCP, exchange with `resource=$PORCUPINE_URL/mcp` and set the client's `Authorization: Bearer MCP_ACCESS_TOKEN` header. Keep API and MCP tokens separate.

## Token lifetime, permissions, and revocation

Tokens are opaque random secrets, not self-contained JWTs. SQLite stores hashes and the issued scope, project snapshot, resource, and expiry. Each request checks expiry, resource, current parent revocation, and current grants. Effective permissions and projects are the intersection of issued authority and current authority. Later grant expansion cannot silently expand an old token.

Revoking a client invalidates its tokens without waiting for their default 15-minute expiry. Ordinary already-authenticated calls are not automatically canceled. Long-running exports additionally poll creator authority and check it before publication. Previously issued S3 presigned links retain their separate lifetime.

Token issuance is limited to five authenticated valid exchanges per second with a burst of ten. Invalid anonymous traffic does not consume this issuance budget. There are at most 64 unexpired tokens per client and 10,000 instance-wide. Capacity and issuance limits return `429` with `Retry-After: 1`. Expired entries are removed during issuance. There is no public token-list or individual token-revocation endpoint.

## Browser and backend ingestion

For `mixed` projects, PostHog browser SDKs keep their public write-only single-project ingestion key in the documented payload field. They do not receive OAuth client secrets. With OAuth enabled and legacy keys disabled, new `/access-keys` credentials are restricted to `events:write` and exactly one project. Use OAuth clients for read, edit, delete, management, and agent access.

A backend can send an API-resource OAuth access token with `events:write` in Authorization. A token granting exactly one project needs no payload selector. A token spanning projects must also supply a valid single-project public ingestion key as the payload selector. That key must resolve to a project the token may write; an arbitrary payload project ID cannot choose scope. An invalid Authorization header cannot fall back to an otherwise valid payload key.

The authenticated service's client ID is never substituted for the event's `distinct_id`. Application visitor identity remains an event concern.

### Project ingestion policy

`POST /api/v1/projects` accepts `ingestion_mode` when creating a project:

| Mode | Capture authority | Creation response |
| --- | --- | --- |
| `mixed` | Public single-project key or authorized API-resource OAuth token | Returns a one-time `ingest_token` |
| `server_only` | API-resource OAuth access token with `events:write` for the selected project | Omits `ingest_token` and creates no public ingestion credential |

Omitting the field selects `mixed`. Existing projects migrate to `mixed`, preserving their ingestion keys. Each project object in creation and listing responses includes `ingestion_mode`. There is no API for changing an existing project's mode.

Create a project for confidential server collection with this body:

```json
{
	"name": "Backend events",
	"ingestion_mode": "server_only"
}
```

Creating a `server_only` project requires configured OAuth and returns `503` when OAuth is disabled. A persisted `server_only` policy continues to reject writes if OAuth is later disabled. Enabling legacy API keys does not relax the policy. Public payload keys, legacy Bearer keys, client secrets, expired tokens, and MCP-audience tokens cannot authenticate server-only captures.

The policy covers `/capture/`, `/batch/`, `/i/v0/e/`, and `/e/`. Batches retain atomic validation and project isolation. A payload `project_id` cannot select a project. Prefer a client scoped to one project so its API-resource token needs no payload selector. A token spanning projects still needs a separately issued valid project key as its selector. On a `server_only` project, that selector grants no write authority without the OAuth token.

`mixed` project events remain client-supplied data. Event names and UUIDs do not establish server authority. Use a separate `server_only` project when the collection boundary must exclude public writers.

## Discovery and errors

OAuth mode exposes these public endpoints:

- `GET /.well-known/oauth-authorization-server` provides RFC 8414 authorization-server metadata, the token endpoint, supported grant and authentication method, scopes, and resource-indicator support.
- `GET /.well-known/oauth-protected-resource/api/v1` describes the API resource.
- `GET /.well-known/oauth-protected-resource/mcp` describes the MCP resource.

Protected-resource metadata follows RFC 9728. API and MCP `401` challenges include the corresponding `resource_metadata` URL; an invalid token also adds `error="invalid_token"`. Resource metadata advertises Bearer headers only. The authorization server advertises the code flow and S256 challenge method when interactive login is enabled.

Token errors use OAuth JSON error names. Invalid client authentication returns `401 invalid_client`; unsupported grants return `400 unsupported_grant_type`; disallowed permissions return `400 invalid_scope`; an unrecognized resource returns `400 invalid_target`. Malformed requests return `400`, oversized token bodies return `413`, and storage failure returns `503 temporarily_unavailable`. API authorization still distinguishes `401` authentication from `403` insufficient scope or project access.

## Verification status

Local Go and race tests cover HTTP and MCP client-credentials flows, project
isolation, separate API/MCP audiences, backend ingestion, and immediate client
revocation. See the [interactive MCP setup and verification](mcp-interactive-oauth.md) for the separate browser flow.
