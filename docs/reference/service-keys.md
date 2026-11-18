# Static credentials for service accounts

Use a service account for a backend, scheduled job, or agent owned by the team. Its keys work independently of the human who configured it. Each key has one audience, project IDs, and permissions. In builtin mode, configure `PORCUPINE_OAUTH_ISSUER`. In [external mode](external-auth.md#native-machine-accounts-and-deferred-work), machine accounts use static API or MCP keys.

<a id="create-a-team-service-account-unreleased"></a>

## Create a team service account

Create a machine account with fixed project permissions as a tenant administrator. For builtin sign-in, see [Connect as the tenant administrator](tenant-access.md#connect-as-the-tenant-administrator).

```sh
porcupine create-tenant-service-account --tenant-id TENANT_ID --body '{"name":"Dashboard backend","permissions":["events:read"],"projects":["PROJECT_ID"]}'
```

In builtin mode, the response returns `client_id` and a one-time `client_secret`. Store them privately if the integration uses OAuth client credentials. The client exchanges them at `/oauth/token` for a short-lived access token scoped to the API or MCP resource. In external mode, the response omits the secret.

For an integration that accepts a static bearer key, create one under that account:

```sh
porcupine create-tenant-service-api-key --tenant-id TENANT_ID --id CLIENT_ID --body '{"name":"Dashboard production","audience":"api","permissions":["events:read"],"projects":["PROJECT_ID"]}'
```

Store the one-time `token` privately. Send it as `Authorization: Bearer <token>` to the API, or load it into `PORCUPINE_API_KEY` for the CLI. For a client that accepts MCP bearer headers, choose `"audience":"mcp"` and use that key only on `/mcp`.

Tenant service accounts and keys use explicit projects from their tenant. They cannot request deployment-wide `*`, `tenant:admin`, or `instance:admin`. Each key's permissions and projects must fit inside its parent account's current grants. Tenant administrators cannot provision an account in another tenant.

List accounts and their keys without exposing secrets:

```sh
porcupine list-tenant-service-accounts --tenant-id TENANT_ID --limit 20 --offset 0
porcupine list-tenant-service-api-keys --tenant-id TENANT_ID --id CLIENT_ID --limit 20 --offset 0
```

Use `update-tenant-service-account` to replace an account's name, permissions, and projects. For example:

```sh
porcupine update-tenant-service-account --tenant-id TENANT_ID --id CLIENT_ID --body '{"name":"Dashboard backend","permissions":["events:read","management:read"],"projects":["PROJECT_ID"]}'
```

An optional `source_id` on account creation binds an ingestion producer and requires `events:write` on one project. An update can omit it or repeat the same value; it cannot change it.

To rotate a key, issue a replacement, switch the integration, then revoke the old key:

```sh
porcupine revoke-tenant-service-api-key --tenant-id TENANT_ID --id CLIENT_ID --key-id KEY_ID
```

Use `revoke-tenant-service-account --tenant-id TENANT_ID --id CLIENT_ID` to revoke the account and all its keys. A repeated deletion of an already revoked account or key returns success while it still belongs to that tenant. Changing the parent account's grants revokes existing child keys; issue replacements with the new grants. Removing the human creator's membership does not revoke this independent team account.

The operation IDs are also the CLI commands. MCP uses `porcupine_docs` to inspect their contracts and `porcupine_call` to call them. HTTP clients use `/api/v1/tenants/{tenant_id}/service-accounts`, with `/{id}/api-keys` for keys. Account updates use `PUT`; revocations use `DELETE`. The running `/openapi.json` describes every field and permission check.

<a id="use-the-operator-api-in-v0140"></a>

## Use the operator API

The operator API provides `create-oauth-client`, `create-service-api-key`, `list-access-keys`, and `revoke-access-key`. An instance administrator creates a machine client through `POST /api/v1/oauth-clients`, then issues keys through `POST /api/v1/oauth-clients/{id}/api-keys`. Use the same key body as above. The corresponding CLI key command is `porcupine create-service-api-key --id CLIENT_ID --body @key.json`.

A key belongs to a machine OAuth client with the `client_credentials` grant. Parent revocation invalidates every key it owns. Changes to the parent's permissions, project set, or producer identity invalidate existing static keys. Granting new permissions to a service account never silently widens an existing token.

## Keep capture credentials separate

A private API key with `events:write` can authenticate a server-only capture using its bearer header. Public browser ingestion uses a separate single-project write credential. Keep service keys out of public SDKs and event payloads. Historical uploads require both `imports:write` and `events:write` on the target project.

Ordinary human analysis connections cannot mint independent machine credentials through `management:write` alone. Use a tenant-administrator connection for the team operations, or an instance administrator for the global operator API. Tenant administration itself uses a human OAuth grant; tenant service accounts receive project capabilities only.
