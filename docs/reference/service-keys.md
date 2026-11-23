# Static credentials for service accounts

Use a service account for a backend, scheduled job, or agent owned by the team. Its keys work independently of the human who configured it. A team service account is tied to one project. Each key can use one audience and a subset of the account's permissions for that project. In builtin mode, configure `PORCUPINE_OAUTH_ISSUER`. In [external mode](external-auth.md#native-machine-accounts-and-deferred-work), machine accounts use static API or MCP keys.

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

For both the account and its keys, send `projects` as an array containing one explicit project ID. The project must belong to the tenant and match the account when you issue a key. Choose project permissions such as `events:read`. The permissions `tenant:admin` and `instance:admin` are unavailable on team credentials.

List accounts and their keys without exposing secrets:

```sh
porcupine list-tenant-service-accounts --tenant-id TENANT_ID --limit 20 --offset 0
porcupine list-tenant-service-api-keys --tenant-id TENANT_ID --id CLIENT_ID --limit 20 --offset 0
```

Use `update-tenant-service-account` to change an account's name or permissions. Repeat the original project ID in the request. Create a separate account to give the integration access to another project. For example:

```sh
porcupine update-tenant-service-account --tenant-id TENANT_ID --id CLIENT_ID --body '{"name":"Dashboard backend","permissions":["events:read","management:read"],"projects":["PROJECT_ID"]}'
```

An optional `source_id` on account creation binds an ingestion producer and requires `events:write` on one project. An update can omit it or repeat the same value; it cannot change it.

An account can have several active keys for the same project. To rotate a key, issue a replacement, switch the integration, then revoke the old key:

```sh
porcupine revoke-tenant-service-api-key --tenant-id TENANT_ID --id CLIENT_ID --key-id KEY_ID
```

Use `revoke-tenant-service-account --tenant-id TENANT_ID --id CLIENT_ID` to revoke the account and all its keys. A repeated deletion of an already revoked account or key returns success while it still belongs to that tenant. Changing the parent account's permissions revokes its existing keys. Issue replacements with the new permissions. Removing the human creator's membership does not revoke this independent team account.

Older team accounts and keys with multiple projects can still be listed and revoked. Use a separate account and new credentials for each project the integration needs. A multi-project account cannot be updated or used to issue new keys, and its existing keys and OAuth tokens no longer authenticate. Multi-project keys and previously issued multi-project OAuth tokens also stop authenticating when their account has one project.

The operation IDs are also the CLI commands. MCP uses `porcupine_docs` to inspect their contracts and `porcupine_call` to call them. HTTP clients use `/api/v1/tenants/{tenant_id}/service-accounts`, with `/{id}/api-keys` for keys. Account updates use `PUT`; revocations use `DELETE`. The running `/openapi.json` describes every field and permission check.

<a id="use-the-operator-api-in-v0140"></a>

## Use the operator API

The operator API provides `create-oauth-client`, `create-service-api-key`, `list-access-keys`, and `revoke-access-key`. An instance administrator creates a machine client through `POST /api/v1/oauth-clients`, then issues keys through `POST /api/v1/oauth-clients/{id}/api-keys`. Use the same key body as above. The corresponding CLI key command is `porcupine create-service-api-key --id CLIENT_ID --body @key.json`.

A key belongs to a machine OAuth client with the `client_credentials` grant. Parent revocation invalidates every key it owns. Changes to the parent's permissions, project set, or producer identity invalidate existing static keys. Granting new permissions to a service account never silently widens an existing token.

## Keep capture credentials separate

A private API key with `events:write` can authenticate a server-only capture using its bearer header. Public browser ingestion uses a separate single-project write credential. Keep service keys out of public SDKs and event payloads. Historical uploads require both `imports:write` and `events:write` on the target project.

Ordinary human analysis connections cannot mint independent machine credentials through `management:write` alone. Use a tenant-administrator connection for the team operations, or an instance administrator for the global operator API. Tenant administration itself uses a human OAuth grant; tenant service accounts receive project capabilities only.
