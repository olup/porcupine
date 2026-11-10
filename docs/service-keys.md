# Static credentials for service accounts

An instance administrator can issue a static key for a backend or shared agent. It belongs to a machine OAuth client with the `client_credentials` grant. Each key has one audience, a name, project IDs and permissions. Service accounts continue to work independently of the human who configured them. Set `PORCUPINE_OAUTH_ISSUER` to enable service accounts on the deployment.

1. Create a machine client through `POST /api/v1/oauth-clients` or the MCP operation `create-oauth-client`. Set `grant_types` to `["client_credentials"]` (the default), and give it only the project and permissions it needs.
2. Use the `client_id` returned by the registration as the path parameter. Call `POST /api/v1/oauth-clients/{id}/api-keys` with an instance-admin API credential, or use `create-service-api-key` through an admin MCP connection. For a read-only dashboard use the following body:

   ```json
   {
     "name": "Dashboard backend",
     "audience": "api",
     "permissions": ["events:read"],
     "projects": ["YOUR_PROJECT_ID"]
   }
   ```

3. In the CLI, the corresponding command is `porcupine create-service-api-key --id CLIENT_ID --body @key.json`, where `key.json` contains the body above. Store the one-time `token` from the response privately. Send it as `Authorization: Bearer <token>` to the API. For an agent that accepts custom headers, choose `"audience": "mcp"` and use its key only on `/mcp`. A machine can instead exchange the owning client's ID and secret for short-lived OAuth tokens.
4. Inspect credentials with `list-access-keys`. The service key entry shows its ID, owner client ID, audience, projects and permissions. To replace a key, issue a new one, switch its caller, then revoke the old key through `DELETE /api/v1/access-keys/{id}`. Other keys on the account keep working.

Revoking an individual key removes its authority for both requests and queued work. Revoking the service client invalidates every key it owns. Changes to the client's permissions, project set or producer identity invalidate its existing static keys; issue replacements under the new grants. Granting a service new permissions never silently widens an existing token.

A private API key with `events:write` can authenticate a server-only capture using its bearer header. Public browser ingestion uses a separate single-project write credential. Keep service keys out of public SDKs and event payloads. Historical uploads accept an API key with both `imports:write` and `events:write`. Service keys operate alongside OAuth with legacy bearer mode disabled.

An administrator can provision independent machine credentials deliberately. Ordinary email-authenticated users and non-admin static service keys cannot mint independent credentials through management-write access. An existing machine OAuth client with management-write access can provision scoped OAuth clients consistent with its configured grants. Provisioning such a separate account grants it a lifecycle of its own; use the dedicated static key endpoint to attach keys to one account.
