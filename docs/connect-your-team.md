# Connect a team to Porcupine

Enable [email delivery](email-transport.md) on a build with email login. Set your canonical public HTTPS origin in `PORCUPINE_OAUTH_ISSUER`. The SMTP overlay sends invitations and verification codes. An operator who prefers to distribute codes directly can enable [manual invitations](local-invitations.md) without configuring SMTP. Manual and email invitations can also be enabled together.

## Connect the first administrator

Optionally set `PORCUPINE_ADMIN_EMAIL` to the mailbox of the operator during deployment. This seeds a durable human account. Keep the separate machine bootstrap credential as a recovery path. The operator can use their mailbox to authorize an administrative tool on a new computer. A revoked account stays revoked after restart.

Email-enabled deployments provide the public PKCE client `porcupine-local` for local read-only tools. They also provide the separate `porcupine-local-admin` client for instance administrators. Both accept the exact loopback callback `http://127.0.0.1:43179/callback`. They use no client secret. The client ID identifies the application; the user's OTP and consent grant the access.

For Codex, add the following to your Codex configuration, replacing the host with your own. The client and its callback are configured once. Use the issuer's `/mcp` URL exactly.

```toml
[mcp_servers.porcupine_admin]
url = "https://analytics.example.com/mcp"
scopes = ["instance:admin"]

[mcp_servers.porcupine_admin.oauth]
client_id = "porcupine-local-admin"
callback_url = "http://127.0.0.1:43179/callback"
callback_port = 43179
```

Run `codex mcp login porcupine_admin --scopes instance:admin`. Enter your authorized administrator email on the Porcupine page, enter the code delivered to its inbox, and approve instance administration. Keep this connection for administrative tasks. The ordinary local client cannot request the admin scope. A regular invited mailbox cannot acquire that scope even with the admin client ID.

## Invite someone once

Ask the connected admin agent to create a project if necessary, then invite the person's address with the required permissions for each project. For example:

> Invite lea@example.com to read the demo project and discover its events. Allow events:read and management:read. Return her account ID.

The agent discovers `invite-human` through `porcupine_docs`. An invitation sends a notice. Each tool connection starts with its own PKCE request and email verification. The code lasts up to five minutes, works only in the browser session that requested it, and allows five attempts. The human selects the projects in the consent form.

For everyday Codex use, copy the configuration above under `porcupine`, set `client_id = "porcupine-local"` and `scopes = ["events:read", "management:read"]`, then run `codex mcp login porcupine`. For other applications, an operator [registers each application](mcp-interactive-oauth.md#register-the-downstream-client) once per deployment with its exact callback and allowed permissions. Multiple people can use that registration. An OAuth-capable CLI targets the issuer's `/api/v1` resource. An MCP client targets `/mcp`.

Use `revoke-oauth-grant` to disconnect a tool; `revoke-human` disables all of a person's connections. See the [human account operations](email-accounts.md) for grant changes and invitation delivery.

## Connect an agent owned by the team

Use a named machine client with the project's permissions. It can exchange its credentials for a short-lived resource token, or an admin can issue it a [scoped service key](service-keys.md) for clients that accept a bearer header. The service account can continue working when the person who configured it leaves. Keep public browser ingestion keys separate from these private credentials.
