# Set up team sign-in

This guide is for the instance operator. If you have already been invited and want to connect an agent or the CLI, use [Connect your tools](../connect-tools.md).

A person can use one invited email address to connect their CLI, local agent and hosted MCP clients. Set `PORCUPINE_OAUTH_ISSUER` to the instance's public HTTPS origin. For email verification codes, enable [SMTP delivery](email-transport.md). The server sends a notice when an administrator grants access and issues a verification code only when the person starts a login. In an OIDC-only development setup without SMTP, share the instance URL and connection instructions yourself.

For unattended jobs or an agent shared by the team, use a [service account](service-keys.md). **Unreleased:** configured Google or OIDC sign-in can use the same invited human accounts as email verification. Both methods lead to the same tool consent and current permissions. See [sign-in configuration](mcp-interactive-oauth.md).

## Connect the first administrator

Optionally set `PORCUPINE_ADMIN_EMAIL` to the mailbox of the operator during deployment. This seeds a durable human account. Keep the separate machine bootstrap credential as a recovery path. The operator can use their mailbox to authorize an administrative tool on a new computer. A revoked account stays revoked after restart.

Deployments with human sign-in provide the public PKCE client `porcupine-local` for local read-only tools. They also provide the separate `porcupine-local-admin` client for instance administrators. Both accept the exact loopback callback `http://127.0.0.1:43179/callback`. They use no client secret. The client ID identifies the application; the user's OTP and consent grant the access.

To administer the instance from the [standalone CLI](../../cli/README.md), set `PORCUPINE_URL` to the issuer and run `porcupine login --admin`. Authenticate with your invited administrator address using the configured sign-in method, then approve the grant. Run `porcupine list-projects` to check the connection. Each tool gets its own grant.

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

Run `codex mcp login porcupine_admin --scopes instance:admin`. Authenticate with your authorized administrator address on the Porcupine page and approve instance administration. Email sign-in requests a fresh inbox code; a configured Google or OIDC provider authenticates the same invited identity. Keep this connection for administrative tasks. The ordinary local client cannot request the admin scope. A regular invited mailbox cannot acquire that scope even with the admin client ID.

## Choose a tenant for your projects {#choose-a-tenant-for-your-projects-unreleased}

Set up the team in this order:

1. Connect the instance operator with `porcupine login --admin`.
2. Keep the automatically created `default` tenant, or create one with `porcupine create-tenant`.
3. Invite the team's first administrator with `invite-tenant-member` and `role: "admin"`.
4. Have that person connect with `porcupine login --tenant-admin` and explicitly select the tenant.
5. Let the tenant administrator create projects, invite members, and provision team service accounts.

For example, the operator can give an administrator control of the default tenant before creating any project:

```sh
porcupine invite-tenant-member --tenant-id default --body '{"email":"lea@example.org","role":"admin"}'
```

The tenant administrator's local OAuth client is `porcupine-local-tenant-admin`, with the sole scope `tenant:admin`. It uses the same exact loopback callback as the other local clients. This role applies to one selected tenant, including future projects. It does not grant deployment administration.

A member can receive reader, contributor, or manager permissions on selected projects, or a default for all of the tenant's projects. Only the instance operator changes hard budgets or suspends ingestion. Follow [Give a team access to its tenant](tenant-access.md) for CLI bodies, MCP examples, and membership revocation.

## Invite someone once

For team access, use [tenant membership invitations](tenant-access.md#create-a-project-and-invite-a-reader). The `invite-human` workflow below is the operator-wide API.

Ask the connected admin agent to create a project if necessary, then invite the person's address with the required permissions for each project. For example:

> Invite lea@example.com to read the demo project and discover its events. Allow events:read and management:read. Return her account ID.

The agent discovers `invite-human` through `porcupine_docs`. An invitation sends a notice. Each tool connection starts with its own PKCE request and email verification. The code lasts up to five minutes, works only in the browser session that requested it, and allows five attempts. The human selects the projects in the consent form.

For everyday Codex use, copy the configuration above under `porcupine`, set `client_id = "porcupine-local"` and `scopes = ["events:read", "management:read"]`, then run `codex mcp login porcupine`. For other applications, an operator [registers each application](mcp-interactive-oauth.md#register-the-downstream-client) once per deployment with its exact callback and allowed permissions. Multiple people can use that registration. An OAuth-capable CLI targets the issuer's `/api/v1` resource. An MCP client targets `/mcp`.

Use `revoke-oauth-grant` to disconnect a tool; `revoke-human` disables all of a person's connections. See the [human account operations](email-accounts.md) for grant changes and invitation delivery.

## Connect an agent owned by the team

Use a named machine client with the project's permissions. It can exchange its credentials for a short-lived resource token, or an admin can issue it a [scoped service key](service-keys.md) for clients that accept a bearer header. The service account can continue working when the person who configured it leaves. Keep public browser ingestion keys separate from these private credentials.
