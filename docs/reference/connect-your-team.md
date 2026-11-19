# Set up team sign-in

Set `PORCUPINE_OAUTH_ISSUER` to the instance's public HTTPS origin. Configure [SMTP](email-transport.md) for invitations and email codes, [Google or OpenID Connect](mcp-interactive-oauth.md) for provider sign-in, or both. If you use OIDC without SMTP, share the instance URL and [connection instructions](../connect-tools.md) yourself.

## Connect the first administrator

To seed a [human operator account](email-accounts.md#deployment-administrator), set `PORCUPINE_ADMIN_EMAIL`. Keep the machine bootstrap credential for recovery. In the [standalone CLI](../../cli/README.md), set `PORCUPINE_URL` to the issuer, run `porcupine login --admin`, and approve the connection with the operator's account. Run `porcupine list-projects` to check it.

Add this to your Codex configuration and replace the example origin with your instance's origin.

```toml
[mcp_servers.porcupine_admin]
url = "https://analytics.example.com/mcp"
scopes = ["instance:admin"]

[mcp_servers.porcupine_admin.oauth]
client_id = "porcupine-local-admin"
callback_url = "http://127.0.0.1:43179/callback"
callback_port = 43179
```

Run `codex mcp login porcupine_admin --scopes instance:admin` and approve instance administration. This public PKCE client has no secret and requires an account with instance administration rights.

### Connect Codex with read access

Copy both tables above and rename `porcupine_admin` to `porcupine`. Set `client_id = "porcupine-local"` and `scopes = ["events:read", "management:read"]`, then run `codex mcp login porcupine`. The local client uses public PKCE without a secret. For other clients, [register the exact callback and allowed scopes](mcp-interactive-oauth.md#register-the-downstream-client). Multiple people can use one client registration.

<a id="choose-a-tenant-for-your-projects-unreleased"></a>

## Choose a tenant for your projects

[Invite the first tenant administrator](tenant-access.md#invite-the-first-tenant-administrator) to the `default` tenant or a new one. The administrator uses a dedicated [CLI or Codex tenant connection](tenant-access.md#connect-as-the-tenant-administrator) to manage that team's projects, members, and service accounts.

## Invite someone once

Use [tenant membership invitations](tenant-access.md#create-a-project-and-invite-a-reader) for team access or [`invite-human`](email-accounts.md#invite-a-person-with-the-operator-api) as an instance operator. For example, ask an administrator's agent:

> Invite lea@example.com to the demo project with events:read and management:read. Return her account ID.

The agent discovers `invite-human` through `porcupine_docs`. With SMTP, the invitation sends a notice. The person requests a fresh code when connecting each tool, or signs in with the configured provider. Give them the [connection guide](../connect-tools.md).

## Connect an agent owned by the team

[Create a project-scoped service account](service-keys.md) for unattended jobs and shared agents. Use its short-lived OAuth tokens in builtin mode or issue a static key for the API or MCP audience.
