# Give people and tools access

Humans sign in with their email identity and authorize each CLI or MCP connection. A backend or shared team agent uses a service credential. The server checks the same permissions through HTTP, CLI, and MCP.

An instance can replace builtin sign-in with an [external access-token issuer](reference/external-auth.md). That provider manages people and grants. Porcupine still enforces tenant/project boundaries and manages native machine keys; local email invitations and memberships are disabled in that mode. The steps below describe builtin sign-in.

## Connect a person

[Configure team sign-in](reference/connect-your-team.md) and [invite their email address](reference/email-accounts.md). The invitation grants access and sends a notice. It does not create a sign-in code. The person requests a verification code when connecting a tool, then selects the projects and permissions to authorize.

[Connect your tools](connect-tools.md) has the user-facing steps. Credentials and email codes belong in the client's authorization flow, not an agent prompt.

## Let a team manage itself

Porcupine has three levels of access:

| Level | Can manage |
| --- | --- |
| Instance operator | Tenants, hard usage budgets, instance-wide administration |
| Tenant administrator | That team's projects, members, and scoped service accounts |
| Member | The selected projects and permissions granted to them |

A fresh installation creates the tenant `default`. The operator can keep it for one team or create another tenant, then invite its first administrator:

```sh
porcupine login --admin
porcupine list-tenants
porcupine invite-tenant-member --tenant-id TENANT_ID --body '{"email":"lea@example.org","role":"admin"}'
```

The invited person connects with `porcupine login --tenant-admin`, selects the tenant, and approves access. They can then create projects and invite a reader:

```sh
porcupine create-project --body '{"name":"Website","tenant_id":"TENANT_ID"}'
porcupine invite-tenant-member --tenant-id TENANT_ID --body '{"email":"analyst@example.org","role":"member","project_scope":"selected","grants":[{"project_id":"PROJECT_ID","role":"reader"}]}'
```

The `reader`, `contributor`, and `manager` presets group project permissions. Explicit grants allow different rights on different projects. Tenant-local `project_scope: "all"` includes future projects; it never means instance-wide access. Read the [complete tenant access workflow](reference/tenant-access.md) and [permission reference](reference/security-and-interfaces.md).

## Connect a backend or shared agent

Create a [service account and scoped key](reference/service-keys.md). Use an API-audience key for HTTP and the CLI, or an MCP-audience key for `/mcp`. Store the secret in a secret manager or private environment variable.

A team-owned service account has its own lifecycle. Revoking its creator's membership does not revoke that account. Create one account per project and grant only the operations the integration needs.

## Remove access

Revoke a tool connection, key, or membership through the management API, CLI, or MCP. Membership changes invalidate affected human-tool grants while preserving unrelated tenant memberships and team credentials. Instance operators can also [unlink a sign-in identity](reference/identity-recovery.md) for recovery. Unlinking is not a permanent provider ban.

Authentication does not create access. An email code or Google/OIDC login proves identity; the person's current memberships and grants determine what they can do.
