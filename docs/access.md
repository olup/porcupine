# Give people and tools access

| Role | Access |
| --- | --- |
| Instance operator | Manages tenants, hard usage budgets, and the deployment |
| Tenant administrator | Manages their team's projects, members, and service accounts |
| Member | Uses the projects and permissions they receive |

HTTP, CLI, and MCP enforce the same permissions. With [external authentication](reference/external-auth.md), the issuer manages human access. Porcupine enforces tenant and project boundaries and issues native machine keys. Local invitations and memberships are disabled in that mode.

## Connect a person

[Configure builtin sign-in](reference/connect-your-team.md) and [invite the member](reference/tenant-access.md#create-a-project-and-invite-a-reader). Each person signs in and approves their own CLI or MCP connection. [Connection steps](connect-tools.md)

## Let a team manage itself

[Set up the first tenant administrator](reference/tenant-access.md#invite-the-first-tenant-administrator), then let that person [create projects and invite members](reference/tenant-access.md#create-a-project-and-invite-a-reader). Assign reader, contributor, or manager presets, or set explicit per-project permissions. [Roles and project scopes](reference/security-and-interfaces.md#tenant-roles-and-project-permissions)

## Connect a backend or shared agent

[Create a service account and scoped key](reference/service-keys.md) for each project the integration needs. Keep the secret in a secret manager or private environment variable. The account works independently of the person who created it.

## Remove access

[Revoke a membership](reference/tenant-access.md#change-or-remove-a-membership), [disconnect one tool or disable a person](reference/email-accounts.md#change-or-revoke-access), or [revoke a service key](reference/service-keys.md#create-a-team-service-account). Instance operators can also [unlink a sign-in identity for recovery](reference/identity-recovery.md).
