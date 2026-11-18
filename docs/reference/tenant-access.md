# Give a team access to its tenant

A tenant groups a team's projects under shared usage budgets. These examples assume a fresh tenant-enabled deployment. Use this guide to give a team control of its projects without giving it control of the deployment. A tenant administrator can create projects, invite members, and manage the team's service accounts. The instance operator keeps infrastructure settings and hard usage budgets.

## Invite the first tenant administrator

Connect as an instance operator. With the standalone CLI, set `PORCUPINE_URL` to the instance's public origin and run:

```sh
porcupine login --admin
porcupine list-tenants
```

A fresh instance already has the tenant `default`. Keep it for one team, or create another tenant:

```sh
porcupine create-tenant --body '{"name":"Research team"}'
```

Keep the returned `tenant.id`. The examples below use `TENANT_ID` as a placeholder for that ID.

Invite the team's administrator:

```sh
porcupine invite-tenant-member --tenant-id TENANT_ID --body '{"email":"lea@example.org","role":"admin"}'
```

The response identifies the human and the new membership. Invitation delivery sends a notice, not a sign-in code. The person requests a code when connecting a tool, or uses the instance's configured Google or OIDC sign-in. In an OIDC-only setup without email transport, share the instance URL and connection instructions yourself. See [sign-in configuration](mcp-interactive-oauth.md).

## Connect as the tenant administrator

As the invited person, run:

```sh
porcupine login --tenant-admin
```

Authenticate with the invited address, select the tenant, and approve the connection. This works before the tenant has any projects. The connection includes future projects in the selected tenant. It does not grant `instance:admin` or access to other tenants.

For a local Codex MCP connection, use the dedicated public client and request its tenant-administration scope:

```toml
[mcp_servers.porcupine_team]
url = "https://analytics.example.com/mcp"
scopes = ["tenant:admin"]

[mcp_servers.porcupine_team.oauth]
client_id = "porcupine-local-tenant-admin"
callback_url = "http://127.0.0.1:43179/callback"
callback_port = 43179
```

Replace the host, then run `codex mcp login porcupine_team --scopes tenant:admin`. The public client has no client secret. The human's current membership and consent grant the access.

For other MCP clients, have the operator [register their exact callback](mcp-interactive-oauth.md#register-the-downstream-client). Tenant administrators do not manage deployment-wide OAuth application registrations.

## Inspect and rename the team

From the tenant-administrator connection, inspect the tenant and its current usage:

```sh
porcupine get-tenant --tenant-id TENANT_ID
porcupine get-tenant-usage --tenant-id TENANT_ID
porcupine update-tenant --tenant-id TENANT_ID --body '{"name":"Research team"}'
```

The name can change without changing the tenant ID. Hard budgets and the ingestion-suspension switch remain instance-operator settings. Ordinary project readers discover their projects with `list-projects`; they do not receive tenant-management or aggregate-usage access.

## Create a project and invite a reader

From the tenant-administrator connection, create the project:

```sh
porcupine create-project --body '{"name":"Website","tenant_id":"TENANT_ID"}'
```

Keep the returned project ID. For a mixed-ingestion project, store its one-time ingestion key privately until you configure the producer.

To replace a public ingestion key, create another single-project write key:

```sh
porcupine create-access-key --body '{"name":"Browser capture","permissions":["events:write"],"projects":["PROJECT_ID"]}'
porcupine list-access-keys
porcupine revoke-access-key --id KEY_ID
```

The secret is returned only at creation. A tenant administrator can list and revoke the tenant's owned credentials, including public ingestion keys. These credentials cannot grant instance administration or access another tenant. Use a [team service account](service-keys.md) for private analytical or backend access.

Invite a reader to that project:

```sh
porcupine invite-tenant-member --tenant-id TENANT_ID --body '{"email":"analyst@example.org","role":"member","project_scope":"selected","grants":[{"project_id":"PROJECT_ID","role":"reader"}]}'
```

A reader can discover events and run SQL, without ingesting, correcting, or deleting events. The person connects their own tool with ordinary `porcupine login`, or follows [Connect your tools](../connect-tools.md) for MCP. Their consent selects the project and requested scopes.

For different access on two projects, give each grant its own role or permission list:

```json
{
  "email": "analyst@example.org",
  "role": "member",
  "project_scope": "selected",
  "grants": [
    {"project_id": "PROJECT_A", "role": "reader"},
    {"project_id": "PROJECT_B", "permissions": ["events:read", "events:write", "management:read"]}
  ]
}
```

Ordinary `porcupine login` requests read access only. To use a contributor's or manager's write permissions, request the project scopes explicitly:

```sh
porcupine login --scopes 'events:read events:write management:read'
```

The browser shows the requested scopes and project choices. The request must fit the person's permissions on every selected project. `--scopes` accepts space-separated or comma-separated project permissions, and cannot be combined with `--admin` or `--tenant-admin`. A registered MCP client can request the same allowed project scopes.

Use `project_scope: "all"` when a member should also receive access to future projects in this tenant:

```json
{"email":"analyst@example.org","role":"member","project_scope":"all","project_role":"reader"}
```

This membership scope belongs to one tenant. It makes future projects available for the person to select at a later tool consent. Existing analyst connections keep their approved project selection; reconnect to include a new project. The dedicated tenant-administrator connection instead retains its administrative role over future projects in the selected tenant. Do not use `*`, which denotes deployment-wide access. Inspect the installed presets with `porcupine get-tenant-project-roles --tenant-id TENANT_ID`, or read the [permission reference](security-and-interfaces.md#tenant-roles-and-project-permissions-unreleased).

## Use the same operations through MCP or HTTP

Give your admin agent a request such as:

> In tenant TENANT_ID, create a Website project and invite analyst@example.org as a reader of that project. Show me the project and human IDs.

The agent first inspects each contract through `porcupine_docs`. It then calls `porcupine_call` with the operation ID. For example, these are MCP `tools/call` parameters:

```json
{"name":"porcupine_docs","arguments":{"operation_id":"invite-tenant-member"}}
```

```json
{"name":"porcupine_call","arguments":{"operation_id":"invite-tenant-member","path":{"tenant_id":"TENANT_ID"},"body":{"email":"analyst@example.org","role":"member","project_scope":"all","project_role":"reader"}}}
```

HTTP clients use `POST /api/v1/tenants/TENANT_ID/members` with the same JSON body and an API-audience access token. The CLI and MCP use the same operation policies. Inspect the running instance's `/openapi.json` for all request fields.

## Change or remove a membership

List the tenant's members:

```sh
porcupine list-tenant-members --tenant-id TENANT_ID --limit 20
```

Follow `next_cursor` with `--cursor` for another page. Keep the returned `human_id` when changing a membership.

Use `update-tenant-member` to replace the membership, omitting `email`:

```sh
porcupine update-tenant-member --tenant-id TENANT_ID --human-id HUMAN_ID --body '{"role":"member","project_scope":"selected","grants":[{"project_id":"PROJECT_ID","role":"reader"}]}'
```

Use `revoke-tenant-member` to remove only that tenant's access:

```sh
porcupine revoke-tenant-member --tenant-id TENANT_ID --human-id HUMAN_ID
```

Permission changes invalidate affected human-tool connections. The person can reconnect with their remaining rights. Other tenant memberships and independent team service accounts remain available. A tenant administrator cannot remove or demote the last active administrator. An instance operator can recover an inaccessible team.

Use [service accounts](service-keys.md) for a backend, scheduled job, or agent owned by the whole team. These accounts have their own project permissions and do not disappear when their creator leaves.
