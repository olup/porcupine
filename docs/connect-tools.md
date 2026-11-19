# Connect your tools

Use this guide when your team already runs Porcupine. Ask its administrator for the instance URL and an invitation to your email address. An invitation grants access; you request a verification code when you sign in to each tool.

If you are setting up the instance, start with [hosting](hosting.md) and [team sign-in](reference/connect-your-team.md).

If you use Porcupine Cloud, sign in with the address invited to your team. For a self-hosted instance with an [external identity provider](reference/external-auth.md), follow its invitation and authorization flow. The email steps below apply to builtin mode.

On instances with builtin sign-in, members use the configured email OTP or Google/OIDC sign-in. Tenant administrators use a separate `tenant:admin` connection and select their team. Follow [tenant access](reference/tenant-access.md) and [sign-in configuration](reference/mcp-interactive-oauth.md).

## Connect an agent through MCP

Give your agent the instance URL and the [agent guide](https://olup.github.io/porcupine/llms.txt). Use the instance's `/mcp` endpoint, for example `https://analytics.example.com/mcp`.

For an email-enabled instance, the connection follows these steps:

1. Start the client's OAuth connection to Porcupine.
2. Enter your invited email address on the Porcupine authorization page.
3. Request a verification code and enter the code from your inbox.
4. Select the projects and approve the permissions shown for this tool.

The administrator registers the application's callback URL once per instance. If your client asks for a client ID or secret, get its registration details from the administrator. Do not use your email verification code as a client secret. Local Codex setup and the dedicated administrator connection are described in [team sign-in](reference/connect-your-team.md).

Once connected, ask your agent:

> List the projects I can access. For the project I choose, discover the event names and properties, then count events by name.

Porcupine exposes four MCP tools:

- `porcupine_schema` discovers the project's SQL schema, observed events, and sampled properties.
- `porcupine_query` runs read-only SQL.
- `porcupine_docs` lists operations available to the connection and describes their inputs.
- `porcupine_call` runs a discovered operation, such as listing projects, exporting results, or managing access. Write operations require the corresponding permissions.

Follow [Query your events](query-events.md) for an example or see the [MCP reference](reference/mcp.md) for tool arguments and responses.

## Connect the CLI

Install the [standalone CLI](../cli/README.md#install) for your operating system. It connects to Porcupine Cloud by default:

```sh
porcupine login
porcupine list-projects
```

The CLI opens a browser to sign in by email or Google/OIDC and approve a connection. Ordinary login requests read access to events and project information. Copy an accessible project ID and run [your first query](query-events.md).

For a self-hosted instance, set `PORCUPINE_URL` to its API origin before running the same commands. If it uses an [external identity provider](reference/external-auth.md), pass `--client-id YOUR_EXTERNAL_PUBLIC_CLIENT` to `porcupine login`.

Use `porcupine login --tenant-admin` for team administration or `porcupine login --scopes 'events:read events:write management:read'` for permitted project writes. On a self-hosted instance with builtin sign-in, choose which tenant or projects to authorize in the browser. An instance operator uses `porcupine login --admin`. See [authentication options](reference/connect-your-team.md).

## Connect a backend or shared agent

Ask an administrator for a [service key](reference/service-keys.md) for the project and permissions that integration needs. Team service accounts belong to a tenant and work independently of the person who created them.

For HTTP or the CLI, use an API-audience key. Load it into `PORCUPINE_API_KEY` through your secret manager. For MCP clients that accept a bearer header, use an MCP-audience key on `/mcp`. A key for one audience cannot be used for the other.

## Change or remove access

A tenant administrator uses `revoke-tenant-member` to remove that team's access while preserving unrelated memberships and independent team service accounts. The instance operator can disconnect one tool with `revoke-oauth-grant`, or disable the human across the deployment with `revoke-human`. See [people](reference/email-accounts.md) and [service keys](reference/service-keys.md).
