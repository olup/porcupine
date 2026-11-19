# Connect your tools

Get the instance URL and use the email address invited to your team. With builtin sign-in, use an email code or the configured Google or OpenID Connect provider. For a self-hosted instance with [external authentication](reference/external-auth.md#connect-tools), follow its provider's invitation and authorization flow.

If you run the instance, [set up team sign-in](reference/connect-your-team.md) first.

## Connect an agent through MCP

Give your agent the [agent guide](https://olup.github.io/porcupine/llms.txt) and your instance's MCP URL, such as `https://analytics.example.com/mcp`.

1. Start the client's OAuth connection to Porcupine.
2. Sign in with the invited address. For email sign-in, request a code and enter it in the same browser session within five minutes. You have five attempts.
3. With builtin sign-in, choose the projects to authorize. Approve the connection in the browser.

For a [registered client](reference/mcp-interactive-oauth.md#register-the-downstream-client), ask your administrator for its client ID and any client secret it requires. Enter credentials and sign-in codes in the client's authorization flow. For local Codex, see the [read-only configuration](reference/connect-your-team.md#connect-codex-with-read-access).

Once connected, ask your agent:

> List the projects I can access. For the project I choose, discover the event names and properties, then count events by name.

See [query examples](query-events.md) and the [four MCP tools](reference/mcp.md).

## Connect the CLI

Install the [standalone CLI](../cli/README.md#install) that matches your server. It connects to Porcupine Cloud by default.

```sh
porcupine login
porcupine list-projects
```

Complete sign-in in the browser, then use a project ID to [run a query](query-events.md). For a self-hosted instance, set `PORCUPINE_URL` to its API origin before login. For an external issuer, also pass `--client-id YOUR_EXTERNAL_PUBLIC_CLIENT` to `porcupine login`.

For team administration, run [`porcupine login --tenant-admin`](reference/tenant-access.md#connect-as-the-tenant-administrator). With builtin sign-in, select the tenant in the browser. An instance operator uses [`porcupine login --admin`](reference/connect-your-team.md#connect-the-first-administrator). To request project write permissions, run `porcupine login --scopes 'events:read events:write management:read'`.

## Connect a backend or shared agent

Ask for a [scoped service key](reference/service-keys.md). Use an API-audience key for HTTP or the CLI. For the CLI, set the destination with `PORCUPINE_URL` or `--url` and load the key into `PORCUPINE_API_KEY` from your secret manager. For an MCP client that accepts a bearer header, use an MCP-audience key on `/mcp`.

## Change or remove access

Use the [access guide](access.md#remove-access) to revoke a membership, tool connection, or key.
