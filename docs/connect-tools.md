# Connect your tools

Use this guide when your team already runs Porcupine. Ask its administrator for the instance URL and an invitation to your email address. An invitation grants access; you request a verification code when you sign in to each tool.

If you are setting up the instance, start with [hosting](hosting.md) and [team sign-in](connect-your-team.md).

## Connect an agent through MCP

Give your agent the instance URL and the [agent guide](https://olup.github.io/porcupine/llms.txt). Use the instance's `/mcp` endpoint, for example `https://analytics.example.com/mcp`.

For an email-enabled instance, the connection follows these steps:

1. Start the client's OAuth connection to Porcupine.
2. Enter your invited email address on the Porcupine authorization page.
3. Request a verification code and enter the code from your inbox.
4. Select the projects and approve the permissions shown for this tool.

The administrator registers the application's callback URL once per instance. If your client asks for a client ID or secret, get its registration details from the administrator. Do not use your email verification code as a client secret. Local Codex setup and the dedicated administrator connection are described in [team sign-in](connect-your-team.md).

Once connected, ask your agent:

> List the projects I can access. For the project I choose, discover the event names and properties, then count events by name.

The agent uses `porcupine_docs` to discover operations and `porcupine_call` to execute them. [Query your events](query-events.md) explains the workflow. The [MCP reference](mcp.md) describes tool arguments and responses.

## Connect the CLI

Install the [standalone CLI](../cli/README.md#install) for your operating system. Replace the example origin with your instance's exact URL:

```sh
export PORCUPINE_URL=https://analytics.example.com
porcupine login
porcupine list-projects
```

Login opens the browser for email verification and consent. The ordinary login requests read access to events and project information. An instance administrator can use `porcupine login --admin` for administrative work.

You should see only projects allowed by your account and the connection you approved. Copy a project ID and continue with [your first query](query-events.md).

CLI browser login requires email sign-in on the instance. The optional upstream OIDC mode supports read-only MCP. See [authentication options](connect-your-team.md).

## Connect a backend or shared agent

Ask an administrator for a [service key](service-keys.md) with the projects and permissions that integration needs. Service accounts belong to the instance and work independently of the person who created them.

For HTTP or the CLI, use an API-audience key. Load it into `PORCUPINE_API_KEY` through your secret manager. For MCP clients that accept a bearer header, use an MCP-audience key on `/mcp`. A key for one audience cannot be used for the other.

## Change or remove access

An administrator can disconnect one tool with `revoke-oauth-grant`, or revoke a person's account with `revoke-human`. Revoking a person disconnects all of that person's tools. Service clients and their keys are managed separately. See [people](email-accounts.md) and [service keys](service-keys.md).
