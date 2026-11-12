# Manage projects

A project separates event data, identities, and delivery pipelines. Every analytics request targets one project ID. Credentials can hold access to several projects, and the server checks the target on every request.

Use the [CLI](../cli/README.md), HTTP API, or a connected MCP agent. For the examples below, set `PORCUPINE_URL` and run `porcupine login --admin` with an instance administrator's email address.

## List and create projects

List the projects you can access:

```sh
porcupine list-projects
```

Create a project for backend-only capture:

```sh
porcupine create-project --body '{"name":"Demo","ingestion_mode":"server_only"}'
```

For browser capture, use `"ingestion_mode":"mixed"`. The creation response includes the project's public `ingest_token`. Store it in your browser configuration and configure the application's allowed origin on the server. For private backend credentials, create a [service key](service-keys.md).

Keep the returned project ID for future requests. Creating a project requires `management:write` and `events:write` with instance-wide project scope. An instance administrator has that authority.

## Rename a project

Find the project ID with `list-projects`, then run:

```sh
porcupine rename-project --id PROJECT_ID --body '{"name":"Research"}'
```

Renaming requires `management:write` for the target project. It preserves the project ID, data, credentials, and pipelines.

## Give someone access

[Invite a person](email-accounts.md) with explicit permissions for each project. A person can have different permissions on several projects. An instance administrator can manage all projects.

For a backend or shared team agent, issue a [service key](service-keys.md) scoped to its projects and required operations. Public ingestion keys cannot query events or administer the instance.

## Delete a project

Deletion permanently removes the project and its managed analytics and export objects. Check the project ID against `list-projects` before running this command. Only an instance administrator can delete a project.

```sh
porcupine delete-project --id PROJECT_ID
```

The server cancels pending project work and cleans up its managed data. Interrupted cleanup is retried after restart and periodically. A completed deletion returns HTTP `204`, including on retry for the same ID. Backups and copies already sent to external destinations have separate retention policies. See [project deletion and recovery](reliability.md#whole-project-deletion).

## Use an agent or HTTP

Ask your MCP agent to list, create, rename, or delete the intended project. It discovers the relevant contract with `porcupine_docs` and executes it with `porcupine_call`. The operation IDs are `list-projects`, `create-project`, `rename-project`, and `delete-project`.

The HTTP routes are `GET` and `POST /api/v1/projects`, plus `PATCH` and `DELETE /api/v1/projects/{id}`. See the [HTTP reference](management-api.md#operations) for the full permissions and response contract.
