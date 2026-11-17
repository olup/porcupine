# Manage projects

A project separates event data, identities, and delivery pipelines. Every analytics request targets one project ID. Credentials can hold access to several projects, and the server checks the target on every request.

**Unreleased:** every project belongs to a [tenant](tenants.md), which shares usage budgets across its projects. Fresh installations provide the tenant `default`. A tenant membership can grant selected projects or all current and future projects in that tenant. Project permissions still control each action; tenant ownership alone does not grant access. These changes are not in v0.14.0.

Use the [CLI](../cli/README.md), HTTP API, or a connected MCP agent. For v0.14.0, set `PORCUPINE_URL` and run `porcupine login --admin` with an instance administrator's email address. In tenant-enabled development builds, a tenant administrator uses `porcupine login --tenant-admin` and selects the team. See [tenant access](tenant-access.md).

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

Keep the returned project ID for future requests. In v0.14.0, creating a project requires `management:write` and `events:write` with instance-wide project scope. In the unreleased tenant implementation, an instance operator or that tenant's administrator can create a project. Include the team's `tenant_id` in the request body. Omitting it selects `default` and still requires authority for that tenant. The response includes the immutable tenant ownership. `list-projects` accepts a `tenant_id` filter and still restricts results to the caller's grants.

## Rename a project

Find the project ID with `list-projects`, then run:

```sh
porcupine rename-project --id PROJECT_ID --body '{"name":"Research"}'
```

Renaming requires `management:write` for the target project. It preserves the project ID, data, credentials, and pipelines.

## Give someone access

In tenant-enabled builds, [invite a tenant member](tenant-access.md#create-a-project-and-invite-a-reader) with selected-project grants or an all-project scope. A person can have different permissions on several projects. Tenant administrators manage their own team; an instance operator can manage every tenant. The [operator-wide invitation API](email-accounts.md) remains separate.

For a backend or shared team agent, issue a [service key](service-keys.md) scoped to its projects and required operations. Public ingestion keys cannot query events or administer the instance.

## Delete a project

Deletion permanently removes the project and its managed analytics and export objects. Check the project ID against `list-projects` before running this command. In v0.14.0, deletion requires an instance administrator. In tenant-enabled development builds, the owning tenant's administrator can also delete it; project-manager permissions alone do not grant whole-project deletion.

```sh
porcupine delete-project --id PROJECT_ID
```

The server cancels pending project work and cleans up its managed data. Interrupted cleanup is retried after restart and periodically. A completed deletion returns HTTP `204`, including on retry for the same ID. Backups and copies already sent to external destinations have separate retention policies. See [project deletion and recovery](reliability.md#whole-project-deletion).

In the unreleased tenant implementation, deleting a project preserves its tenant's monthly acceptance counters. Deletion does not refund an ingestion allowance.

## Use an agent or HTTP

Ask your MCP agent to list, create, rename, or delete the intended project. It discovers the relevant contract with `porcupine_docs` and executes it with `porcupine_call`. The operation IDs are `list-projects`, `create-project`, `rename-project`, and `delete-project`.

The HTTP routes are `GET` and `POST /api/v1/projects`, plus `PATCH` and `DELETE /api/v1/projects/{id}`. See the [HTTP reference](management-api.md#operations) for the full permissions and response contract.
