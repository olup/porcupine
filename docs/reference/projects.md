# Manage projects

A project separates event data, identities, and delivery pipelines. Every analytics request targets one project ID. Credentials can hold access to several projects, and the server checks the target on every request.

Every project belongs to a [tenant](tenants.md), which shares usage budgets across its projects. Fresh installations provide the tenant `default`. A tenant membership can grant selected projects or all current and future projects in that tenant. Project permissions still control each action; tenant ownership alone does not grant access.

Use the [CLI](../../cli/README.md), HTTP API, or a connected MCP agent. Set `PORCUPINE_URL` and run `porcupine login --admin` with an instance administrator's email address. A tenant administrator uses `porcupine login --tenant-admin` and selects the team. See [tenant access](tenant-access.md).

## List and create projects

List the projects you can access:

```sh
porcupine list-projects
```

Create a project for backend-only capture:

```sh
porcupine create-project --body '{"name":"Demo","ingestion_mode":"server_only"}'
```

For browser capture, use `"ingestion_mode":"mixed"`. The creation response includes the project's public `ingest_token`. Use it in your browser configuration. For private backend credentials, create a [service key](service-keys.md).

Keep the returned project ID for future requests. An instance operator or that tenant's administrator can create a project. Include the team's `tenant_id` in the request body. Omitting it selects `default` and still requires authority for that tenant. The response includes the immutable tenant ownership. `list-projects` accepts a `tenant_id` filter and still restricts results to the caller's grants.

## Get a browser ingestion key

Reading the active public key requires `management:read` for the project.

```sh
porcupine get-client-ingestion-key --id PROJECT_ID
```

If an older mixed project has no retrievable browser key, the owning tenant's administrator or an instance operator can run `porcupine ensure-client-ingestion-key --id PROJECT_ID`. This creates a browser key once and returns the same active key on retry. Previously issued keys continue working. A `server_only` project cannot provision a browser key.

Browser keys can send events to their own project. To revoke one, find the `client_ingestion` key for that project with `porcupine list-access-keys`, then run `porcupine revoke-access-key --id KEY_ID`. Run `ensure-client-ingestion-key` to issue a replacement and update the browser configuration.

## Rename a project

Find the project ID with `list-projects`, then run:

```sh
porcupine rename-project --id PROJECT_ID --body '{"name":"Research"}'
```

Renaming requires `management:write` for the target project. It preserves the project ID, data, credentials, and pipelines.

## Give someone access

In builtin sign-in mode, [invite a tenant member](tenant-access.md#create-a-project-and-invite-a-reader) with selected-project grants or an all-project scope. A person can have different permissions on several projects. Tenant administrators manage their own team; an instance operator can manage every tenant. The [operator-wide invitation API](email-accounts.md) remains separate.

For a backend or shared team agent, issue a [service key](service-keys.md) scoped to its projects and required operations. Public ingestion keys cannot query events or administer the instance.

## Delete a project

Deletion removes access to the project. Hosted physical reclamation waits for retention, the operator restore floor, and reconciliation of object ownership. [Hosted recovery requirements](../design/hosted-architecture.md#recovery-and-retention) Check the project ID against `list-projects` before running this command. An instance administrator or the owning tenant’s administrator can delete it. Project-manager permissions alone do not grant whole-project deletion.

```sh
porcupine delete-project --id PROJECT_ID
```

The server cancels pending project work and cleans up its managed data. Interrupted cleanup is retried after restart and periodically. A completed deletion returns HTTP `204`, including on retry for the same ID. Backups and copies already sent to external destinations have separate retention policies. See [project deletion and recovery](reliability.md#whole-project-deletion).

Deleting a project preserves its tenant's monthly acceptance counters. Deletion does not refund an ingestion allowance.

## Use an agent or HTTP

Ask your MCP agent to list, create, rename, or delete the intended project. It discovers the relevant contract with `porcupine_docs` and executes it with `porcupine_call`. The operation IDs are `list-projects`, `create-project`, `rename-project`, and `delete-project`.

The HTTP routes are `GET` and `POST /api/v1/projects`, plus `PATCH` and `DELETE /api/v1/projects/{id}`. See the [HTTP reference](management-api.md#operations) for the full permissions and response contract.
