# Tenants and usage budgets

A tenant groups projects that share an ingestion allowance and interactive query limits. On a fresh installation, Porcupine creates the tenant `default`, named `Default`. You can keep every project there for a single-team deployment. No additional database or service is required.

## Ownership and access

For a fresh deployment, follow [team setup](tenant-access.md): connect the instance operator, choose or create a tenant, and invite its first administrator. That administrator can create the team's projects and invite its members.

Each project belongs to one tenant. Its `tenant_id` is returned with project metadata and cannot be changed after creation. Producers send events to a project; Porcupine resolves its tenant from stored project ownership. A field supplied in an event cannot select a different tenant.

A human can belong to several tenants with different roles. A tenant administrator manages that tenant's projects, memberships, and team service accounts. A member receives project permissions through an `all` or `selected` project scope. `all` includes future projects in that tenant; `selected` includes only the listed projects. Role presets provide reader, contributor, or manager permissions, and explicit permission lists allow different access on each project. [Tenant access](tenant-access.md)

Only an instance operator with `instance:admin` can create tenants, change their budgets, or suspend ingestion. Tenant administrators can inspect their own tenant's configuration and usage. Project readers can inspect their effective query limits through the project schema, but cannot change budgets or read aggregate tenant consumption. An operator-issued `*` project grant covers the whole deployment; tenant-wide access never uses that wildcard. [Permissions](security-and-interfaces.md#tenant-roles-and-project-permissions)

## Create a tenant and its projects

The HTTP API, generated CLI, and MCP use these operation IDs and the same permission checks:

| Operation ID | HTTP route | Purpose |
| --- | --- | --- |
| `list-tenants` | `GET /api/v1/tenants` | List configuration and budgets |
| `create-tenant` | `POST /api/v1/tenants` | Create a tenant |
| `get-tenant` | `GET /api/v1/tenants/{tenant_id}` | Read its configuration |
| `update-tenant` | `PATCH /api/v1/tenants/{tenant_id}` | Rename it or allow/suspend new ingestion |
| `update-tenant-budgets` | `PUT /api/v1/tenants/{tenant_id}/budgets` | Replace its complete budget configuration |
| `get-tenant-usage` | `GET /api/v1/tenants/{tenant_id}/usage` | Read one month's consumption |
| `delete-tenant` | `DELETE /api/v1/tenants/{tenant_id}` | Block access and start operator-only resumable deletion |
| `get-tenant-deletion` | `GET /api/v1/tenants/{tenant_id}/deletion` | Read operator-only cleanup status after tenant removal |

Creating a tenant, replacing budgets, and changing the ingestion switch require `instance:admin`. Tenant administrators can rename their own tenant and read its configuration and usage. `list-tenants` returns only tenants the caller can administer, unless the caller is an instance operator. Ordinary members use `list-projects` to discover their allowed projects. The running operation contracts describe the caller's available actions. `list-tenants` accepts `limit` (default 100, maximum 200). Pass a returned `next_cursor` as `cursor` for the next page. Tenant deletion is described below. There is no manual budget-reset operation.

After connecting the matching CLI with an instance administrator's credential, inspect the default tenant and its current usage:

```sh
porcupine list-tenants
porcupine get-tenant --tenant-id default
porcupine get-tenant-usage --tenant-id default
```

These commands are generated from the same API operations.

For example, create a tenant with this body for `create-tenant`:

```json
{"name":"Research team"}
```

Omitting `budgets` gives unlimited monthly ingestion and the deployment's default interactive limits. Keep the returned `tenant.id`. Then call `create-project` with:

```json
{"name":"Website","tenant_id":"TENANT_ID","ingestion_mode":"server_only"}
```

Omit `tenant_id` to use `default`. `list-projects` accepts an optional `tenant_id` filter, while still returning only projects the caller may access. See [project management](projects.md).

With MCP, first request an operation's contract using `porcupine_docs`. Call it through `porcupine_call` with its `operation_id`, `path`, and `body`. For example:

```json
{
  "operation_id": "update-tenant-budgets",
  "path": {"tenant_id": "TENANT_ID"},
  "body": {
    "ingestion_monthly_events": 100000,
    "ingestion_monthly_exceptions": 5000,
    "hot_query_max_concurrent": 1,
    "hot_query_timeout_ms": 3000,
    "hot_query_max_rows": 500,
    "hot_query_max_result_bytes": 524288
  }
}
```

These are technical limits, not a subscription or price. Replacing budgets does not change recorded consumption.

## Provision a tenant from a private service

Configure the [OAuth issuer](oauth.md#operator-configuration) or [external authentication](external-auth.md). As the instance operator, create a machine account with `create-oauth-client` using `permissions: ["instance:admin"]` and `projects: ["*"]`. Use the same grant and `audience: "api"` with `create-service-api-key`. Keep the one-time token in your service's secret store and send it as an API bearer credential. For rotation, deploy the replacement before calling `revoke-access-key`. [Service key setup](service-keys.md)

Use the [tenant and project operations](#create-a-tenant-and-its-projects) and store the returned `tenant.id` in your customer record. Give producers [ingestion credentials](projects.md) or a scoped service key when they run privately. Give dashboards a separate project-scoped reader. Read per-tenant consumption or list [operator usage](usage.md). Keep billing and customer records in your application.

Use the running server's `/openapi.json` for request and response schemas. Test provisioning, tenant isolation, ingestion, and query visibility before routing production traffic.

## Monthly ingestion allowance

`ingestion_monthly_events` counts new unique canonical rows durably accepted across all projects in the tenant. A supported identity control or accepted historical person snapshot also consumes a row. Duplicate retries, ignored snapshots, invalid rows, and rolled-back work consume nothing. A later edit, event deletion, or project deletion does not refund usage.

`ingestion_monthly_exceptions` sets an additional allowance for canonical rows named `$exception`. Each accepted exception uses one place in the shared event allowance and one place in the exception allowance. Omit this setting or pass `null` to remove the additional limit. A value of `0` prevents new exception acceptance while other events can still use the shared allowance.

Use `null` for unlimited ingestion and `0` to disallow new events. Zero still permits retries of events already accepted. Porcupine records usage in the same SQLite transaction as durable acceptance; it does not wait for analytical delivery. A quota refusal does not leave a partially charged acceptance transaction. In default standalone mode, this transaction also enforces the tenant's complete allowance exactly.

In [hosted mode](shared-deployment.md#shared-budgets), workers admit live events against cached policy and published usage. Counts can lag while a worker has buffered events or unpublished durable effects, so concurrent workers can exceed an allowance. Authentication revocation uses current authority.

Periods are UTC calendar months, from midnight on the first day until midnight on the next month's first day. Historical imports consume the allowance of the month when they are accepted, even if their occurrence dates are years earlier. A new month opens a new counter; previous counters remain available. There is no reset command. An operator can raise the allowance when more ingestion is needed.

Call `get-tenant-usage` without `period` for the current month, or pass `period=2026-11` for a selected month. The response includes:

```json
{
  "tenant_id": "TENANT_ID",
  "period_start": "2026-11-01T00:00:00Z",
  "period_end": "2026-12-01T00:00:00Z",
  "accepted_events": "75000",
  "accepted_exceptions": "2500",
  "remaining_events": "25000",
  "remaining_exceptions": "2500",
  "ingestion_allowed": true,
  "exception_ingestion_allowed": true
}
```

Counts are decimal strings. `remaining_events` is `null` for an unlimited allowance. Reducing a budget below existing consumption leaves zero remaining; it does not rewrite the counter. The usage response's `ingestion_allowed` combines the operator switch with the selected period's remaining allowance, using the current budget configuration. It is not a historical record of past budget settings.

`accepted_exceptions` counts exceptions recorded since the upgrade that enabled exception metering. The first UTC month can be partial, and earlier months show zero metered exceptions. The shared event totals still include all rows previously counted.

`remaining_exceptions` is the balance under the exception budget alone. It is `null` with no exception budget. `exception_ingestion_allowed` is true when ingestion is enabled and both the shared and exception budgets allow new acceptance.

Hosted aggregate usage reflects published durable counters. It can lag memory admission and the DuckLake commit. The report is not a hard cross-worker quota or spending cap.

## Suspend or resume ingestion

Send `{"ingestion_allowed":false}` to `update-tenant` to stop accepting new events. Send `true` to lift that suspension. An exhausted monthly budget still prevents new acceptance until the allowance increases or the next UTC month begins.

Suspension does not stop delivery of already accepted events, duplicate retries, queries, exports, or authorized deletions. Those operations retain their own permissions and limits.

When a tenant is suspended or its ingestion allowance is exhausted:

- Native and Segment ingestion return `429` without event receipts for rejected work.
- PostHog compatibility ingestion returns an intentional terminal `200` with `quota_limited`. This does not mean the rejected events were stored. Queue and storage failures remain errors.
- Historical imports retain their checkpoint. Resume the same import after raising the allowance or after the next month starts; accepted rows are not charged again.

Exception quota refusals use `exception_ingestion_budget_exceeded`. An exception-only PostHog request returns `200` with `quota_limited: ["exceptionEvent"]`, the batch key used by the supported browser SDK's `captureException` method. A mixed live batch that exceeds the exception allowance receives `429` and commits no new events. Resend the analytics events separately with the original IDs. An import chunk that reaches the exception limit keeps its existing checkpoint and can resume after the limit increases or a new month begins.

## Delete a tenant

Only an instance operator with `instance:admin` can start deletion or read deletion status. Tenant administration alone cannot delete a tenant.

```sh
porcupine delete-tenant --tenant-id TENANT_ID
porcupine get-tenant-deletion --tenant-id TENANT_ID
```

`delete-tenant` returns HTTP `202` after a durable transaction blocks ingestion and project access, revokes credentials owned by the tenant, and records each project for cleanup. Repeat the same ID after a timeout. Status survives removal of the tenant row. An ID that never existed returns `404`. There is no cancellation or restoration operation.

New resource admission stops when the fence commits. Cleanup cancels and drains already admitted project work before removing its managed state.

Standalone cleanup uses the existing project deletion paths for analytical data, Porcupine-managed exports, unpublished pipeline artifacts, imports, jobs, and local state. It removes tenant memberships and owned machine credentials while preserving global humans and their access to other tenants. A tool connection that spans A and B loses A and keeps B. Global identity records may retain inert opaque old project IDs; public grants, consent, and resource access exclude deleted projects.

In both profiles, tenant deletion removes operator-issued credentials owned by that tenant, including keys, OAuth clients, dependent service keys, and tokens. Instance-wide credentials survive. A credential spanning tenants keeps its surviving projects. If a legacy credential's scope was already empty before upgrading to v0.15.1, Porcupine cannot infer its owner. Review and revoke any such credentials you can attribute to the tenant.

`state=deleting` means cleanup remains pending. Recovery retries it after restart and periodically. Hosted cleanup also waits for recovery retention and reconciliation of scheduled output and managed objects.

`state=logically_deleted` means managed current data and active tenant control records were removed. `physical_purge=pending_verification` explicitly means that noncurrent S3 versions, retained copies, or backup history have not been proven absent. This implementation never reports `purged`. Tenant deletion does not override point-in-time recovery retention or erase published objects in a customer's pipeline destination.

An internal ownership inventory retains opaque managed object and multipart-upload keys plus a hashed storage namespace before their cleanup checkpoints disappear. Published customer pipeline outputs are excluded. The inventory contains no payload, access credentials, or identifying destination labels and remains until physical removal can be verified. Export cleanup requires the storage fingerprint recorded before the first upload. A changed destination or an upgraded checkpoint with no fingerprint stays pending rather than deleting from the wrong namespace. Objects whose ownership was already discarded by an older build remain unverified and require operator reconciliation.

A small permanent operator-only status tombstone retains the tenant ID, state, timestamps, project progress, and constant recovery categories. Minimal cumulative usage totals retain opaque IDs and UTC periods, without names, email addresses, secrets, SQL, or event payloads. Deletion never refunds usage. No audit retention period or pricing policy is selected by this operation.

The current offline credential-reset restore preserves opaque cleanup holds before neutralizing source export and pipeline jobs. Deleting a restored tenant stays `deleting` with `error_code=restored_artifacts_pending` until source ownership and outstanding current artifacts are reconciled. Reset and deletion do not contact or delete the source bucket. Reconciliation must verify the original namespace, published customer ownership, current objects, and multipart uploads. This version provides no ignore-hold endpoint or automatic reconciliation. Hosted recovery keeps operational keys and resumable cleanup instead of this access reset.

The `default` tenant can be deleted. Startup honors its tombstone and never recreates it. Create an explicit replacement tenant and provide its ID when creating projects; omitting the ID then returns `404`.

## Interactive query limits

All projects in a tenant share `hot_query_max_concurrent`. SQL queries, catalog discovery, and event profiles reserve a place before execution and release it when they finish or fail. A full tenant or deployment pool returns `429`; creating another project or credential does not create more places.

Hosted reserves tenant places immediately in PostgreSQL across workers, while enforcing local process capacity separately. Active executions renew 30-second reservations every five seconds, independently of HTTP cancellation. Unresponsive or uncertain executions quarantine the affected project's interactive engine without terminating the worker. Automatic expiry can permit physical concurrency above the tenant limit during a network partition. These controls are independent of the ingestion budget cache. See [native timeouts and worker supervision](shared-deployment.md#native-timeouts-and-worker-supervision) for renewal, shutdown, and failure semantics.

`hot_query_timeout_ms`, `hot_query_max_rows`, and `hot_query_max_result_bytes` bound each interactive SQL execution. Row or byte overflow fails rather than silently truncating the result. A SQL `LIMIT` can reduce a result, but does not replace the execution deadline.

Effective limits are the lower of the tenant budget and the deployment ceiling. The deployment defaults are two analytical slots per worker, 5,000 ms, 1,000 rows, and 1 MiB. Increasing a tenant budget cannot raise a deployment ceiling. Exports and scheduled S3 queries share worker capacity and have [separate execution budgets](export-api.md#consistency-retries-and-revocation).

A project reader can call `get-event-schema` to see configured `tenant_budgets`, `deployment_limits`, and effective interactive `limits`. The configured monthly allowance is visible; aggregate tenant consumption is not. Use [query discovery](../query-events.md) before building a query, and [exports](../query-events.md#export-a-result) for a larger result. Operators configure deployment ceilings in the [configuration reference](configuration.md#interactive-query-ceilings).

## Deployment boundary

The default uses separate control and journal SQLite files in one Porcupine process. The journal performs exact transactional ingestion accounting; control stores policy and replayable aggregate usage. Each batch refreshes policy under a local admission lock, which also serializes operator budget changes. Tenant usage reads reconcile journal high-water counters into control without double-counting. Query admission remains local. The [hosted profile](shared-deployment.md) stores operational metadata and shared query reservations in PostgreSQL. Unique committed events are counted by replayable publication of DuckLake effects. Hosted live acknowledgment can precede that accounting.

See the [roadmap](../roadmap.md#hosted-reliability) for planned hosted reliability work.

Moving from v0.14.0 requires a fresh data directory. A pre-tenant or combined control/journal SQLite database is rejected at startup; no automatic migration or compatibility mode is provided. Preserve an older deployment and its matching backup separately if you need to export data into a fresh instance.
