# Tenants and usage budgets

**Unreleased.** This guide describes the tenant implementation in development. The published v0.14.0 release does not include these operations or settings.

A tenant groups projects that share an ingestion allowance and interactive query limits. On a fresh installation, Porcupine creates the tenant `default`, named `Default`. You can keep every project there for a single-team deployment. No additional database or service is required.

## Ownership and access

For a fresh deployment, follow [team setup](connect-your-team.md#choose-a-tenant-for-your-projects-unreleased): connect the instance administrator, choose or create a tenant, create its projects, then invite people to those projects.

Each project belongs to one tenant. Its `tenant_id` is returned with project metadata and cannot be changed after creation. Producers send events to a project; Porcupine resolves its tenant from stored project ownership. A field supplied in an event cannot select a different tenant.

Permissions still name projects explicitly. A person or service can have access to several projects, including projects in different tenants. Adding access to one project does not expose another project's events or the tenant's aggregate usage. An operator-issued `*` project grant covers the whole deployment, including all tenants; it is not a tenant-local wildcard.

There is no invitation to an entire tenant. A person invited to existing projects does not gain access to projects created later in that tenant.

Only a credential with `instance:admin` can create projects, manage tenants, change budgets, suspend ingestion, or read tenant-wide usage. Project readers can see the configured budgets and deployment ceilings through their project's schema, but cannot change them or read aggregate consumption. There is no separate tenant administrator role. Project permissions continue to control queries, imports, event changes, and pipelines. [Permissions](security-and-interfaces.md)

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

Every operation in this table requires `instance:admin`. `list-tenants` accepts `limit` (default 100, maximum 200). Pass a returned `next_cursor` as `cursor` for the next page. There is no tenant deletion or manual budget-reset operation.

After connecting the matching CLI with an instance administrator's credential, inspect the default tenant and its current usage:

```sh
porcupine list-tenants
porcupine get-tenant --tenant-id default
porcupine get-tenant-usage --tenant-id default
```

These commands are generated from the same API operations. The released v0.14.0 CLI does not contain them.

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
    "hot_query_max_concurrent": 1,
    "hot_query_timeout_ms": 3000,
    "hot_query_max_rows": 500,
    "hot_query_max_result_bytes": 524288
  }
}
```

These are technical limits, not a subscription or price. Replacing budgets does not change recorded consumption.

## Monthly ingestion allowance

`ingestion_monthly_events` counts new unique canonical rows durably accepted across all projects in the tenant. A supported identity control or accepted historical person snapshot also consumes a row. Duplicate retries, ignored snapshots, invalid rows, and rolled-back work consume nothing. A later edit, event deletion, or project deletion does not refund usage.

Use `null` for unlimited ingestion and `0` to disallow new events. Zero still permits retries of events already accepted. Porcupine records usage in the same SQLite transaction as durable acceptance; it does not wait for analytical delivery. A quota refusal does not leave a partially charged acceptance transaction. In default standalone mode, this transaction also enforces the tenant's complete allowance exactly.

In optional [shared mode](shared-deployment.md#shared-budgets), each project owner publishes cumulative usage to PostgreSQL and refreshes cached policy on a ten-second cycle. Its own unpublished usage counts immediately. Other owners' usage and policy changes can lag, allowing soft quota overshoot. Publication is idempotent; duplicate reports do not charge twice. An expired cache must refresh before new acceptance continues. Authentication revocation does not use this policy cache.

Periods are UTC calendar months, from midnight on the first day until midnight on the next month's first day. Historical imports consume the allowance of the month when they are accepted, even if their occurrence dates are years earlier. A new month opens a new counter; previous counters remain available. There is no reset command. An operator can raise the allowance when more ingestion is needed.

Call `get-tenant-usage` without `period` for the current month, or pass `period=2026-11` for a selected month. The response includes:

```json
{
  "tenant_id": "TENANT_ID",
  "period_start": "2026-11-01T00:00:00Z",
  "period_end": "2026-12-01T00:00:00Z",
  "accepted_events": "75000",
  "remaining_events": "25000",
  "ingestion_allowed": true
}
```

Counts are decimal strings. `remaining_events` is `null` for an unlimited allowance. Reducing a budget below existing consumption leaves zero remaining; it does not rewrite the counter. The usage response's `ingestion_allowed` combines the operator switch with the selected period's remaining allowance, using the current budget configuration. It is not a historical record of past budget settings.

On a shared deployment, aggregate usage reflects the counters published so far. It can lag events already durably accepted on owner volumes. These APIs do not promise a hard cross-worker quota or spending cap.

## Suspend or resume ingestion

Send `{"ingestion_allowed":false}` to `update-tenant` to stop accepting new events. Send `true` to lift that suspension. An exhausted monthly budget still prevents new acceptance until the allowance increases or the next UTC month begins.

Suspension does not stop delivery of already accepted events, duplicate retries, queries, exports, or authorized deletions. Those operations retain their own permissions and limits.

When a tenant is suspended or its ingestion allowance is exhausted:

- Native and Segment ingestion return `429` without event receipts for rejected work.
- PostHog compatibility ingestion returns an intentional terminal `200` with `quota_limited`. This does not mean the rejected events were stored. Queue and storage failures remain errors.
- Historical imports retain their checkpoint. Resume the same import after raising the allowance or after the next month starts; accepted rows are not charged again.

## Interactive query limits

All projects in a tenant share `hot_query_max_concurrent`. SQL queries, catalog discovery, and event profiles reserve a place before execution and release it when they finish or fail. A full tenant or deployment pool returns `429`; creating another project or credential does not create more places.

Shared mode reserves tenant places immediately in PostgreSQL across workers, while enforcing local process capacity separately. A crashed request or failed release keeps its reservation until the execution deadline, object-storage grace, PostgreSQL metadata grace, and 30-second safety margin have elapsed. If native work still has not returned after that deadline and both I/O allowances plus 15 seconds, a watchdog terminates the worker with status `70` before the place can be reused. Run it under a supervisor that restarts with the same owner volume. These safeguards are independent of the ten-second ingestion budget cache. [Native timeouts and supervision](shared-deployment.md#native-timeouts-and-worker-supervision)

`hot_query_timeout_ms`, `hot_query_max_rows`, and `hot_query_max_result_bytes` bound each interactive SQL execution. Row or byte overflow fails rather than silently truncating the result. A SQL `LIMIT` can reduce a result, but does not replace the execution deadline.

Effective limits are the lower of the tenant budget and the deployment ceiling. The deployment defaults are two analytical places, 5,000 ms, 1,000 rows, and 1 MiB. Increasing a tenant budget cannot raise a deployment ceiling. Existing background exports and pipelines keep their separate execution budgets and deployment-level admission controls.

A project reader can call `get-event-schema` to see configured `tenant_budgets`, `deployment_limits`, and effective interactive `limits`. The configured monthly allowance is visible; aggregate tenant consumption is not. Use [query discovery](query-events.md) before building a query, and [exports](exports.md) for a larger result. Operators configure deployment ceilings in the [hosting guide](hosting.md#unreleased-interactive-query-ceilings).

## Deployment boundary

The default uses exact transactional ingestion accounting and local query admission in one Porcupine process with SQLite. The optional unreleased [shared deployment](shared-deployment.md) uses PostgreSQL metadata, shared query reservations, ten-second usage publication and policy refresh, owner-local SQLite queues, and S3 Parquet. No event payloads enter operational PostgreSQL or PostgreSQL-backed DuckLake catalogs.

Hosted billing, rate limits across workers, replicated acceptance, automatic owner failover, and separately provisioned query compute remain [future architecture work](roadmap.md#future-saas-architecture-to-evaluate).

The unreleased tenant schema requires a fresh data directory. A pre-tenant SQLite database is rejected at startup; no automatic migration or compatibility mode is provided. Preserve an older deployment and its matching backup separately if you need to export data into a fresh instance.
