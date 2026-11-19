# Check instance status

Use `get-instance-status` to inspect the running server's version, dependency checks, and optional service configuration. Your credential needs `management:read`. A project-scoped management reader can use it without instance administration. The response contains no destination URLs or credentials. Project IDs in execution health are visible only to instance administrators.

## From an agent, CLI, or HTTP

Ask your connected MCP agent to inspect the instance status. It discovers `get-instance-status` with `porcupine_docs` and calls it through `porcupine_call`.

From the CLI, set `PORCUPINE_URL` and sign in, then run:

```sh
porcupine get-instance-status
```

For HTTP, use an API-audience bearer credential:

```sh
curl --fail-with-body "$PORCUPINE_URL/api/v1/status" \
	-H "Authorization: Bearer $PORCUPINE_API_KEY"
```

An example response is:

```json
{
	"status": "ok",
	"version": "dev",
	"checked_at": "2026-11-12T12:00:00Z",
	"checks": {
		"sqlite": "ok",
		"ducklake": "ok"
	},
	"configuration": {
		"analytics": true,
		"query_exports": false,
		"webhook_pipelines": true,
		"s3_query_pipelines": true,
		"email_sign_in": true,
		"oidc_sign_in": false,
		"native_s3_storage": false
	}
}
```

## Interpret the response

`checks.sqlite` tests the local journal and control database connections in standalone and reports `not_configured` in hosted. `checks.ducklake` verifies the native archive registry identity. Each probe has a one-second deadline. An unconfigured analytical store reports `not_configured`. These dependency checks do not verify every project catalog or a drained delivery queue. For instance administrators, the separate `ducklake_cleanup` field reports the [native cleanup policy and progress](ducklake-cleanup.md). It is omitted for project-scoped callers.

The overall `status` is `degraded` when a live check fails. An administrator also sees `degraded` when a DuckLake cleanup attempt has failed or was interrupted, until that catalog succeeds or is deleted. When native backups are configured, administrators also receive `native_backup` with schedule, checkpoint time and progress. A failed or interrupted backup degrades their status. See [native backups](native-backups.md). The report still returns HTTP `200`, so clients must inspect the JSON status. Optional services that are not configured do not degrade the instance. Checks run afresh, and responses use `Cache-Control: no-store`. Only one report can run at a time; a concurrent request returns `429`.

Administrators use `native_backup.source_id` and `native_backup.latest_bundle` to identify an [S3 backup for recovery](native-backup-recovery.md#restore-an-s3-bundle).

In hosted deployments, `checks.shared_control` checks PostgreSQL. `checks.query_execution` is `unavailable` when this worker is draining, has an uncertain query reservation, or has quarantined a project's interactive engine. This degrades the report for all callers. Instance administrators also receive `query_execution` with `active`, `uncertain_permits`, `draining`, and `unhealthy_projects`. Reasons are `native_query_unresponsive` or `query_permit_uncertain`. Active counts include canceled calls that have not returned. An expired reservation can already have been reassigned even while the old call remains alive. The check reads execution state without starting native work. See [hosted query lifecycle](shared-deployment.md#native-timeouts-and-worker-supervision).

Administrators also receive `hosted_ingestion`. Its `dropped_requests` and `dropped_events` counters count isolated requests and their events discarded by the reporting worker since startup. `quarantined_projects` maps affected project IDs to error codes, including quarantines reported by other workers. The administrator report is `degraded` while this map has entries. A successful recovery or flush clears shared quarantine. See [webhook limits](pipeline-api.md#hosted-webhook-capacity) for the signals emitted when hosted webhook output is refused.

`configuration.analytics` reports that the native analytical store is configured; it is not a health check. `configuration.native_s3_storage` reports the native analytical S3 option. It does not test bucket connectivity or disclose its prefix or credentials.

Fields under `configuration` describe deployment configuration. They do not prove remote connectivity or grant the caller permission to use that service. The check sends no email, writes no S3 objects, and makes no requests to project destinations or the identity provider. Analytical S3 configuration does not prove bucket connectivity.

`configuration.external_authentication` identifies the replacement of builtin sign-in. `checks.external_authentication` reports whether the configured signing-key cache is fresh and its last refresh succeeded, without making a provider request. A missing or expired cache, or a failed refresh, reports `unavailable` and degrades this report. Public ingestion and native machine keys remain available; `/readyz` does not depend on the external issuer. See the [external authentication contract](external-auth.md).

Read query syntax and execution limits through the project's [schema operation](analytics.md#event-schema). Inspect project delivery and pipeline runs through their own APIs.

## Health and readiness endpoints

| Endpoint | Purpose |
| --- | --- |
| `GET /healthz` | Process liveness. |
| `GET /readyz` | Local SQLite or hosted PostgreSQL readiness. Also returns `503` for hosted query draining, uncertain reservations, or project quarantine. |
| `GET /api/v1/status` | Authenticated dependency and configuration report. |

`porcupine status` calls `/readyz`. Use `porcupine get-instance-status` for the detailed report. Authentication depends on the control database. When it is unavailable the detailed report can fail before its probes run. Use `/readyz` for that condition.

A hosted worker with one quarantined project becomes unready as a whole. An orchestrator may remove it from public routing, although direct requests for healthy projects still work while capacity remains. `/healthz` stays a process-liveness check.

## Verify delivery before routing traffic

Check readiness, read the authenticated instance report, then capture a synthetic event and verify that SQL can find it. Track pending-event age, queue bytes, pending mutations, delivery errors, free disk space, memory pressure, and maintenance. Use the [operator dashboard and visibility probe](observability.md) to check ingestion, queue pressure, and the time from capture acknowledgment to SQL visibility.
