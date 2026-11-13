# Check instance status

Available in v0.10.0 and later on both the server and standalone CLI.

Use `get-instance-status` to inspect the running server's version, dependency checks, and optional service configuration. Your credential needs `management:read`. A project-scoped management reader can use it without instance administration. The response contains no project IDs, event counts, destination URLs, or credentials.

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
	"version": "0.11.0",
	"checked_at": "2026-11-12T12:00:00Z",
	"checks": {
		"sqlite": "ok",
		"ducklake": "ok",
		"clickhouse": "not_configured"
	},
	"configuration": {
		"query_exports": false,
		"webhook_pipelines": true,
		"s3_query_pipelines": true,
		"email_sign_in": true,
		"oidc_sign_in": false,
		"s3_tiering": false
	}
}
```

## Interpret the response

`checks.sqlite` tests the local database connection. `checks.ducklake` verifies the native archive registry identity. Optional ClickHouse deployments use an authenticated `SELECT 1` for `checks.clickhouse`. Each probe has a one-second deadline. An unused backend reports `not_configured`. These checks do not verify every project catalog, maintenance progress, or a drained delivery queue.

The overall `status` is `degraded` when a live check fails. The report still returns HTTP `200`, so clients must inspect the JSON status. Optional services that are not configured do not degrade the instance. Checks run afresh, and responses use `Cache-Control: no-store`. Only one report can run at a time; a concurrent request returns `429`.

Fields under `configuration` describe deployment configuration. They do not prove remote connectivity or grant the caller permission to use that service. The check sends no email, writes no S3 objects, and makes no requests to project destinations or the identity provider. Storage policy configuration does not prove that ClickHouse has already moved data to S3.

Read query syntax and execution limits through the project's [schema operation](analytics.md#event-schema). Inspect project delivery and pipeline runs through their own APIs.

## Health and readiness endpoints

The existing public endpoints remain available:

| Endpoint | Purpose |
| --- | --- |
| `GET /healthz` | Process liveness. |
| `GET /readyz` | SQLite readiness. Returns `503` if local storage is unavailable. |
| `GET /api/v1/status` | Authenticated dependency and configuration report. |

`porcupine status` continues to call `/readyz`. Use `porcupine get-instance-status` for the detailed report. Authentication itself depends on SQLite, so if local storage is unavailable, the detailed report can fail before its probes run. Use `/readyz` for that condition.
