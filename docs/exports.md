# Export query results

Use an export when an interactive query would return too many rows or bytes. An export runs asynchronously, writes private result files, and returns download links after success. It uses the same project permissions, SQL policy, current event revisions, and deletion rules as interactive queries.

The operator must enable export storage. Your connection needs `events:read` for the project. Export storage is separate from analytical files and recurring [pipelines](pipelines.md).

## Submit a query

Ask your agent to export a query and wait for the files, or save this body as `export.json`:

```json
{
	"idempotency_key": "daily-signups-november",
	"sql": "SELECT CAST(event_time AS DATE) AS day, count(*) AS total FROM events WHERE event = $event GROUP BY day ORDER BY day",
	"parameters": {"event": "signup_completed"},
	"format": "csv"
}
```

Replace the event name with one found through [discovery](query-events.md). Reuse the idempotency key only for the same logical export.

```sh
porcupine create-event-export --id PROJECT_ID --body @export.json
```

Keep the returned export ID, then inspect its state:

```sh
porcupine get-event-export --id PROJECT_ID --export-id EXPORT_ID
```

## Download completed files

Poll with a bounded delay until the state is `succeeded`, `failed`, or `canceled`. A failed or canceled export publishes no download links.

```sh
porcupine list-event-export-files --id PROJECT_ID --export-id EXPORT_ID
```

Download the files before the private links expire, within five minutes. You can request fresh links while the export remains retained. Treat links as credentials.

See the [export reference](reference/export-api.md) for formats, pagination, cancellation, operator budgets, retention, and S3 configuration. Exports still run within the server's resource limits; they do not provide separately provisioned compute.
