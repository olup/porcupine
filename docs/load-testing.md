# Load testing

The HTTP load script below currently uses ClickHouse-specific run-marker SQL. Use it on the optional ClickHouse backend. Native sustained-load scenarios are documented in the [DuckLake validation report](ducklake-validation.md) and [maintenance investigation](ducklake-maintenance.md).

`scripts/load.py` uses Python's standard library to drive a configured HTTP or
HTTPS service. It creates synthetic events in an existing project. It does not
create projects, change storage settings, or delete the generated events.

Create a dedicated test project. Give one machine client `events:write` and another `events:read` on that project. Save the credentials outside the repository in a JSON file with mode `600`. The writer can send to a `server_only` project. The script requests and renews access tokens for the API audience at the configured issuer.

```json
{
  "project_id": "DEDICATED_LOAD_PROJECT_ID",
  "oauth": {
    "issuer": "https://analytics.example.test",
    "write": {"client_id": "WRITER_CLIENT_ID", "client_secret": "WRITER_SECRET"},
    "read": {"client_id": "READER_CLIENT_ID", "client_secret": "READER_SECRET"}
  }
}
```

To exercise public ingestion on a mixed project, include its `ingest_token` as a top-level field and select `--ingestion-auth public`. Keep the reader for query verification.

Run a short ramp:

```sh
python3 scripts/load.py \
  --url https://analytics.example.test \
  --credentials /private/porcupine-load-keys.json \
  --stage 10:2:20:4 \
  --stage 30:10:50:8 \
  --query-interval 1 \
  --drain-timeout 180 \
  --output /private/porcupine-load-report.json
```

Each repeated `--stage` is `seconds:requests_per_second:events_per_batch:concurrency`.
For example, `30:10:50:8` schedules 10 batch requests per second, each containing
50 events, for 30 seconds with at most eight ingestion requests in flight.
The target is therefore 500 events per second. There is one additional query
thread. Stages wait for their in-flight requests to finish before the next starts.

The default ramp is 10 seconds at `2:20:4`, then 20 seconds at `10:50:8`.
`--property-bytes` controls synthetic padding per event and defaults to 128.
The script validates every stage's body size before sending any events.

The scheduler never builds an unbounded backlog. It reports request slots skipped
because all workers were busy or because the generator fell behind. It does not
issue catch-up bursts. HTTP responses, worker counts, and latency histograms are
bounded. Request timeouts default to 15 seconds. HTTPS certificate verification
is enabled; use `--ca-file` for a private certificate authority. Redirects are
rejected so credentials are never forwarded to another endpoint.

Every run gets a fresh `run_id`. Events carry that value, a `synthetic` flag and
stable UUIDs derived from the run, stage, request and event indexes. The script
replays one successful batch exactly once and requires zero newly accepted events
on replay. `--run-id` can choose a marker, but preflight rejects an existing run.

A concurrent query counts only this run's `porcupine_load` events using bound SQL
parameters and `use_cache: false`. After ingestion, the script polls until the acknowledged unique event
count matches ClickHouse and the project's original and mutation queues are empty.
It requires two matching observations because query results and delivery status
do not share one database snapshot. Use a dedicated load project so unrelated
traffic cannot keep its queue nonempty.

The JSON report includes request status counts, accepted and sent event counts,
errors, achieved throughput, generator skips and p50/p95/p99 request latency.
Latency percentiles use fixed-memory histogram upper bounds with approximately
two percent resolution. It reports ingestion stages, concurrent queries, duplicate
replay and drain polling separately. Credentials and response bodies are excluded.
The output file is written with owner-only permissions.

Exit status is zero only when the final count matches, the duplicate replay is
verified, no request failed and no target request slot was skipped. Capacity tests
can intentionally produce nonzero status; inspect the aggregate report to separate
server errors from generator saturation. A timed-out request may already have
committed. Such ambiguity is reported as an error, and an excess final count also
fails verification. The script does not automatically retry failed ingestion.


## Queue capacity during outages

The original-event queue has two instance-wide limits. `PORCUPINE_QUEUE_MAX_EVENTS`
defaults to 100,000 pending events. `PORCUPINE_QUEUE_MAX_BYTES` defaults to
268,435,456 bytes, or 256 MiB. Set the byte limit as a positive decimal integer.
For example, `PORCUPINE_QUEUE_MAX_BYTES=67108864` limits pending original payloads
to 64 MiB. Compose forwards both settings to the service.

The byte charge is the stored UTF-8 length of the project ID, UUID, event name,
distinct ID, properties JSON, event and receipt timestamps, identity snapshot and
serialized ingestion metadata, plus 64 bytes per row. The service checks the charge after cookieless identity and
property processing. Both limits cover all projects sharing the SQLite queue.

If a new event would exceed either limit, ingestion returns HTTP 503 with
`Retry-After: 1`. A batch either commits completely or leaves the queue unchanged.
Retries with an already accepted stable UUID do not consume more capacity and
remain accepted while the queue is full. Delivery and queued-event deletion
release the corresponding byte charge transactionally.

SQLite triggers maintain count and byte totals in a single accounting row.
Existing pending rows are backfilled once when the accounting schema is first
created. Capture reads those totals without scanning the queue. If an existing
queue already exceeds a reduced limit, the service preserves its accepted rows
and rejects new unique events until enough capacity is released.

This payload budget is separate from SQLite file size. Reserve disk space for
indexes, WAL, durable deduplication receipts, mutation snapshots and other
metadata. During outage tests, use both limits to bound the backlog and inspect
HTTP 503 counts in the report. Increasing the event-count limit alone does not
increase the byte budget.
