# Monitor a Porcupine deployment

Run `make build cli` in this checkout. The metrics listener
and visibility probe were added after v0.15.1. Prometheus scrapes each process,
and Grafana displays the measurements. Run the visibility probe in a dedicated
synthetic project to measure the time from capture acknowledgment to SQL
visibility.

## Enable the private metrics listener

Add `metrics` to your existing server JSON configuration and restart the
server. This example uses port 9464 so Prometheus can keep port 9090 on the
same host.

```json
{
	"metrics": {
		"enabled": true,
		"listen": "127.0.0.1:9464"
	}
}
```

The listener is disabled by default. When enabled, it defaults to
`127.0.0.1:9090` if you omit `listen`. The equivalent server environment
variables are `PORCUPINE_METRICS_ENABLED` and `PORCUPINE_METRICS_LISTEN`.

`GET /metrics` uses a separate listener with no application authentication.
The server accepts an IP address and port on loopback, an RFC 1918 network, or
an IPv6 unique local network. Keep the listener on loopback if Prometheus
shares the network namespace. If Prometheus runs elsewhere, bind to the
worker's private IP, such as `10.10.2.14:9464`, and allow only the scraper to
reach that port. Exclude the port from your public proxy, load balancer, and
container port mappings. For containers on a private network, give Prometheus
direct access to each worker on that network.

## Connect Prometheus and Grafana

Add a scrape job to Prometheus. This example assumes Prometheus shares the
server's network namespace. In a hosted fleet, use service discovery or list
each worker separately using a distinct target address.

```yaml
scrape_configs:
  - job_name: porcupine
    scrape_interval: 15s
    metrics_path: /metrics
    static_configs:
      - targets: ["127.0.0.1:9464"]
```

After reloading Prometheus, check that `up{job="porcupine"}` returns 1 for each
worker. Scraping a load-balanced address mixes samples from different
processes under one target and gives incorrect counter rates. [Prometheus
scrape configuration](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#scrape_config)

Add the Prometheus data source to Grafana and set its scrape interval to match
the target interval. Download
[porcupine-operator.json](../../deployment/observability/porcupine-operator.json).
In Grafana, open **Dashboards**, choose **New**, then **Import dashboard**.
Upload the JSON and select the Prometheus data source. The dashboard defaults to the
`porcupine` server job and the `porcupine-visibility-probe` probe job. Change
the job names from the dashboard if your deployment uses other names. Use
**Worker** to narrow server panels to one or more scrape targets. [Grafana
dashboard import](https://grafana.com/docs/grafana/latest/visualizations/dashboards/build-dashboards/import-dashboards/)

## Read the dashboard

The counters and histograms belong to one server process and reset when it
restarts. A fresh series can have gaps until Prometheus has enough scrapes to
calculate a rate. Fleet rates sum the per-target rates. Fleet percentiles
combine histogram bucket rates before calculating a quantile. [Prometheus
histograms](https://prometheus.io/docs/practices/histograms/)

Queue depth, bytes, and oldest age are snapshots. Check the per-worker panels
for uneven pressure. Standalone samples the SQLite queue every five seconds.
A growing sample age means the dashboard may still show an old snapshot. The
hosted snapshot includes work in flight. Interpret admission and commit
counters using [storage and acknowledgment by profile](../storage.md#deployment-profiles).
The dashboard cannot count work lost before a worker reports it. Read durable
consumption and storage measurements in the authenticated [usage report](usage.md).

| Symptom | What to check |
| --- | --- |
| Scrape availability drops | Check the worker and the private listener. Confirm each replica has its own scrape target. |
| HTTP errors or management and MCP failures increase | Check the endpoint or operation and inspect the affected worker's stderr. |
| Admission rejections, queue depth, or oldest age increase | Check whether only one worker is under pressure. Compare the backlog with flush failures and retries. |
| Hosted drops appear | Check buffering and shutdown logs for recorded discards. Use the visibility probe to check the write and read path. |
| Query latency increases | Compare time spent in admission and backend execution. Check admission outcomes and per-worker backlog during the same interval. |
| Visibility slows or times out | Check ingestion, flushing, and queries in the same time window. If the probe crosses workers, confirm that both endpoints share the same deployment. |

HTTP ingestion traffic includes validation and authentication failures.
Admission counters begin after validation. A durable batch can succeed with
only duplicates. The identity-ignored event count is a subset of accepted
durable events. Hosted event throughput distinguishes memory admissions
(`buffered`) from events in a successful native commit (`native_committed`).
The native count includes only commits observed by the worker. Replays
do not increment it. Commits whose response was lost can be missing from
this operational count.

A standalone flush attempt covers projecting originals and acknowledging
them in the journal. A hosted attempt covers one call to the live flusher.
Retries and split batches can produce several attempts. Use the probe to
measure SQL visibility from the client.

The query panels group operations as `sql`, `catalog`, and `profile`.
Catalog covers event discovery, including recent event listings that read
DuckLake. Admission includes reserving capacity and recording the durable
usage start. Backend time stops when the backend returns, even if the
request context expired earlier. Used worker slots include reservations
for background work.

## Collect server logs

Collect stderr from each serving process with a JSON log parser. Filter by
`level` or `msg`. Request failure entries include safe fields such as
`route`, `status`, and `duration_ms`. Operation failures can include
`transport` and `operation`. Keep your supervisor's worker identity so you
can find the process that produced a metric spike. The server uses bounded
labels for metrics and omits request bodies, query text, and credentials
from its request telemetry. Use the dashboard for successful requests and
4xx rates. Background failures can include opaque project and batch IDs.
Startup failure entries include a safe `reason` when Porcupine recognizes
the cause, such as `address_in_use` or `bootstrap_token_required`.

Listener startup entries include `listener` and `address`. If the process exits
with a server error, check `stage`. The values `bind_api` and `bind_metrics`
identify address binding failures, and `open_storage` identifies failures
initializing storage. The values `serve_api` and `serve_metrics` identify a
listener that stopped with an error. At shutdown, `shutdown_http` identifies
an HTTP shutdown error and `close_storage` identifies a storage close error.

## Measure live query visibility

Create a dedicated project. Issue two [API-audience service keys](service-keys.md)
scoped to that project. Grant the writer `events:write` and the reader
`events:read`.

On a builtin instance with OAuth unconfigured, use a `mixed` project. An
instance administrator can issue separate project-scoped bearer keys with
`create-access-key`. [OAuth and legacy keys](oauth.md#operator-configuration)

```sh
porcupine create-access-key --body '{"name":"Visibility probe writer","permissions":["events:write"],"projects":["PROJECT_ID"]}'
porcupine create-access-key --body '{"name":"Visibility probe reader","permissions":["events:read"],"projects":["PROJECT_ID"]}'
```

Store the one-time tokens in your secret store. Provide the writer as
`PORCUPINE_PROBE_INGEST_TOKEN` and the reader as `PORCUPINE_PROBE_QUERY_TOKEN`.
Run the probe with your origins and the same project ID.

```sh
porcupine visibility-probe \
	--ingest-url https://writer.example.com \
	--query-url https://reader.example.com \
	--project PROJECT_ID
```

Omit `--query-url` to use the ingestion origin for both requests. The probe
accepts HTTPS origins and HTTP origins on loopback. To check
cross-worker hosted visibility, use origins that pin capture and queries to
different workers in the same deployment. If both origins load balance, the
worker pair depends on the routing policy.

Each run sends one native `porcupine_visibility_probe` event with a unique
message ID. After the capture acknowledgment, the probe polls SQL for that
event. A visible event exits with status 0. A timeout or failure exits with a
nonzero status. The measurement is from capture acknowledgment to the first
successful poll that sees the event. It includes polling delay and the
read request. Keep probe runs in the dedicated project because they write
real events. By default, the probe waits up to 30 seconds after acknowledgment
and up to five seconds for an individual HTTP request. It waits 250 milliseconds
between polls that return no match.

For continuous measurement, add `--interval 30s` to wait 30 seconds between
runs. Add the probe's listener under your existing Prometheus `scrape_configs`.
The listener defaults to `127.0.0.1:9465` and can be set with
`--metrics-listen`. Keep the listener private using the same network
restrictions as the server metrics listener.

```yaml
  - job_name: porcupine-visibility-probe
    scrape_interval: 15s
    static_configs:
      - targets: ["127.0.0.1:9465"]
```

The visibility histogram contains successful runs. Use the run outcomes
alongside latency to see timeouts, ingestion errors, and query errors.
Use the scrape panel to verify that Prometheus reaches the probe.
