<img src="site/assets/favicon.svg" width="72" height="72" alt="Porcupine logo">

# Porcupine

Headless, self-hosted product analytics. Send events to Porcupine and give your agent access to a project. It can discover event names and properties, write ClickHouse SQL, and use the results to draw a chart or write a report.

I built Porcupine because I like PostHog and its SDKs, but I rarely open the dashboard. I already ask my agent to write the queries and draw the graphs. I wanted to self-host just the parts I use. Porcupine runs as a Go service with embedded SQLite and a ClickHouse server. There’s no admin UI. You manage projects, access and destinations through MCP, the HTTP API or the standalone CLI.

[Quickstart](#quickstart) · [Guide for agents](https://olup.github.io/porcupine/llms.txt) · [Documentation](docs/README.md) · [Release downloads](https://github.com/olup/porcupine/releases/tag/v0.9.0)

## Try it with your agent

If your agent is already connected to an instance, give it this prompt:

> Read https://olup.github.io/porcupine/llms.txt. List the projects I can read. In the project I choose, discover the event names, count the last seven days of events by name, and show me the SQL you used.

The MCP endpoint exposes two tools. `porcupine_docs` finds the operations your credential can use and describes their inputs. `porcupine_call` runs the selected operation. A person can connect through OAuth. A backend or shared agent can use a service credential scoped to its projects. [Connect your team](docs/connect-your-team.md) · [MCP setup](docs/mcp.md)

## Quickstart

On a machine with Git, curl, Python 3 and Docker Compose 2.24.4 or newer, run the following commands in a POSIX shell. The example uses two containers and binds the API to `localhost:8080`. It needs no cloud account or email provider.

```sh
git clone --branch v0.9.0 https://github.com/olup/porcupine.git
cd porcupine
umask 077
python3 - <<'PY'
import secrets
with open(".env", "x") as env:
    for key in ("PORCUPINE_BOOTSTRAP_TOKEN", "PORCUPINE_CLICKHOUSE_PASSWORD"):
        print(key + "=" + secrets.token_hex(32), file=env)
    print("PORCUPINE_OAUTH_ISSUER=http://localhost:8080", file=env)
PY
docker compose -f compose.yaml -f compose.oauth.yaml -f compose.release.yaml up -d
curl --fail --silent --show-error --retry 30 --retry-all-errors --retry-delay 1 http://localhost:8080/readyz
python3 examples/hello.py
```

The example creates a project, captures one `hello` event, and waits for ClickHouse. It prints the new project ID and the result of a SQL query:

```json
[{"event": "hello", "total": "1"}]
```

Stop the example with `docker compose -f compose.yaml -f compose.oauth.yaml -f compose.release.yaml down`. The volumes keep your data. For a shared instance, set the public HTTPS origin and configure access. [Hosting guide](docs/hosting.md) · [CLI for macOS, Linux and Windows](cli/README.md)

## What you can do

| Task | How it works |
| --- | --- |
| Collect events | Send native HTTP events from a backend, use the supported PostHog SDK profiles, or send Segment track, page and identify calls. Browser collection supports a cookieless profile. [SDK compatibility](docs/compatibility.md) · [Native and Segment inputs](docs/native-and-segment-ingestion.md) |
| Understand usage | Discover the properties actually present in a project, then query its `events` table with bounded, read-only ClickHouse SQL. Opt-in identity linking ties an anonymous visit to a signed-in user. [Queries and examples](docs/analytics.md) · [Identity](docs/data-and-identity.md) |
| Deliver data | Forward individual events or timed batches to a webhook. Run a SQL query on a schedule and send the results to S3. Use a one-off export for a large download. [Pipelines](docs/pipelines.md) · [SQL exports](docs/exports.md) |
| Migrate and manage | Import PostHog files with resumable uploads. Create or rename projects, grant access and delete a project through the management API. [PostHog imports](docs/posthog-import.md) · [Management operations](docs/management-api.md) |
| Keep history on a small server | SQLite durably queues accepted events until ClickHouse receives them. Optional cold storage moves older ClickHouse parts to S3 and keeps them queryable. [Storage](docs/storage.md) · [Backups and recovery](docs/online-backup.md) |

PostHog compatibility covers **ingestion**. Porcupine has its own query and management API. See the [tested SDK configurations](docs/compatibility.md) before switching an application’s capture host.

## Development

Build the server with `make build` and the separate CLI with `make cli`. The repository pins the Go toolchain in [go.mod](go.mod).

```sh
make check              # Go tests, vet, formatting and generated CLI catalog
make integration        # local ClickHouse
make integration-s3     # ClickHouse and local S3-compatible storage
make compat-install
make compat             # pinned PostHog SDKs
```

[Architecture](docs/architecture.md) · [Benchmark workloads and results](docs/benchmarks.md) · [MIT license](LICENSE)
