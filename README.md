<img src="site/assets/favicon.svg" width="72" height="72" alt="Porcupine logo">

# Porcupine

Porcupine collects product events and lets a coding agent query them with SQL. It’s headless. I run it on a Lightsail VM.

I like PostHog. I just found that for the things I do, I hardly ever need to open it. I’ll ask Codex a question about usage and have it show me the query and the graph together. What I wanted to keep was the bit where I could call `posthog.capture()` from my app. Running all of PostHog myself seemed like a lot for that.

[Quickstart](#quickstart) · [Guide for agents](https://olup.github.io/porcupine/llms.txt) · [Documentation](docs/README.md) · [Release downloads](https://github.com/olup/porcupine/releases/tag/v0.9.0)

## Try it with your agent

One of my first tests was to open a fresh Codex chat and ask it to count events by name. It found two `page_viewed` events and one `signup_completed` in the demo project. Here’s what I’d ask on a project with some traffic.

> Which events did we record this week? Show me the counts and the SQL you ran.

Connect your agent to `/mcp` and grant it read access to a project. It uses `porcupine_docs` to find out what it can call, then `porcupine_call` to run the query. [Connect your team](docs/connect-your-team.md) · [MCP setup](docs/mcp.md)

## Quickstart

The example runs Porcupine and ClickHouse on your laptop. You’ll need Git, curl, Python 3 and Docker Compose 2.24.4 or newer. Run this in a POSIX shell.

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

The script creates a project, sends one event called `hello`, and polls ClickHouse until it can count it.

```json
[{"event": "hello", "total": "1"}]
```

Run `docker compose -f compose.yaml -f compose.oauth.yaml -f compose.release.yaml down` to stop both containers. The data stays in the volumes. [Hosting guide](docs/hosting.md)

## What runs on the server?

One Go service takes the events over HTTP. SQLite holds them on disk until they reach ClickHouse. Older ClickHouse data can live on S3 and still turn up in the same queries. [Storage](docs/storage.md) · [Recovery](docs/online-backup.md)

I can send JSON straight from a backend, keep a supported PostHog SDK in a web app, or use Segment. Browser capture can be cookieless. There’s also an uploader for historical PostHog events. [SDK compatibility](docs/compatibility.md) · [Native and Segment inputs](docs/native-and-segment-ingestion.md) · [Imports](docs/posthog-import.md)

For a customer who wants a file every morning, I can save a SQL query in a pipeline and point it at their S3 bucket. For something like a signup notification, a webhook will do. [Pipelines](docs/pipelines.md) · [One-off exports](docs/exports.md)

## Development

`make build` builds the server. `make cli` builds the standalone client. You can also download the CLI for macOS, Linux or Windows. [CLI guide](cli/README.md)

```sh
make check              # Go tests, vet, formatting and generated CLI catalog
make integration        # local ClickHouse
make integration-s3     # ClickHouse and local S3-compatible storage
make compat-install
make compat             # pinned PostHog SDKs
```

[Architecture](docs/architecture.md) · [Benchmark workloads and results](docs/benchmarks.md) · [MIT license](LICENSE)
