<img src="site/assets/favicon.svg" width="72" height="72" alt="Porcupine logo">

# Porcupine

Porcupine is a headless product analytic tool and general event store. It's compatible with pothog and segment client. 

Headless means it ships no ui at all, and once installed is managed, operated or queried by an agent or a human though an mcp or a cli.

It came to be because I wanted a lightweight product analytic tool to self host, and realised that no one on my team ever opens the posthog ui - its all managed and queried by our local agents. 

Existing option where big inspirations - posthog, openpanel, pug - but no one offered the specific blend of lightness and power I envisionned - with features like direct sql queries, outward pipelines or s3 offload.

The rest of this readme is LLM generated.

[Agent quickstart](https://olup.github.io/porcupine/llms.txt) · [Local setup](#local-setup) · [Documentation](docs/README.md) · [Release downloads](https://github.com/olup/porcupine/releases/tag/v0.9.0)

## Quickstart

Paste this into your coding agent:

> Read https://olup.github.io/porcupine/llms.txt and follow the guide to start Porcupine on my machine. Send a test event and show me the SQL that counts it.

### Local setup

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
