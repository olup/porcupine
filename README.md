<img src="site/assets/favicon.svg" width="72" height="72" alt="Porcupine logo">

# Porcupine

Porcupine is a headless product analytic tool and general event store. It's compatible with pothog and segment client. 

Headless means it ships no ui at all, and once installed is managed, operated or queried by an agent or a human though an mcp or a cli.

It came to be because I wanted a lightweight product analytic tool to self host, and realised that no one on my team ever opens the posthog ui - its all managed and queried by our local agents. 

Existing option where big inspirations - posthog, openpanel, pug - but no one offered the specific blend of lightness and power I envisionned - with features like direct sql queries, outward pipelines or s3 offload.

The rest of this readme is LLM generated.

[Agent quickstart](https://olup.github.io/porcupine/llms.txt) · [Local setup](#local-setup) · [Documentation](docs/README.md) · [Release downloads](https://github.com/olup/porcupine/releases/tag/v0.12.0)

Version 0.12.0 embeds DuckDB and DuckLake in the server. The default setup stores analytical data on the local SSD. S3 query destinations are supported; analytical S3 offload remains available only with the optional ClickHouse backend. [Storage options](docs/storage.md)

## Quickstart

Paste this into your coding agent:

> Read https://olup.github.io/porcupine/llms.txt and follow the guide to start Porcupine on my machine. Send a test event and show me the SQL that counts it.

### Local setup

The example runs one Porcupine server with embedded DuckDB and DuckLake on your laptop. You’ll need Git, curl, OpenSSL, jq and Docker Compose 2.24.4 or newer. Install the [standalone CLI](cli/README.md) and run this in a POSIX shell.

```sh
git clone https://github.com/olup/porcupine.git
cd porcupine
umask 077
(
    set -eu
    umask 077
    set -C
    bootstrap_token=$(openssl rand -hex 32)
    cat > .env <<EOF
PORCUPINE_BOOTSTRAP_TOKEN=$bootstrap_token
PORCUPINE_OAUTH_ISSUER=http://localhost:8080
EOF
)
docker compose -f compose.yaml -f compose.oauth.yaml -f compose.release.yaml up -d
curl --fail --silent --show-error --retry 30 --retry-all-errors --retry-delay 1 http://localhost:8080/readyz
sh examples/hello.sh
```

The script creates a project, sends one event called `hello`, and waits until DuckLake can count it.

```json
[{"event": "hello", "total": "1"}]
```

Run `docker compose -f compose.yaml -f compose.oauth.yaml -f compose.release.yaml down` to stop the server. The data stays in the volumes. [Hosting guide](docs/hosting.md)

## What runs on the server?

One Go service accepts events over HTTP. SQLite holds accepted work on disk until DuckLake commits it. DuckDB queries each project's local catalog and Parquet files. No separate analytical database service is needed. [Storage](docs/storage.md) · [Recovery](docs/restore.md)

I can send JSON straight from a backend, keep a supported PostHog SDK in a web app, or use Segment. Browser capture can be cookieless. There’s also an uploader for historical PostHog events. [SDK compatibility](docs/compatibility.md) · [Native and Segment inputs](docs/native-and-segment-ingestion.md) · [Imports](docs/posthog-import.md)

For a customer who wants a file every morning, I can save a SQL query in a pipeline and point it at their S3 bucket. For something like a signup notification, a webhook will do. [Pipelines](docs/pipelines.md) · [One-off exports](docs/exports.md)

## Development

`make build` builds the native server with CGO and a C/C++ toolchain. `make cli` builds the standalone client. You can also download the CLI for macOS, Linux or Windows. [CLI guide](cli/README.md)

```sh
make check              # Go tests, vet, formatting and generated CLI catalog
make ducklake-test      # native storage, queries and lifecycle tests
make integration        # optional ClickHouse backend
make integration-s3     # ClickHouse and local S3-compatible storage
make compat-install
make compat             # pinned PostHog SDKs
```

For the local documentation preview, install `docs-requirements.txt` in a Python virtual environment and run `make docs-preview`. The preview is available at `http://127.0.0.1:8787/porcupine/docs/`.

[Architecture](docs/architecture.md) · [Benchmark workloads and results](docs/benchmarks.md) · [MIT license](LICENSE)
