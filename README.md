<img src="site/assets/favicon.svg" width="72" height="72" alt="Porcupine logo">

# Porcupine

Porcupine is a headless product analytics tool and general event store. It supports PostHog and Segment clients.

Headless means it ships no UI. Once installed, an agent or a human can manage and query it through MCP or the CLI.

It came to be because I wanted a lightweight product analytics tool to self-host, and realised that no one on my team ever opens the PostHog UI. Our local agents manage and query it.

PostHog, OpenPanel, and Pug inspired it. I wanted a lightweight service with direct SQL queries, outbound pipelines, and S3 storage.

The rest of this readme is LLM generated.

[Agent quickstart](https://olup.github.io/porcupine/llms.txt) · [Local setup](#local-setup) · [Documentation](docs/README.md) · [Release downloads](https://github.com/olup/porcupine/releases/tag/v0.19.4)

## Quickstart

Paste this into your coding agent:

> Read https://olup.github.io/porcupine/llms.txt and follow the guide to start Porcupine on my machine. Send a test event and show me the SQL that counts it.

### Local setup

The example runs one Porcupine server with embedded DuckDB and DuckLake on your laptop. You’ll need Git, curl, OpenSSL, jq and Docker Compose 2.24.4 or newer. Install the [standalone CLI](cli/README.md) and run this in a POSIX shell.

```sh
git clone --branch v0.19.4 https://github.com/olup/porcupine.git
cd porcupine
docker compose -f compose.yaml -f compose.release.yaml --profile setup run --rm init
docker compose -f compose.yaml -f compose.release.yaml --profile tools run --rm config-check
docker compose -f compose.yaml -f compose.release.yaml up -d porcupine
curl --fail --silent --show-error --retry 30 --retry-all-errors --retry-delay 1 http://localhost:8080/readyz
sh examples/hello.sh
```

The script creates a project, sends one event called `hello`, and waits until DuckLake can count it.

```json
[{"event": "hello", "total": "1"}]
```

Run `docker compose -f compose.yaml -f compose.release.yaml down` to stop the server. The data stays in the volumes. [Hosting guide](docs/hosting.md)

## Use and run Porcupine

Porcupine embeds DuckDB and DuckLake. See [standalone and hosted storage](docs/storage.md#deployment-profiles), [team setup](docs/reference/tenant-access.md), and [upgrades](docs/hosting.md#upgrade).

Hosted ingestion can lose acknowledged events if a worker crashes before committing them to DuckLake.

Send events with [native or Segment inputs](docs/reference/native-and-segment-ingestion.md) or a [supported PostHog SDK](docs/reference/compatibility.md). Use the CLI to [import history](docs/imports.md).

[Query or export events](docs/query-events.md) with SQL. For scheduled results or webhook delivery, use a [pipeline](docs/pipelines.md).

## Development

`make build` builds the native server with CGO and a C/C++ toolchain. `make cli` builds the standalone client. You can also download the CLI for macOS, Linux or Windows. [CLI guide](cli/README.md)

```sh
make check              # fast loop: contracts, vet, formatting and short Go tests
make check-full         # local lint and full race suite
make integration        # native smoke, S3 and offline restore drills
make integration-s3     # local HTTP/TLS S3-compatible fixtures
make compat-install
make compat             # pinned PostHog SDKs
```

Run `./scripts/aws-emulator-test.sh` for an optional check against the official
AWS Emulator. It runs the versioned import publication and recovery test and the
native S3 restart and project deletion test. It requires Docker, the AWS CLI,
and the native Go build prerequisites above. The script starts a disposable
HTTPS emulator on `127.0.0.1:19297`, uses its generated credentials and
certificate, and starts the existing local PostgreSQL fixture. Set
`PORCUPINE_AWS_EMULATOR_PORT` to a free port if needed. Pass a Go test regex as
the first argument to select other tests in `internal/porcupine`.

The runner builds the emulator image on first use and prints the emulator version
on every run. The emulator keeps state in memory and has limited IAM policy
evaluation. [AWS Emulator
documentation](https://docs.aws.amazon.com/emulator/latest/userguide/)

For the local documentation preview, run `make docs-preview`. Go downloads and verifies the pinned Hugo binary on the first build, then reuses it from your user cache. No other runtime is needed. The preview is available at `http://127.0.0.1:8787/porcupine/docs/`.

[Architecture](docs/architecture.md) · [MIT license](LICENSE)
