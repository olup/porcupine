<img src="site/assets/favicon.svg" width="72" height="72" alt="Porcupine logo">

# Porcupine

Porcupine is a headless product analytic tool and general event store. It's compatible with pothog and segment client. 

Headless means it ships no ui at all, and once installed is managed, operated or queried by an agent or a human though an mcp or a cli.

It came to be because I wanted a lightweight product analytic tool to self host, and realised that no one on my team ever opens the posthog ui - its all managed and queried by our local agents. 

Existing option where big inspirations - posthog, openpanel, pug - but no one offered the specific blend of lightness and power I envisionned - with features like direct sql queries, outward pipelines or s3 offload.

The rest of this readme is LLM generated.

[Agent quickstart](https://olup.github.io/porcupine/llms.txt) · [Local setup](#local-setup) · [Documentation](docs/README.md) · [Release downloads](https://github.com/olup/porcupine/releases/tag/v0.15.0)

Version 0.15.0 embeds DuckDB and DuckLake in the server. The default setup stores analytical data on the local SSD. New deployments can put DuckLake data files on S3 while keeping their catalogs and SQLite local. S3 query destinations remain separate from analytical storage. [Storage options](docs/storage.md)

## Quickstart

Paste this into your coding agent:

> Read https://olup.github.io/porcupine/llms.txt and follow the guide to start Porcupine on my machine. Send a test event and show me the SQL that counts it.

### Local setup

The example runs one Porcupine server with embedded DuckDB and DuckLake on your laptop. You’ll need Git, curl, OpenSSL, jq and Docker Compose 2.24.4 or newer. Install the [standalone CLI](cli/README.md) and run this in a POSIX shell.

```sh
git clone --branch v0.15.0 https://github.com/olup/porcupine.git
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

## What runs on the server?

One Go service accepts events over HTTP. SQLite holds accepted work on disk until DuckLake commits it. DuckDB queries each project's local catalog and Parquet files. No separate analytical database service is needed. [Storage](docs/storage.md) · [Recovery](docs/reference/restore.md)

[Tenants and usage budgets](docs/reference/tenants.md) group projects under operator-controlled monthly ingestion allowances and interactive query limits. A fresh instance creates a `default` tenant. Keep it for one team, or create a tenant before adding its projects. The instance operator invites the team's first tenant administrator, who can create projects, invite readers or other members, and manage scoped team service accounts. Follow the [tenant setup](docs/reference/tenant-access.md) after connecting the instance operator. Upgrading from v0.14.0 requires a fresh data directory. [Upgrade steps](docs/hosting.md#move-from-v0140).

The optional [hosted profile](docs/design/hosted-profile-composition.md) uses PostgreSQL for metadata and S3 for analytical data. It acknowledges live admission to worker memory. A crash can lose acknowledged events that have yet to commit.

I can send JSON straight from a backend, keep a supported PostHog SDK in a web app, or use Segment. Browser capture can be cookieless. The standalone CLI imports native, PostHog, or Segment history from files or directories. [SDK compatibility](docs/reference/compatibility.md) · [Native and Segment inputs](docs/reference/native-and-segment-ingestion.md) · [Imports](docs/imports.md)

For a customer who wants a file every morning, I can save a SQL query in a pipeline and point it at their S3 bucket. For something like a signup notification, a webhook will do. [Pipelines](docs/pipelines.md) · [One-off exports](docs/exports.md)

Import historical events with `porcupine import --project PROJECT_ID --format native FILE_OR_DIRECTORY`. Choose `native`, `posthog`, or `segment`. The command streams gzip, resumes interrupted uploads, preserves occurrence times and deduplicates stable IDs. [Import guide](docs/imports.md)

## Development

`make build` builds the native server with CGO and a C/C++ toolchain. `make cli` builds the standalone client. You can also download the CLI for macOS, Linux or Windows. [CLI guide](cli/README.md)

```sh
make check              # fast loop: contracts, vet, formatting and short Go tests
make check-full         # complete local validation; CI runs the same race suite in parallel groups
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
