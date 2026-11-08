# Porcupine

Agent-first, self-hosted product analytics with no built-in UI. Agents discover
events and properties, write SQL, and query their project's data through MCP or
the HTTP API. Applications send events through a PostHog-compatible endpoint.

[Project site](https://olup.github.io/porcupine/) ·
[Connect an agent](docs/mcp.md) · [API reference](docs/management-api.md) · [MIT license](LICENSE)

Porcupine is an early project. It does not implement the full PostHog product or
its default SDK configuration. Human login, identity
merging, feature flags, and session replay are outside the current scope. See the
[compatibility profile](docs/compatibility.md) before pointing an SDK at it.

## Quickstart

Requires Git, Docker with Compose, and Python 3. This starts a local instance with
OAuth enabled, fresh private credentials, and persistent Docker volumes.
ClickHouse is private to the Compose network; HTTP binds to `127.0.0.1:8080`.

```sh
git clone https://github.com/olup/porcupine.git
cd porcupine
umask 077
python3 - <<'PY'
import secrets
with open(".env", "x") as env:
    env.write("PORCUPINE_BOOTSTRAP_TOKEN=" + secrets.token_hex(32) + "\n")
    env.write("PORCUPINE_CLICKHOUSE_PASSWORD=" + secrets.token_hex(32) + "\n")
    env.write("PORCUPINE_OAUTH_ISSUER=http://localhost:8080\n")
PY
docker compose -f compose.yaml -f compose.oauth.yaml up -d --build
```

The configuration step refuses to overwrite an existing `.env`. Keep this file
private. Changing its bootstrap secret does not reset credentials in an existing
database. For a public deployment, use HTTPS, set the issuer to its exact public
origin, and configure a reverse proxy before exposing the service.

Run this example from the same directory. It exchanges the bootstrap secret for
an administrator access token, creates a project, sends one cookieless event,
and waits up to 90 seconds for SQL to find it. Credentials and the short-lived
token stay in memory; only the project ID and query result are printed.

```sh
python3 examples/hello.py
```

The [example](examples/hello.py) asks ClickHouse for:

```sql
SELECT event, count() AS total
FROM events
WHERE event = {name:String}
GROUP BY event
```


The result contains `hello` with a total of `"1"`. ClickHouse 64-bit integers use
JSON strings to preserve precision. Running the example again creates another
project. `docker compose -f compose.yaml -f compose.oauth.yaml down` stops the
services and keeps their volumes. Adding `--volumes` destroys the local data.

Use the bootstrap administrator only for setup. Register separate
[OAuth clients](docs/oauth.md) with permissions and project grants for each
service or agent. Browser ingestion keys are public, write-only project
credentials; never put an OAuth client secret in a browser. Projects can instead
require authenticated backend collection with `ingestion_mode: "server_only"`.

## SQL and agents

The native API exposes project creation, credentials, event/schema discovery,
read-only SQL, revision-checked corrections and deletions, and optional private
SQL exports. [OpenAPI](docs/management-api.md) is served at `/openapi.json`.

SQL addresses the project-local `events` table. The service validates the SQL
subset and executes it with a restricted ClickHouse reader and row policies.
Project grants apply to HTTP and MCP, including administrators. Queries are
bounded; exports use separate operator-controlled budgets. Start with the
[schema and query examples](docs/analytics.md).

The Streamable HTTP endpoint at `/mcp` exposes two tools: `porcupine_docs` for
permission-filtered contracts and `porcupine_call` for API operations. Obtain an
OAuth token for the `/mcp` resource; an `/api/v1` token cannot authenticate MCP.
Clients must support machine credentials or accept a token from their host.
There is no interactive OAuth login flow. See [MCP setup](docs/mcp.md).

## Delivery pipelines

[Project pipelines](docs/pipelines.md) send accepted original events to webhooks
in event or closed-window batch mode, or run scheduled receipt-window S3 queries. They are separate from native ClickHouse cold storage and one-off
SQL downloads. API and MCP reads require `pipelines:read` plus `events:read`;
configuration and retries require `pipelines:write` plus `events:read`.

The operator enables the runtime with a private 32-byte encryption key and
controls custom S3 endpoint origins. Destination credentials are write-only,
encrypted inputs. Delivery is at least once, and supported restores always
pause destinations for review before any resume. No external integration or
production destination is implied by the local test coverage.

## Storage and operations

- **Durable acceptance:** a successful capture follows the SQLite transaction
  commit. Count and byte limits bound pending payloads; a full queue returns a
  retryable error. Keep a stable event `uuid` across retries. Delivery is at least
  once, with deduplication in project queries. See [reliability](docs/reliability.md).
- **Persistent state:** preserve both Docker volumes. SQLite holds credentials,
  queue state, and receipts; ClickHouse holds analytical history. Delivered event
  payloads leave the local queue. Permanent deduplication receipts are the
  default; [bounded receipts](docs/receipts.md) are optional.
- **Cookieless collection:** the supported SDK profile uses daily, project-scoped
  pseudonyms rather than browser storage. These are not persistent people across
  days. Configure trusted proxies explicitly. See [identity](docs/data-and-identity.md).
- **Cold S3 storage:** an optional operator-only Compose override moves eligible
  ClickHouse parts to S3, where they remain queryable. It does not impose an
  age-based deletion policy or replace a backup. Keep local disk metadata as
  well as S3 objects. See [storage](docs/storage.md).
- **Recovery and exports:** use [online checkpoints](docs/online-backup.md) for
  paired SQLite/ClickHouse recovery. Optional [SQL exports](docs/exports.md)
  publish gzip CSV or JSONL to a separate private S3 prefix after full success.

See [.env.example](.env.example) for configuration and
[load testing](docs/load-testing.md) for measurement. The
[local benchmark](docs/benchmarks.md) records the workload, resource limits and results.

## Development

The server requires Go 1.26.10 or newer and builds without CGO. Docker is needed
for integration tests; the pinned SDK suite also needs Node.js and Chromium.

```sh
make check              # vet, Go tests, formatting
make integration        # real local ClickHouse
make integration-s3     # ClickHouse and local S3-compatible storage
make compat-install
make compat             # pinned official PostHog SDK profile
```

See the [roadmap](docs/roadmap.md) for planned work and the
[access and isolation reference](docs/security-and-interfaces.md) for security boundaries.
