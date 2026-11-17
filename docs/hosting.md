# Host Porcupine 0.14.0

The default server embeds DuckDB and DuckLake. One container holds the Go service, SQLite acceptance database, project catalogs, and Parquet data. No separate analytical database is required. Use persistent local storage and one application writer per data directory.

The [v0.14.0 release](https://github.com/olup/porcupine/releases/tag/v0.14.0) provides Linux amd64 and arm64 server images, plus six standalone CLI archives for macOS, Linux, and Windows. The server image bundles its compatible DuckLake, HTTPFS and AWS extensions. The CLI is a separate executable and needs no native database libraries.

## Choose local SQLite or shared PostgreSQL (unreleased)

The development server supports both deployment modes in the same binary. The published v0.14.0 image supports only the standalone mode. Match the CLI and server versions.

| Choose | Local SQLite, the default | Shared PostgreSQL |
| --- | --- | --- |
| Application workers | One process with its own persistent volume | Several workers, each with its own persistent SQLite volume |
| Accounts, project ownership, budgets, and coordination | Local SQLite | Shared PostgreSQL |
| Event acceptance queue and retry receipts | Local SQLite | Project owner's local SQLite |
| DuckLake catalog metadata | Local project catalogs | PostgreSQL |
| Analytical event files | Local SSD or S3 | S3 |
| Tenant and project support | Multiple tenants, each with projects | Multiple tenants, each with projects |

Local mode uses two separate SQLite files. `PORCUPINE_DB` selects the ingestion journal, defaulting to `./data/porcupine.db` for a native process. `PORCUPINE_CONTROL_DB` selects the control database; when omitted it defaults to the journal path plus `.control`. The container defaults are `/data/porcupine.db` and `/data/porcupine.db.control`. Keep both files and their SQLite auxiliary files on persistent local storage.

Control contains accounts, grants, tenants, project metadata, policies, and reported usage. The journal contains accepted events, retry receipts, identity state, durable acceptance counters, imports, and delivery jobs. They are linked by persistent instance identities. Startup rejects a missing or unrelated replacement database; restore both together. Shared mode replaces only the control database with PostgreSQL and rejects `PORCUPINE_CONTROL_DB`.

For one server or one team, keep SQLite. PostgreSQL is not required to use tenants, email accounts, budgets, the API, CLI, or MCP. A fresh tenant-enabled instance creates `default`; you can keep every project there.

Choose [shared PostgreSQL deployment](shared-deployment.md) when workers need common management state and analytical catalogs. PostgreSQL stores metadata, not an intermediate copy of event payloads. Shared mode still needs durable project ownership and the owner's SQLite volume. It does not provide automatic writer failover, transparent project rebalancing, or coordinated whole-instance backup.

After deployment, [connect the instance operator, choose a tenant, and invite its first administrator](tenant-access.md). That administrator creates the team's projects, invites members, and manages team service accounts. Members receive permissions on selected projects or all projects in their tenant. Hard budgets and suspension remain instance-operator settings in both storage modes.

The standalone setup below remains the default. Start a fresh data directory for the unreleased tenant-enabled schema; existing combined control/journal and pre-tenant data directories are not migrated.

## Laptop with Docker Compose

Install Git, curl, OpenSSL, jq, the [standalone CLI](../cli/README.md), Docker, and Docker Compose 2.24.4 or newer. Use a POSIX shell, such as WSL on Windows.

```sh
git clone https://github.com/olup/porcupine.git
cd porcupine
umask 077
(
    set -eu
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

The setup refuses to overwrite `.env`. Preserve that file privately. The example creates a project, sends a synthetic `hello` event, and waits until SQL counts it. Each run creates another project. [Project management](projects.md)

The release overlay removes the source build and selects `ghcr.io/olup/porcupine:0.14.0`. Set `PORCUPINE_IMAGE` to a different explicit tag or digest when needed. Keep the overlay after `compose.yaml`.

Only `127.0.0.1:8080` is published. The `porcupine-data` named volume contains the complete `/data` directory, including SQLite and DuckLake. Keep the same Compose project name and volume when replacing a container.

```sh
docker compose -f compose.yaml -f compose.oauth.yaml -f compose.release.yaml ps
docker compose -f compose.yaml -f compose.oauth.yaml -f compose.release.yaml stop
```

`stop` and `down` preserve named volumes unless `--volumes` is added. Do not remove a volume containing data you intend to retain.

### Load an image without registry access

Choose the Docker server's architecture and verify the archive before loading it:

```sh
case "$(docker info --format '{{.Architecture}}')" in
  amd64|x86_64) PORCUPINE_RELEASE_ARCH=amd64 ;;
  arm64|aarch64) PORCUPINE_RELEASE_ARCH=arm64 ;;
  *) echo 'Unsupported release architecture' >&2; exit 1 ;;
esac
PORCUPINE_RELEASE_ARCHIVE="porcupine-0.14.0-linux-$PORCUPINE_RELEASE_ARCH.tar.gz"
PORCUPINE_RELEASE_URL=https://github.com/olup/porcupine/releases/download/v0.14.0
curl --fail --location --output "$PORCUPINE_RELEASE_ARCHIVE" "$PORCUPINE_RELEASE_URL/$PORCUPINE_RELEASE_ARCHIVE"
curl --fail --location --output SHA256SUMS "$PORCUPINE_RELEASE_URL/SHA256SUMS"
expected=$(awk -v name="$PORCUPINE_RELEASE_ARCHIVE" '$2 == name { print $1 }' SHA256SUMS)
actual=$(openssl dgst -sha256 "$PORCUPINE_RELEASE_ARCHIVE" | awk '{ print $NF }')
test -n "$expected" && test "$actual" = "$expected" || {
    echo 'Archive checksum mismatch' >&2
    exit 1
}
docker load -i "$PORCUPINE_RELEASE_ARCHIVE"
docker compose -f compose.yaml -f compose.oauth.yaml -f compose.release.yaml up -d
```

The server image includes its native extension. An offline host also needs any optional proxy image transferred separately.

## Single Linux VM with HTTPS

Use the same Compose deployment on a Linux VPS or Lightsail instance. Install Docker and Compose first. Create a DNS record for your own hostname pointing to the VM. Allow inbound TCP 80 and 443 to the proxy, restrict SSH to your administrative access, and keep API port 8080 private.

Start with the private `.env` above. Change its issuer and add your hostname:

```dotenv
PORCUPINE_PUBLIC_HOST=analytics.example.com
PORCUPINE_OAUTH_ISSUER=https://analytics.example.com
```

Use your real hostname in both entries. Keep the hostname without a scheme in `PORCUPINE_PUBLIC_HOST`. Create `Caddyfile` in the repository root:

```caddyfile
{$PORCUPINE_PUBLIC_HOST} {
    reverse_proxy porcupine:8080 {
        header_up Host 127.0.0.1:8080
    }
}
```

Create `compose.vm.yaml` alongside it:

```yaml
services:
  porcupine:
    environment:
      PORCUPINE_TRUSTED_PROXIES: 172.30.86.10/32
  caddy:
    image: caddy:2.11.9
    restart: unless-stopped
    environment:
      PORCUPINE_PUBLIC_HOST: ${PORCUPINE_PUBLIC_HOST:?Set your public hostname}
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./Caddyfile:/etc/caddy/Caddyfile:ro
      - caddy-data:/data
      - caddy-config:/config
    networks:
      default:
        ipv4_address: 172.30.86.10
networks:
  default:
    ipam:
      config:
        - subnet: 172.30.86.0/24
          gateway: 172.30.86.1
volumes:
  caddy-data:
  caddy-config:
```

Choose a subnet that does not overlap your LAN, VPN, or existing Docker networks. If changing it, update the subnet, gateway, proxy address, and trusted-proxy entry together. The higher static address avoids Docker's first dynamic container addresses. Only that proxy address is trusted for forwarded client IPs.

Caddy uses the public hostname to obtain and renew certificates. DNS and public ports must work for its certificate challenges. See [Caddy HTTPS setup](https://caddyserver.com/docs/quick-starts/https). This recipe assumes clients reach Caddy directly; adding a CDN or another proxy requires a separate review of forwarded headers.

The fixed upstream Host preserves the MCP SDK's loopback-host protection. OAuth metadata still uses the configured public issuer. Caddy supplies forwarded client information; it does not trust arbitrary incoming forwarded headers by default. See [reverse-proxy header behavior](https://caddyserver.com/docs/caddyfile/directives/reverse_proxy).

Validate and start with the complete overlay set:

```sh
docker compose -f compose.yaml -f compose.oauth.yaml -f compose.release.yaml -f compose.vm.yaml \
  run --rm --no-deps caddy caddy validate --config /etc/caddy/Caddyfile --adapter caddyfile
docker compose -f compose.yaml -f compose.oauth.yaml -f compose.release.yaml -f compose.vm.yaml up -d
curl --fail https://analytics.example.com/readyz
curl --fail https://analytics.example.com/.well-known/oauth-authorization-server
```

Replace the example hostname in the checks. Keep Caddy's volumes across restarts so certificate state persists. The laptop-only `hello.sh` deliberately refuses a public issuer; use the [OAuth](oauth.md) and [analytics](analytics.md) requests for remote setup and verification.

## Persistent storage and resources

The image runs as UID/GID 65532. Named volumes initialize from its writable `/data` directory. A bind-mounted directory needs ownership and permissions that let this user write. Keep SQLite, the DuckLake registry, catalogs, and data files together. Do not share them between simultaneous writers or use a network filesystem as the local catalog directory.

The default Compose server has a 2 GiB container memory limit. Two CPU cores and 2 GiB are the resource budget used for local architecture tests, not a hosted capacity guarantee. Leave additional memory for the operating system, Docker, and a proxy. A 4 GiB VM provides more headroom for a small deployment. Measure your event sizes, active projects, concurrent queries, and maintenance before choosing capacity.

DuckDB's memory setting is per resident project engine and is shared by that engine's work. It does not cap total process RSS. The number of resident project engines, their memory settings, native allocations, SQLite, and the Go heap all contribute to container memory. A container limit can terminate the process under pressure; durable accepted work retries after restart.

Native resource settings are deployment controls:

| Setting | Default | Accepted range |
| --- | --- | --- |
| `PORCUPINE_DUCKLAKE_MEMORY_MB` | 512 MiB | 64 through 32,768 MiB per resident project engine |
| `PORCUPINE_DUCKLAKE_THREADS` | 2 | 1 through 4 per engine |
| `PORCUPINE_DUCKLAKE_MAX_OPEN_PROJECTS` | 4 | 1 through 64 resident engines |
| `PORCUPINE_DUCKLAKE_MAINTENANCE_INTERVAL` | 2 seconds | 1 second through 24 hours |

These ranges validate configuration; they are not safe combined sizing recommendations. Keep the total engine budgets below the host's available memory with room for other allocations.

Maintenance leaves a two-second gap after each pass by default. It flushes small inlined writes and uses native size-filtered compaction. It runs concurrently with event projection; ordinary event writes remain serialized to preserve revisions and retry IDs. These short passes group small files toward an 8 MB target. A separate hourly cleanup cycle expires historical snapshots and reclaims obsolete files, including in cold project catalogs. See the [cleanup policy and controls](ducklake-cleanup.md) and [maintenance measurements](ducklake-maintenance.md).

Use the deployment's `PORCUPINE_DUCKLAKE_DIR` for the managed native directory. The default image uses `/data/ducklake`; preserve it with SQLite. The default original-event queue is bounded by 100,000 events and 256 MiB of pending payloads. These are queue budgets, not total disk limits. Receipts, SQLite overhead and WAL, snapshots, temporary work, and Parquet history require additional space.

Live events have no automatic age-based deletion. Historical snapshots default to seven days, followed by a two-day obsolete-file grace period; active readers can extend these windows. SQLite keeps permanent replay receipts. Monitor free disk, pending-event age, pending mutations, delivery errors, memory pressure, and maintenance.

`/readyz` checks SQLite, not the analytical catalogs or a drained queue. Use [instance status](instance-status.md), capture a synthetic event, and verify SQL visibility before routing traffic.

### Unreleased interactive query ceilings

These settings belong to the unreleased [tenant implementation](tenants.md), not the published v0.14.0 image. They set deployment-wide safety ceilings. Tenant budgets can lower them, but cannot raise them.

| Setting | Default | Accepted range |
| --- | --- | --- |
| `PORCUPINE_QUERY_MAX_CONCURRENT` | 2 | 2–64 deployment analytical places |
| `PORCUPINE_QUERY_TIMEOUT_MS` | 5,000 | 1–60,000 ms |
| `PORCUPINE_QUERY_MAX_ROWS` | 1,000 | 1–100,000 rows |
| `PORCUPINE_QUERY_MAX_RESULT_BYTES` | 1,048,576 | 256–67,108,864 bytes |

The concurrency pool also retains the existing background export safety slot; tenant interactive budgets do not create additional deployment capacity. These settings do not change DuckDB's per-engine memory budget or provide process isolation. Increase them only within the host's measured resource capacity. An unset value uses the default. Query readers discover their effective limits through `get-event-schema`.

Each fresh installation starts with one default tenant and unlimited monthly ingestion. Only an instance operator can create additional tenants or set hard budgets. A tenant administrator can inspect their own tenant's configuration and aggregate usage, without changing those operator limits. This works in the default single-process SQLite mode and optional [shared mode](shared-deployment.md). In shared mode, query places are reserved across workers, and each process retains its own local resource ceiling.

Shared PostgreSQL analytical metadata also uses `PORCUPINE_DUCKLAKE_METADATA_TIMEOUT_SECONDS`: default 10, range 2–60 seconds. This setting applies to native connection, statement, lock, and TCP waits and is not accepted for local catalogs. `get-event-schema` reports `limits.metadata_request_timeout_seconds`. Some native network calls can outlast cancellation; the setting does not guarantee that every query timeout returns an HTTP error immediately.

Supervise shared workers and restart them with the same SQLite volume and worker identity. If a native interactive execution exceeds its SQL deadline, S3 and PostgreSQL I/O allowances, and a 15-second safety margin, the process exits with status `70`. This happens before its shared reservation's later expiry can admit a replacement. The whole process stops, so clients may need stable-ID retries. Default local mode has no shared watchdog. See [shared execution and status](shared-deployment.md#native-timeouts-and-worker-supervision) for the failure contract and the `shared_control`/`worker_registration` status checks.

## Optional destinations

| Capability | Configuration |
| --- | --- |
| One-time SQL downloads | Add `compose.exports.yaml` with a private export bucket and prefix |
| Webhooks and scheduled S3 query pipelines | Set a stable `PORCUPINE_PIPELINE_KEY` or mounted key file, then create project-scoped destinations |
| Human sign-in | Add the OAuth and email overlays with an SMTP provider and initial admin email |

Keep the release and OAuth overlays when adding another overlay. Pipeline key files need an explicit container mount; a host pathname alone does not mount the file. Keep encryption keys privately with deployment recovery material. [Exports](exports.md) · [Pipelines](pipelines.md) · [Team sign-in](connect-your-team.md)

S3 destinations store query results. For analytical files, a fresh DuckLake deployment can use the [native S3 option](ducklake-s3.md). SQLite and project catalogs stay local. This does not migrate an existing SSD archive or apply a hot-to-cold age policy. [Storage options](storage.md)

## Authentication, backups, and upgrades

Keep the original bootstrap secret privately for recovery. Changing `.env` after initialization does not replace the stored credential. Humans authorize their CLI or MCP tools using email verification or optional upstream OIDC. Backends and shared agents can use scoped service keys or OAuth client credentials. [Connect your team](connect-your-team.md) · [Service keys](service-keys.md)

Before storing important history, configure [scheduled native backups](native-backups.md) or exercise a [coordinated stopped-volume backup and restore](restore.md). Preserve the complete data volume, private configuration, pipeline encryption key, and matching image version off the host. A live file copy or query-result export is not a consistent instance checkpoint.

Before upgrading, record the image digest and overlay list, stop the application, and take a complete matching checkpoint. Replace the single writer with the new image using the same volume and overlays. Review release notes before reopening traffic. Early releases do not promise online schema upgrades or cross-version rollback.

**Unreleased tenant schema:** start with an empty data directory. The server rejects pre-tenant SQLite databases rather than assigning existing projects automatically. There is no in-place migration or compatibility mode for this pre-launch change. Keep an older instance and its matching recovery material separately if you need to export data into a fresh tenant-enabled instance.

Version v0.14.0 supports only DuckDB/DuckLake. It refuses legacy analytical configuration rather than silently creating a fresh archive. An earlier external-database deployment needs a fresh native target and an explicit export/import migration. Keep its matching backup until validation succeeds. A changed image tag alone does not convert stored history or SQL. See [storage and migration boundaries](storage.md#earlier-analytical-backends).
