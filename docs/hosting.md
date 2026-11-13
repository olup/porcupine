# Host Porcupine 0.10.0

The server runs one Go process with a persistent SQLite volume. ClickHouse stores analytical history on a private network. Start with Docker Compose on a laptop, add HTTPS on a single VM, or connect the application to an existing ClickHouse server.

Use one application writer per SQLite database on local persistent storage. Multiple projects share that instance. Host failure requires restoring a checkpoint or recovering the host.

The [v0.10.0 release](https://github.com/olup/porcupine/releases/tag/v0.10.0) contains two Linux server image archives and six standalone CLI archives for macOS, Linux, and Windows on amd64 and arm64. Install the [CLI](../cli/README.md) on the machine that will call the HTTP API. Use the server image for the deployment.

## Laptop with Docker Compose

Install the [standalone CLI](../cli/README.md), Git, curl, OpenSSL, jq, Docker, and Docker Compose 2.24.4 or newer. Docker Desktop supplies Docker and Compose on macOS and Windows. Use a POSIX shell for the commands below, such as a WSL shell with Docker integration on Windows. Start in the directory where you want to clone Porcupine.

```sh
git clone https://github.com/olup/porcupine.git
cd porcupine
umask 077
(
    set -eu
    umask 077
    set -C
    bootstrap_token=$(openssl rand -hex 32)
    clickhouse_password=$(openssl rand -hex 32)
    cat > .env <<EOF
PORCUPINE_BOOTSTRAP_TOKEN=$bootstrap_token
PORCUPINE_CLICKHOUSE_PASSWORD=$clickhouse_password
PORCUPINE_OAUTH_ISSUER=http://localhost:8080
EOF
)
docker compose -f compose.yaml -f compose.oauth.yaml -f compose.release.yaml up -d
curl --fail --silent --show-error --retry 30 --retry-all-errors --retry-delay 1 http://localhost:8080/readyz
sh examples/hello.sh
```

The checkout provides the current guides and shell examples. The release overlay pins the server to v0.10.0.

The setup refuses to overwrite an existing `.env`. Preserve it privately. The example exchanges the bootstrap credential, creates a project, captures a cookieless event, and waits for SQL to return `hello` with total `"1"`. Only the project ID and result are printed. Each run creates another project. An instance administrator can remove the example with the printed project ID using the [project management API](management-api.md#operations).

`compose.release.yaml` must follow the base file. It removes the source build and defaults to `ghcr.io/olup/porcupine:0.10.0`. Set `PORCUPINE_IMAGE` to another explicit tag, digest, or locally loaded image when required. The overlay pulls only when that image is missing. Its reset syntax uses [Compose merge rules](https://docs.docker.com/reference/compose-file/merge/).

Only `127.0.0.1:8080` is published. ClickHouse has no host port. Named volumes preserve `/data/porcupine.db` and `/var/lib/clickhouse` through container replacement:

```sh
docker compose -f compose.yaml -f compose.oauth.yaml -f compose.release.yaml ps
docker compose -f compose.yaml -f compose.oauth.yaml -f compose.release.yaml stop
```

`stop` keeps containers and volumes. `down` also keeps volumes unless `--volumes` is added. Do not use `--volumes` on data you intend to retain. Keep the same Compose project name and directory when returning to an existing deployment.

### Load a release image without registry access

Select the Docker server's architecture. Download the `porcupine-0.10.0-linux-*` archive for that architecture, verify its checksum, then load it.

```sh
case "$(docker info --format '{{.Architecture}}')" in
  amd64|x86_64) PORCUPINE_RELEASE_ARCH=amd64 ;;
  arm64|aarch64) PORCUPINE_RELEASE_ARCH=arm64 ;;
  *) echo 'Unsupported release architecture' >&2; exit 1 ;;
esac
PORCUPINE_RELEASE_ARCHIVE="porcupine-0.10.0-linux-$PORCUPINE_RELEASE_ARCH.tar.gz"
PORCUPINE_RELEASE_URL=https://github.com/olup/porcupine/releases/download/v0.10.0
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

The archive retains the release tag, so the same overlay uses it. [Docker load](https://docs.docker.com/reference/cli/docker/image/load/) accepts compressed archives and restores image tags. A fully offline host also needs the pinned ClickHouse image and any optional proxy images transferred separately.

## Single Linux VM with HTTPS

Use the same Compose deployment on a Linux VPS or Lightsail instance. Install Docker and Compose first. Create a DNS record for your own hostname pointing to the VM. Allow inbound TCP 80 and 443 to the proxy, restrict SSH to your administrative access, and keep API port 8080 and ClickHouse ports closed publicly.

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

## Application container with a separate ClickHouse service

Use this instead of the base Compose application when ClickHouse has a separate lifecycle. Keep one application container attached to its SQLite volume. The dedicated ClickHouse provisioning user inserts events and manages the database objects, users, grants, views, and row policies used for project isolation. Project deletion also needs permission to inspect and mutate the shared event table, terminate the project's active reader queries, and drop its readers, policies and namespace. Application OAuth clients do not receive those database credentials.

The following runnable example starts a separate ClickHouse container on a private Docker network. It assumes the laptop `.env` already exists and port 8080 is free. Its volumes are distinct from the Compose volumes:

```sh
umask 077
(
    set -eu
    umask 077
    set -C
    . ./.env
    cat > .env.warehouse <<EOF
CLICKHOUSE_USER=porcupine_admin
CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1
CLICKHOUSE_PASSWORD=$PORCUPINE_CLICKHOUSE_PASSWORD
EOF
)
docker network create porcupine-private
docker volume create porcupine-warehouse-data
docker volume create porcupine-standalone-data
docker run -d --name porcupine-warehouse --restart unless-stopped \
  --network porcupine-private --network-alias warehouse \
  --env-file .env.warehouse --memory 2g \
  --mount type=volume,source=porcupine-warehouse-data,target=/var/lib/clickhouse \
  clickhouse/clickhouse-server:26.8.36.3
docker run -d --name porcupine-app --restart unless-stopped \
  --network porcupine-private --publish 127.0.0.1:8080:8080 \
  --env-file .env \
  --env PORCUPINE_ADDR=0.0.0.0:8080 --env PORCUPINE_DB=/data/porcupine.db \
  --env PORCUPINE_CLICKHOUSE_URL=http://warehouse:8123 \
  --env PORCUPINE_CLICKHOUSE_DATABASE=porcupine \
  --env PORCUPINE_CLICKHOUSE_USER=porcupine_admin \
  --mount type=volume,source=porcupine-standalone-data,target=/data \
  "${PORCUPINE_IMAGE:-ghcr.io/olup/porcupine:0.10.0}"
curl --fail --silent --show-error --retry 30 --retry-all-errors --retry-delay 1 http://localhost:8080/readyz
sh examples/hello.sh
```

For an existing remote warehouse, omit the example ClickHouse container and set `PORCUPINE_CLICKHOUSE_URL` to its reachable private address or trusted HTTPS endpoint. Set its database, username, and private password separately. Do not publish ClickHouse to the internet to make this work. A remote deployment still needs network policy and TLS appropriate to that connection.

The stock application image runs as UID/GID 65532. Named volumes initialize from its writable `/data` directory. A bind-mounted directory instead needs ownership and permissions that let that user write. Do not mount the same SQLite database into a second writer or put it on a network filesystem.

## Resource settings and scope

The base Compose file caps ClickHouse at 2 GiB and leaves the Go container uncapped. For explicit limits, create `compose.resources.yaml` and include it in every deployment command:

```yaml
services:
  porcupine:
    cpus: 1.0
    mem_limit: 512m
  clickhouse:
    cpus: 2.0
    mem_limit: 2g
```

These match the container limits used by the [local reference benchmark](benchmarks.md). They are not a capacity guarantee or a minimum-memory claim. A development Docker VM needs room beyond those limits for Docker and the operating system. A small Linux VM with 4 GiB provides headroom for this example; larger queries, exports, merges, backups, and long retention need measurement on the actual host.

The default original-event queue is bounded by 100,000 events and 256 MiB of pending payloads. These are not total disk limits. SQLite overhead, WAL, receipts, mutation state, and analytical history require additional space. Default receipts persist indefinitely; [bounded receipts](receipts.md) are optional. History has no age-based DELETE TTL. Monitor free disk, queue lag, delivery errors, memory pressure, and backup duration rather than sizing only from event count.

`/readyz` checks SQLite readiness, not ClickHouse health or a drained queue. Verify capture, SQL visibility, and project delivery state before routing traffic. A ClickHouse outage can fill the bounded local queue; new capture then receives backpressure.

## Optional destinations and cold storage

| Capability | How to enable | What remains your responsibility |
| --- | --- | --- |
| Native ClickHouse cold storage | Add `compose.s3.yaml` and its operator settings | Existing bucket/prefix, credentials, local disk metadata, backups; same SQL queries |
| One-time SQL downloads | Add `compose.exports.yaml` with a separate private export destination | Bucket policy, multipart cleanup, object lifecycle, expiring download access |
| Webhook and scheduled S3 pipelines | Set a stable `PORCUPINE_PIPELINE_KEY` or mounted key file, then use scoped APIs | Destination credentials, receiver deduplication, bucket retention, restore review |

Keep the release and OAuth overlays when adding an optional overlay. Native cold storage moves eligible parts after the default enabled 30-day hot period; it does not expire total history. One-time exports and pipelines do not change that storage policy. Keep their object prefixes separate. See [storage](storage.md), [exports](exports.md), and [pipelines](pipelines.md) for exact configuration and failure contracts. Pipeline key files need an explicit container mount; setting a host pathname alone does not mount it.

## Authentication, backups, and upgrades

Keep the original bootstrap secret privately for recovery. Exchange it as OAuth client `bootstrap` to provision the instance. Changing `.env` on an initialized deployment leaves the original credential in effect.

For human access to the CLI or MCP, configure [email sign-in and the initial administrator](connect-your-team.md), or configure an [OIDC provider for interactive clients](mcp-interactive-oauth.md). The laptop quickstart works with machine credentials alone. A backend or shared agent can use a [project-scoped service key](service-keys.md) or exchange its own OAuth client credentials. Each bearer credential has an API or MCP audience.

For browser capture, allow the exact browser origins and issue a single-project ingestion key. For trusted backends, create a `server_only` project. See the [ingestion policy](oauth.md#project-ingestion-policy).

Before storing important history, configure and exercise [paired online checkpoints](online-backup.md). The [backup Compose overlay](backup-compose.md) supplies shared SQLite lock-file mounts and a ClickHouse backup disk. Add it to the same complete overlay set used by the application. Preserve the SQLite bundle, native archive, private deployment configuration, software version, and pipeline encryption key separately off the host. A live volume copy, S3 bucket listing, or export is not that checkpoint.

Before upgrading, take a verified checkpoint, record the current image and full overlay list, and review schema changes. Change `PORCUPINE_IMAGE` to the intended release tag or digest, pull it, and replace the single application container with that same overlay list. Do not start old and new writers together. Preserve both database volumes and proxy state. Early releases do not promise online schema upgrades or cross-version rollback.

If only the container changed and storage remains compatible, an earlier image may be usable after review. If schema or data changed, restore a matching checkpoint into isolated storage with its matching software version instead of assuming that changing an image tag undoes migration. Restore invalidates saved access tokens and export jobs by default and pauses all pipelines. Review checkpoint-time credentials, replay later revocations, and reconcile external delivery before explicitly resuming traffic or destinations.

## Verification of these recipes

Local checks exercised the release overlay with a source-built image override, the documented Compose/OAuth flow and `hello.sh`, restart persistence, and a standalone image with private ClickHouse 26.8.36.3. A Caddy reverse-proxy check verified TLS using a local test CA, fixed issuer metadata, capture and SQL, and MCP initialization. Its proxy used the same fixed trusted address and upstream Host configuration.

The v0.9.0 release was also deployed on a single AWS Lightsail VM with an existing ClickHouse and HTTPS proxy. The published CLI renamed a synthetic project and an MCP client deleted it. The final project inventory matched its pre-upgrade contents. The [release workflow](https://github.com/olup/porcupine/actions/runs/42829136190) built the server images, packaged both CLI architectures for each OS, and ran the native CLI on each OS.
