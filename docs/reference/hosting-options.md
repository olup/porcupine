# Hosting options

Use the [quickstart](../../README.md#quickstart) to start a new v0.15.1 instance. The release provides server images for Linux amd64 and arm64 and standalone CLI archives for macOS, Linux, and Windows. The server bundles its compatible DuckLake, HTTPFS, AWS, and PostgreSQL extensions.

The release overlay selects the tagged image and removes the source build. Keep it after `compose.yaml`. Set `PORCUPINE_IMAGE` to an explicit digest when pinning a deployment.

## Load an image without registry access

Choose the Docker server's architecture and verify the archive before loading it:

```sh
case "$(docker info --format '{{.Architecture}}')" in
  amd64|x86_64) PORCUPINE_RELEASE_ARCH=amd64 ;;
  arm64|aarch64) PORCUPINE_RELEASE_ARCH=arm64 ;;
  *) echo 'Unsupported release architecture' >&2; exit 1 ;;
esac
PORCUPINE_RELEASE_ARCHIVE="porcupine-0.15.1-linux-$PORCUPINE_RELEASE_ARCH.tar.gz"
PORCUPINE_RELEASE_URL=https://github.com/olup/porcupine/releases/download/v0.15.1
curl --fail --location --output "$PORCUPINE_RELEASE_ARCHIVE" "$PORCUPINE_RELEASE_URL/$PORCUPINE_RELEASE_ARCHIVE"
curl --fail --location --output SHA256SUMS "$PORCUPINE_RELEASE_URL/SHA256SUMS"
expected=$(awk -v name="$PORCUPINE_RELEASE_ARCHIVE" '$2 == name { print $1 }' SHA256SUMS)
actual=$(openssl dgst -sha256 "$PORCUPINE_RELEASE_ARCHIVE" | awk '{ print $NF }')
test -n "$expected" && test "$actual" = "$expected" || {
    echo 'Archive checksum mismatch' >&2
    exit 1
}
docker load -i "$PORCUPINE_RELEASE_ARCHIVE"
docker compose -f compose.yaml -f compose.release.yaml --profile setup run --rm init
docker compose -f compose.yaml -f compose.release.yaml up -d porcupine
```

The server image includes its native extension. An offline host also needs any optional proxy image transferred separately.

## Single Linux VM with HTTPS

Follow the [VM deployment guide](../hosting.md#one-vm) for TLS, a trusted proxy, and a public OAuth issuer. Run the server under a supervisor. To deploy with Docker, use the [configuration file and container setup](configuration.md#container-setup). If the proxy runs in another container, put it on the same private Docker network as Porcupine. Point it at the server service, configure its address in `trusted_proxies`, and forward the fixed upstream Host shown in the VM guide. Publish ports 80 and 443 only on the proxy.

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

Maintenance leaves a two-second gap after each pass by default. It flushes small inlined writes and uses native size-filtered compaction. It runs concurrently with event projection; ordinary event writes remain serialized to preserve revisions and retry IDs. These short passes group small files toward an 8 MB target. A separate hourly cleanup cycle expires historical snapshots and reclaims obsolete files, including in cold project catalogs. See the [cleanup policy and controls](ducklake-cleanup.md) and [maintenance measurements](../design/ducklake-maintenance.md).

Use the deployment's `PORCUPINE_DUCKLAKE_DIR` for the managed native directory. The default image uses `/data/ducklake`; preserve it with SQLite. The default original-event queue is bounded by 100,000 events and 256 MiB of pending payloads. These are queue budgets, not total disk limits. Receipts, SQLite overhead and WAL, snapshots, temporary work, and Parquet history require additional space.

Live events have no automatic age-based deletion. Historical snapshots default to seven days, followed by a two-day obsolete-file grace period; active readers can extend these windows. SQLite-journal deployments keep replay receipts permanently by default. They support [optional receipt pruning](receipts.md#bounded-receipts); review its acceptance tradeoff before enabling it. Monitor free disk, pending-event age, pending mutations, delivery errors, memory pressure, and maintenance.

`/readyz` checks the operational store. Verify the analytical path separately. Use [instance status](instance-status.md), capture a synthetic event, and verify SQL visibility before routing traffic.

### Interactive query ceilings {#unreleased-interactive-query-ceilings}

The [configuration reference](configuration.md#interactive-query-ceilings) owns the JSON keys, environment equivalents, defaults, and accepted ranges. [Tenant budgets](tenants.md#interactive-query-limits) describes effective limits and permissions. [Hosted execution and supervision](shared-deployment.md#native-timeouts-and-worker-supervision) owns PostgreSQL metadata timeouts, reservation renewal, automatic expiry, and project quarantine.

## Optional destinations

Configure the `export.s3`, `ducklake.s3`, and `backup.s3` destinations separately. A stable private pipeline key encrypts destination credentials. Mount secret files explicitly where the server can read them. [Server configuration](configuration.md)

## Backups and upgrades

Take a complete checkpoint and record the image digest and private configuration. For a v0.14.0 deployment, follow the [fresh-storage cutover procedure](../hosting.md#move-from-v0140). For a recoverable checkpoint, follow the [backup guide](../backup-and-restore.md).
