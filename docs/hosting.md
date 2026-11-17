# Host Porcupine 0.14.0

For the published [v0.14.0 release](https://github.com/olup/porcupine/releases/tag/v0.14.0), start with the [quickstart](../README.md#quickstart). It starts the matching image, sends a test event, and waits for SQL visibility. The [release hosting reference](reference/hosting-options.md) covers HTTPS, offline images, and resource settings for that tagged checkout.

The configuration-file examples below are **Unreleased**. They require a current source build and fresh storage. Keep the server and CLI versions matched.

## Pick a deployment

| Goal | Recommended starting point |
| --- | --- |
| Try it on a laptop | One process, local SQLite and DuckLake, loopback HTTP |
| Run on one VM | The same local storage, an HTTPS proxy, and independent backups |
| Keep analytical files on S3 | The VM setup with an existing private bucket and a dedicated prefix |
| Run several workers | [Shared PostgreSQL deployment](reference/shared-deployment.md), with a durable local journal per worker |

Start with one server and local files. PostgreSQL is not required for tenants, people, budgets, the API, CLI, or MCP. [Storage](storage.md) describes the state owned by SQLite, DuckLake, PostgreSQL, and S3.

## Configure the development build

Build and initialize the server executable. These setup commands are separate from the standalone API client:

```sh
make build
./bin/porcupine init --config porcupine.json
```

`init` generates the private bootstrap and pipeline secrets. Keep them. Choose one of the examples below, then merge it into the generated file with `jq`:

```sh
jq -s '.[0] * .[1]' porcupine.json docs/examples/configuration/laptop.json |
	./bin/porcupine config install --config porcupine.json
```

Replace `laptop.json` with `vm.json` or `vm-s3.json` for the other profiles. The examples contain no secrets. Merging preserves the generated storage paths and credentials. Change the example hostname and bucket values in your private `porcupine.json` before starting a public deployment.

Validate the edited file, then start the server:

```sh
./bin/porcupine config check --config porcupine.json
./bin/porcupine --config porcupine.json
```

`config check` validates local settings without opening databases or contacting providers. Its output masks secrets. It does not test provider reachability. The [configuration reference](reference/configuration.md) has the loading rules, settings index, and container setup and replacement commands.

### Laptop

[laptop.json](examples/configuration/laptop.json) sets the OAuth issuer to `http://localhost:8080`.

The generated native configuration keeps the HTTP listener on `127.0.0.1:8080` and data under `./data`. No PostgreSQL, S3, or email provider is needed for a local test using the bootstrap credential. To connect people through OAuth, configure [team sign-in](reference/connect-your-team.md).

### One VM

[vm.json](examples/configuration/vm.json) sets the public issuer to `https://analytics.example.com` and trusts a proxy on the same host.

Keep the native listener on loopback. Point an HTTPS proxy at `127.0.0.1:8080`. For example, a host-installed Caddy can use:

```caddyfile
analytics.example.com {
	reverse_proxy 127.0.0.1:8080 {
		header_up Host 127.0.0.1:8080
	}
}
```

Use your hostname in both the issuer and proxy configuration. Allow public ports 80 and 443 to the proxy and keep 8080 private. The fixed upstream Host preserves MCP's loopback-host check. If the proxy runs in another container or behind a CDN, configure its actual trusted address instead of copying this loopback example. The [VM reference](reference/hosting-options.md#single-linux-vm-with-https) includes the Docker proxy variant.

Run the server under a supervisor and preserve its data directory and private configuration. Configure [backups](backup-and-restore.md) before storing important history.

### One VM with analytical S3

[vm-s3.json](examples/configuration/vm-s3.json) adds an analytical bucket, prefix, and region to the VM profile.

Use an existing private bucket and its actual region. Supply the server's credentials through the AWS credential chain, or configure the explicit access-key pair privately. Select S3 before starting a fresh archive; editing these fields does not move an existing local archive. SQLite and DuckLake catalogs remain on persistent local storage.

Analytical S3 is separate from export results, pipeline destinations, and backups. Use an independent [S3 backup destination](reference/native-backup-recovery.md) to recover the complete instance. [Analytical S3 settings and permissions](reference/ducklake-s3.md)

## Choose local SQLite or shared PostgreSQL (unreleased)

The same binary supports both modes. Keep the default local mode for one server. Shared mode adds PostgreSQL control and catalog metadata, S3 Parquet, and stable worker identities. Each project still depends on its owner's local journal and volume; there is no automatic owner failover. The [storage mode comparison](storage.md#unreleased-separate-control-and-journal) and [shared setup](reference/shared-deployment.md) describe the boundary and required settings.

A fresh development installation creates tenant `default`. Connect the instance operator and [invite its first administrator](access.md).

## Keep data and resources safe

Run one server process per local data directory. Preserve both SQLite databases and their auxiliary files with DuckLake and private configuration. Do not share local catalogs or SQLite files between writers or use a network filesystem for them. The container runs as user 65532; a bind mount must let that user read private configuration and write data.

The default Compose memory limit is 2 GiB, a test profile rather than a capacity guarantee. DuckDB's per-project memory setting does not cap process RSS. Size for resident engines, queries, ingestion, maintenance, and the operating system. [Resource settings](reference/hosting-options.md#persistent-storage-and-resources)

`/readyz` checks SQLite. Before routing traffic, check the authenticated [instance report](reference/instance-status.md), capture a synthetic event, and verify SQL visibility. Monitor pending-event age, queue bytes, disk, memory, and delivery errors.

## Configure access and backups

Configure [email or Google/OIDC sign-in](reference/connect-your-team.md), then [invite people and create service credentials](access.md). Keep the bootstrap secret privately.

Record the image digest and private configuration before upgrading, and take a complete checkpoint. Cross-version rollback is not guaranteed. The unreleased tenant-enabled layout requires fresh storage; recognized later versions use numbered migrations and reject unfamiliar or newer schemas before writes. [Storage versions](storage.md#unreleased-schema-versions)
