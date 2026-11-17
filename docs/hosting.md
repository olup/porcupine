# Host Porcupine 0.15.0

For the [v0.15.0 release](https://github.com/olup/porcupine/releases/tag/v0.15.0), start with the [quickstart](../README.md#quickstart). It starts the matching image, sends a test event, and waits for SQL visibility. The [release hosting reference](reference/hosting-options.md) covers HTTPS, offline images, and resource settings for that tagged checkout.

Use a fresh data directory for a new installation. Keep the server and CLI versions matched. For an existing instance, follow the [upgrade steps](#move-from-v0140).

## Pick a deployment

| Goal | Recommended starting point |
| --- | --- |
| Try it on a laptop | One process, local SQLite and DuckLake, loopback HTTP |
| Run on one VM | The same local storage, an HTTPS proxy, and independent backups |
| Keep analytical files on S3 | The VM setup with an existing private bucket and a dedicated prefix |
| Run several workers | [Shared PostgreSQL deployment](reference/shared-deployment.md), with a durable local journal per worker |

Start with one server and local files. PostgreSQL is not required for tenants, people, budgets, the API, CLI, or MCP. [Storage](storage.md) describes the state owned by SQLite, DuckLake, PostgreSQL, and S3.

<a id="configure-the-development-build"></a>

## Configure the server

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

<a id="choose-local-sqlite-or-shared-postgresql-unreleased"></a>

## Choose a storage mode

Standalone keeps the journal and control databases on the local volume. Owner-routed shared mode adds PostgreSQL control and catalog metadata, S3 Parquet, and stable worker identities. Each project depends on its owner’s local journal and volume for accepted data until analytical projection. [Owner-routed setup](reference/shared-deployment.md)

The opt-in hosted profile uses PostgreSQL and S3 with disposable worker storage. Live acknowledgment covers admission to bounded worker memory. A crash or incomplete drain can lose acknowledged events that have yet to commit, and the flush interval does not bound loss during an outage. Start with a fresh PostgreSQL database and dedicated analytical S3 prefix. Configure and exercise PostgreSQL and S3 recovery before admitting traffic. [Hosted setup and recovery requirements](design/hosted-profile-composition.md)

A fresh installation creates tenant `default`. Connect the instance operator and [invite its first administrator](access.md).

## Provision a tenant from a private service

For one team, keep the built-in `default` tenant and use [team setup](reference/tenant-access.md). If you manage several teams from your own application, the matching server exposes the same operator API used by the standalone CLI. Configure the OAuth issuer or [external authentication](reference/external-auth.md) first.

1. As the instance operator, use `create-oauth-client` to create a machine account with `permissions: ["instance:admin"]` and `projects: ["*"]`. Issue its key with `create-service-api-key`, the same grant, and `audience: "api"`. Store the returned one-time token in the private service's secret store. Send it as an API bearer credential. [Machine credentials](reference/service-keys.md)
2. From the service, call `POST /api/v1/tenants` with the team's name. Save the returned `tenant.id` in your application's customer record. If the team needs limits, send the complete policy to `PUT /api/v1/tenants/{tenant_id}/budgets`. [Tenant budgets](reference/tenants.md)
3. Call `POST /api/v1/projects` with the project name and `tenant_id`. Give producers the project's ingestion credential, or issue a scoped team service key for a private producer. Give dashboards a separate project-scoped reader key. [Project ingestion modes](reference/projects.md)
4. Read the month's allowance through `GET /api/v1/tenants/{tenant_id}/usage`. Use `GET /api/v1/usage` for the operator's paginated numerical report. Keep customer records and billing in your application. [Numerical usage and its limits](reference/usage.md)
5. To remove access, revoke the relevant key through its management API. Rotate a provisioner key by deploying a replacement before revoking the old key with `revoke-access-key`. [Credential rotation](reference/service-keys.md)

Inspect the running instance's `/openapi.json` for request and response schemas. To exercise provisioning, tenant isolation, and restart recovery in a disposable shared deployment, run the [two-worker check](reference/shared-deployment.md#exercise-provisioning-and-owner-restart).

## Keep data and resources safe

Run one server process per local data directory. Preserve both SQLite databases and their auxiliary files with DuckLake and private configuration. Do not share local catalogs or SQLite files between writers or use a network filesystem for them. The container runs as user 65532; a bind mount must let that user read private configuration and write data.

The default Compose memory limit is 2 GiB, a test profile rather than a capacity guarantee. DuckDB's per-project memory setting does not cap process RSS. Size for resident engines, queries, ingestion, maintenance, and the operating system. [Resource settings](reference/hosting-options.md#persistent-storage-and-resources)

`/readyz` checks the operational store. Before routing traffic, check the authenticated [instance report](reference/instance-status.md), capture a synthetic event, and verify SQL visibility. Monitor pending-event age, queue bytes, disk, memory, and delivery errors.

## Configure access and backups

Configure [email or Google/OIDC sign-in](reference/connect-your-team.md), then [invite people and create service credentials](access.md). Keep the bootstrap secret privately.

## Move from v0.14.0

v0.15.0 introduces separate acceptance and control databases and tenant ownership. Start it with fresh storage. Keep the v0.14.0 image digest, complete data and backup bundles, overlay files, and private configuration together for recovery. Restore v0.14.0 bundles with the matching v0.14.0 tools. An image swap on the old volume cannot migrate it.

1. Pause producers at cutover and wait for pending writes to become queryable. Take a complete v0.14.0 checkpoint before changing credentials or routes.
2. Initialize v0.15.0 in a separate volume or directory. Transfer required history with an explicit export and [historical import](imports.md), preserving original timestamps and stable producer IDs where available. Imports carry events. Recreate accounts, grants, credentials, and destination settings separately.
3. Update clients to match the server. Remove the `use_cache` request field. Replace old Compose overlays with the [server configuration](reference/configuration.md) and review settings before enabling pipelines.
4. Verify counts, event times, identity behavior, corrections and deletes, then switch producers and readers. Keep the old instance isolated with its recovery material until the new deployment is validated. Returning to the old instance requires reconciling writes accepted after cutover.

Moving an owner-routed deployment to the hosted profile also requires fresh PostgreSQL metadata and a fresh S3 destination. Preserve the owner journals until their accepted work has reached DuckLake and you have verified the data you intend to transfer.
