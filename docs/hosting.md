# Host Porcupine

Start a new instance with the [quickstart](../README.md#quickstart) and install the CLI from the [current release](https://github.com/olup/porcupine/releases/tag/v0.18.0). If you already run Porcupine, check the [upgrade steps](#upgrade) first and match the CLI to your server.

<a id="choose-local-sqlite-or-shared-postgresql-unreleased"></a>
<a id="choose-a-storage-mode"></a>

## Pick a deployment

- For a laptop, start with the [local quickstart](../README.md#local-setup).
- For a standalone VM, configure [HTTPS and persistent storage](#one-vm). To store analytical files in S3, start with the [S3 profile](#one-vm-with-analytical-s3).
- For replaceable workers behind an ingress, follow the [hosted setup and recovery runbook](design/hosted-architecture.md#configuration).

See [storage and acknowledgment by profile](storage.md#deployment-profiles) and [offline image loading](reference/hosting-options.md#load-an-image-without-registry-access).

<a id="configure-the-development-build"></a>

## Configure the server

For a native setup, build the matching version and initialize its private configuration.

```sh
make build
./bin/porcupine init --config porcupine.json
```

Copy the [configuration example](#laptop) for your deployment to `profile.json` and set its hostname and bucket where applicable. Merge that file into the generated configuration to preserve its secrets and storage paths.

```sh
jq -s '.[0] * .[1]' porcupine.json profile.json |
	./bin/porcupine config install --config porcupine.json
./bin/porcupine config check --config porcupine.json
./bin/porcupine --config porcupine.json
```

See [server configuration](reference/configuration.md) for validation and container setup.

### Laptop

[laptop.json](examples/configuration/laptop.json) sets the OAuth issuer to `http://localhost:8080`. The native defaults listen on `127.0.0.1:8080` and store data under `./data`. Use the generated bootstrap credential for a local test.

### One VM

[vm.json](examples/configuration/vm.json) sets a public HTTPS issuer and trusts a proxy on the same host. Follow the [VM deployment steps](reference/hosting-options.md#single-linux-vm-with-https).

### One VM with analytical S3

Use [vm-s3.json](examples/configuration/vm-s3.json) to [configure a fresh archive on S3](reference/ducklake-s3.md#configure-a-new-deployment).

## Configure access and backups

[Set up the first tenant administrator](reference/tenant-access.md) with builtin sign-in, or configure [external authentication](reference/external-auth.md). Follow the [backup guide](backup-and-restore.md) before storing production data.

## Provision a tenant from a private service

Follow [tenant provisioning](reference/tenants.md#provision-a-tenant-from-a-private-service) if you manage teams from your own application.

## Keep data and resources safe

Review [persistent storage and memory settings](reference/hosting-options.md#persistent-storage-and-resources). Follow the [readiness and delivery checks](reference/instance-status.md#verify-delivery-before-routing-traffic) before routing traffic. To monitor the deployment, follow the [Prometheus, Grafana, and visibility probe setup](reference/observability.md).

## Upgrade

Choose the procedure for the version you are running.

### Upgrade from v0.17.0

1. Pause producers and let accepted events become queryable. In hosted mode, drain all workers and check for shutdown errors. Acknowledged events can still be in memory if a drain fails.
2. Stop all v0.17.0 instances. Keep the matching image and private configuration. Back up all standalone storage, including any analytical objects in S3, or take a coordinated hosted recovery point. [Backup and recovery methods](backup-and-restore.md#choose-a-method)
3. If you limit browser capture to specific websites, set `allowed_origins` before starting the new server. Start v0.18.0 with the existing storage. [Browser ingestion settings](reference/configuration.md#browser-ingestion)
4. Update the CLI. For self-hosted deployments, set `PORCUPINE_URL` or pass `--url`. Check readiness and confirm that a new test event becomes queryable before resuming producers.

Hosted startup applies PostgreSQL control migrations 23 and 24. They store the shared quarantine reason and index unpublished live reservations by project. Recovery reads the reservations directly, so the upgrade removes the trigger that queued them.

SQLite control migration 9 and PostgreSQL control migration 25 add the browser key type. Existing credentials continue to work. For an existing mixed project, a tenant administrator can provision a browser key that authorized project members can retrieve.

To roll back after the migration, restore the pre-upgrade recovery point before starting v0.17.0. Reconcile any writes accepted after cutover before resuming producers.

### Upgrade from v0.16.0

1. Pause producers and let the existing workers finish writing accepted events. Stop all v0.16.0 workers and keep the last working image and configuration.
2. Back up the stopped deployment. Use [native backups](backup-and-restore.md) for standalone or the [PostgreSQL and S3 recovery policy](design/hosted-architecture.md#recovery-and-retention) for hosted.
3. Start v0.17.0 with the existing configuration and storage. Check readiness and confirm that a new test event becomes queryable before resuming producers.

Hosted startup migrates the PostgreSQL control database and queues existing effect streams for one reconciliation pass. To roll back, restore the pre-upgrade recovery point before starting v0.16.0.

Replace older [multi-project team credentials](reference/service-keys.md) with one service account and new keys per project before restarting integrations.

### Upgrade from v0.15.1

v0.16.0 uses the same storage schemas as v0.15.1. Schedule downtime for the server or worker fleet.

1. Pause producers and wait until pending writes become queryable. In hosted mode, drain all workers and wait for shutdown to complete. Investigate a drain error before proceeding because acknowledged live events can still be in memory.
2. Stop the old instances. Take a complete recovery point with the tools for your current version. Keep the matching image and private configuration. Use [native backups](backup-and-restore.md) for standalone or the [PostgreSQL and S3 recovery policy](design/hosted-architecture.md#recovery-and-retention) for hosted. Restore the pre-upgrade recovery point if you need to roll back.
3. Start the v0.16.0 server or workers with the existing configuration and storage. Update the CLI, check readiness, and verify that a test event becomes queryable before resuming producers.

The Prometheus listener is disabled by default. If you collect server stderr, configure your log collector to parse JSON. [Monitoring setup](reference/observability.md)

### Upgrade from v0.15.0

v0.16.0 applies the numbered migrations introduced in v0.15.1 when it starts on v0.15.0 storage. Follow the [upgrade steps](#upgrade-from-v0151) with these precautions.

- For hosted, prevent old workers from restarting against the upgraded databases. Tickets from before the incarnation migration have a NULL incarnation and never expire automatically. [Legacy activity ticket procedure](design/hosted-architecture.md#upgrade-worker-activity-tickets)
- Before enabling hosted file reclamation, verify the [recovery floor and cleanup grace](reference/ducklake-cleanup.md#hosted-profile).
- Before relying on tenant deletion to erase credentials, review [legacy credentials with empty project scopes](reference/tenants.md#delete-a-tenant).

### Move from v0.14.0

Porcupine uses separate acceptance and control databases and tenant ownership. Start v0.16.0 with fresh storage. Keep the v0.14.0 image digest, complete data and backup bundles, overlay files, and private configuration together for recovery. Restore v0.14.0 bundles with the matching v0.14.0 tools. An image swap on the old volume cannot migrate it.

1. Pause producers at cutover and wait for pending writes to become queryable. Take a complete v0.14.0 checkpoint before changing credentials or routes.
2. Initialize v0.16.0 in a separate volume or directory. Transfer required history with an explicit export and [historical import](imports.md), preserving original timestamps and stable producer IDs where available. Imports carry events. Recreate accounts, grants, credentials, and destination settings separately.
3. Update clients to match the server. Remove the `use_cache` request field. Replace old Compose overlays with the [server configuration](reference/configuration.md) and review settings before enabling pipelines.
4. Verify counts, event times, identity behavior, corrections and deletes, then switch producers and readers. Keep the old instance isolated with its recovery material until the new deployment is validated. Returning to the old instance requires reconciling writes accepted after cutover.
