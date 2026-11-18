# Host Porcupine 0.15.1

Start a local instance with the [v0.15.1 quickstart](../README.md#quickstart). Keep the server and CLI versions matched. To reuse an existing deployment, follow the steps to [upgrade from v0.15.0](#upgrade-from-v0150) or [move from v0.14.0](#move-from-v0140).

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

[vm-s3.json](examples/configuration/vm-s3.json) adds settings for an existing private bucket. Choose S3 before initializing a fresh archive. See [analytical S3 configuration](reference/ducklake-s3.md) and [backups to a separate destination](reference/native-backup-recovery.md).

## Configure access and backups

[Set up the first tenant administrator](reference/tenant-access.md) with builtin sign-in, or configure [external authentication](reference/external-auth.md). Follow the [backup guide](backup-and-restore.md) before storing production data.

## Provision a tenant from a private service

Follow [tenant provisioning](reference/tenants.md#provision-a-tenant-from-a-private-service) if you manage teams from your own application.

## Keep data and resources safe

Review [persistent storage and memory settings](reference/hosting-options.md#persistent-storage-and-resources). Follow the [readiness and delivery checks](reference/instance-status.md#verify-delivery-before-routing-traffic) before routing traffic. For a source build with observability, see [Prometheus, Grafana, and the visibility probe](reference/observability.md).

## Upgrade from v0.15.0

v0.15.1 applies numbered migrations to existing v0.15.0 storage at startup. Schedule downtime for the server or worker fleet.

1. Pause producers and allow pending writes to become queryable. In hosted mode, drain all workers and wait for shutdown to complete. Investigate a drain error before proceeding because acknowledged live events can still be in memory.
2. Stop the old instances. Take a complete recovery point with v0.15.0 tools and retain the matching image and private configuration. Use [native backups](backup-and-restore.md) for standalone or the [PostgreSQL and S3 recovery policy](design/hosted-architecture.md#recovery-and-retention) for hosted. Restore that pre-upgrade recovery point if you need to roll back.
3. For hosted, prevent old workers from restarting against the upgraded databases. Review the [legacy activity ticket procedure](design/hosted-architecture.md#upgrade-worker-activity-tickets). Tickets from before the incarnation migration have a NULL incarnation and never expire automatically. Verify the [recovery floor and cleanup grace](reference/ducklake-cleanup.md#hosted-profile) before enabling hosted file reclamation.
4. Start the v0.15.1 server or workers with the existing configuration and storage. Update the CLI, check readiness, and verify that a test event becomes queryable before resuming producers. Review [legacy credentials with empty project scopes](reference/tenants.md#delete-a-tenant) before relying on tenant deletion to erase those credentials.

## Move from v0.14.0

The v0.15 series uses separate acceptance and control databases and tenant ownership. Start v0.15.1 with fresh storage. Keep the v0.14.0 image digest, complete data and backup bundles, overlay files, and private configuration together for recovery. Restore v0.14.0 bundles with the matching v0.14.0 tools. An image swap on the old volume cannot migrate it.

1. Pause producers at cutover and wait for pending writes to become queryable. Take a complete v0.14.0 checkpoint before changing credentials or routes.
2. Initialize v0.15.1 in a separate volume or directory. Transfer required history with an explicit export and [historical import](imports.md), preserving original timestamps and stable producer IDs where available. Imports carry events. Recreate accounts, grants, credentials, and destination settings separately.
3. Update clients to match the server. Remove the `use_cache` request field. Replace old Compose overlays with the [server configuration](reference/configuration.md) and review settings before enabling pipelines.
4. Verify counts, event times, identity behavior, corrections and deletes, then switch producers and readers. Keep the old instance isolated with its recovery material until the new deployment is validated. Returning to the old instance requires reconciling writes accepted after cutover.
