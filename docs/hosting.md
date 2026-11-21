# Host Porcupine

Start a new instance with the [quickstart](../README.md#quickstart) and install the CLI from the [current release](https://github.com/olup/porcupine/releases/tag/v0.20.0). If you already run Porcupine, check the [upgrade steps](#upgrade) first and match the CLI to your server.

<a id="choose-local-sqlite-or-shared-postgresql-unreleased"></a><!-- Legacy URL fragment. -->
<a id="choose-a-storage-mode"></a>

## Pick a deployment

- For a laptop, start with the [local quickstart](../README.md#local-setup).
- For a standalone VM, configure [HTTPS and persistent storage](#one-vm). To store analytical files in S3, start with the [S3 profile](#one-vm-with-analytical-s3).
- For replaceable workers behind an ingress, follow the [hosted setup and recovery runbook](reference/shared-deployment.md#configuration).

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

Follow the steps for your installed version.

### Upgrade from v0.19.6

1. Pause producers and wait for accepted events to become queryable. In hosted mode, drain every worker and investigate any shutdown errors before continuing. A failed drain can leave acknowledged events in worker memory.
2. Stop all v0.19.6 server processes. Keep the v0.19.6 image and private configuration.
3. Take a complete recovery point. For standalone, capture both SQLite databases and the DuckLake storage, including any analytical objects in S3, together. For hosted, coordinate PostgreSQL recovery with S3 object versions. [Backup and recovery methods](backup-and-restore.md#choose-a-method)
4. Start v0.20.0 with the existing configuration and storage. Existing analytical stores keep plaintext Parquet. For a new encrypted store or an offline standalone move to fresh storage, follow the [encryption setup and recovery steps](reference/configuration.md#ducklake-parquet-encryption).
5. Update the CLI. Check readiness and verify that a new test event becomes queryable before resuming producers.

Standalone startup advances the SQLite journal to version 11. Hosted startup advances PostgreSQL control to version 28. The DuckLake native registry advances to version 2 for a local SQLite registry or version 6 for a PostgreSQL registry.

To roll back, drain and stop v0.20.0, restore the complete pre-upgrade recovery point, and start v0.19.6 with its original configuration. Reconcile writes accepted after cutover before resuming producers.

### Upgrade from v0.19.5

1. Pause producers and wait for accepted events to become queryable. In hosted mode, drain every worker and investigate any shutdown errors before continuing. A failed drain can leave acknowledged events in worker memory.
2. Stop all v0.19.5 server processes. Keep the v0.19.5 image and private configuration.
3. Take a complete recovery point. For standalone, capture both SQLite databases and the DuckLake storage, including any analytical objects in S3, together. For hosted, coordinate PostgreSQL recovery with S3 object versions. [Backup and recovery methods](backup-and-restore.md#choose-a-method)
4. Start v0.19.6 with the existing configuration and storage.
5. Update the CLI. Check readiness and verify that a new test event becomes queryable before resuming producers.

v0.19.6 uses the v0.19.5 storage schemas. To roll back, drain and stop v0.19.6, then start v0.19.5 with the same configuration and storage.

### Upgrade from v0.19.4

1. Pause producers and wait for accepted events to become queryable. In hosted mode, drain every worker and check for shutdown errors. Investigate a failed drain before continuing because acknowledged events can still be in worker memory.
2. Stop all v0.19.4 server processes. Keep the v0.19.4 image and private configuration.
3. Take a complete recovery point. For standalone, capture both SQLite databases and the DuckLake storage, including any analytical objects in S3, together. For hosted, coordinate PostgreSQL recovery with S3 object versions. [Backup and recovery methods](backup-and-restore.md#choose-a-method)
4. Start v0.19.5 with the existing configuration and storage.
5. Update the CLI. Check readiness and verify that a new test event becomes queryable before resuming producers.

v0.19.5 uses the v0.19.4 storage schemas. To roll back, drain and stop v0.19.5, then start v0.19.4 with the same configuration and storage.

### Upgrade from v0.19.3

1. Pause producers and wait for accepted events to become queryable. In hosted mode, drain every worker and check for shutdown errors. Investigate a failed drain before continuing because acknowledged events can still be in worker memory.
2. Stop all v0.19.3 server processes. Keep the v0.19.3 image and private configuration.
3. Take a complete recovery point. For standalone, capture both SQLite databases and the DuckLake storage, including any analytical objects in S3, together. For hosted, coordinate PostgreSQL recovery with S3 object versions. [Backup and recovery methods](backup-and-restore.md#choose-a-method)
4. Start v0.19.4 with the existing configuration and storage.
5. Update the CLI. Check readiness and verify that a new test event becomes queryable before resuming producers.

Startup adds the catalog summary tables to the control database. Standalone SQLite control advances to version 10, and hosted PostgreSQL control advances to version 27. The standalone journal and DuckLake schemas stay at their existing versions.

v0.19.3 refuses the newer control schema in both profiles. To roll back, restore the complete pre-upgrade recovery point before starting v0.19.3. Reconcile writes accepted after cutover before resuming producers.

### Upgrade from v0.19.2

1. Pause producers and wait for accepted events to become queryable. In hosted mode, drain every worker and check for shutdown errors. Investigate a failed drain before continuing because acknowledged events can still be in worker memory.
2. Keep the v0.19.2 image and private configuration. Take a complete recovery point before starting v0.19.3. For standalone, capture both SQLite databases and the DuckLake storage, including any analytical objects in S3, together. For hosted, coordinate PostgreSQL recovery with S3 object versions. [Backup and recovery methods](backup-and-restore.md#choose-a-method)
3. Stop any remaining v0.19.2 processes and start v0.19.3 with the existing configuration and storage.
4. Update the CLI. Check readiness and verify that a new test event becomes queryable before resuming producers.

Standalone startup migrates the SQLite journal to version 10 and counts pending events and their charged bytes by project. The migration runs even if you leave the optional [per-project queue limits](reference/reliability.md#acceptance-and-queue-limits) unset. Hosted PostgreSQL control and DuckLake catalog schema versions stay the same.

For standalone rollback, restore the complete pre-upgrade recovery point before starting v0.19.2. v0.19.2 refuses a version 10 journal. Reconcile writes accepted after cutover before resuming producers.

For hosted rollback, use the v0.19.2 configuration. The optional `acceptance.flush_interval` setting requires v0.19.3.

### Upgrade from v0.19.1

1. Pause producers and wait for accepted events to become queryable. In hosted mode, drain every worker and check for shutdown errors.
2. Stop v0.19.1 and start v0.19.2 with the existing configuration and storage.
3. Update the CLI and check readiness. Confirm that a new test event becomes queryable before resuming producers.

### Upgrade from v0.19.0

1. Pause producers and wait for accepted events to become queryable. In hosted mode, drain every worker and check for shutdown errors.
2. Stop v0.19.0 and start v0.19.1 with the existing configuration and storage.
3. Update the CLI and check readiness. Confirm that a new test event becomes queryable before resuming producers.

<a id="upgrade-from-v0180"></a>
<a id="upgrade-from-v0170"></a>
<a id="upgrade-from-v0160"></a>
<a id="upgrade-from-v0151"></a>
<a id="upgrade-from-v0150"></a>
<a id="move-from-v0140"></a>

See the [upgrade history](upgrades.md) for the v0.14.0 cutover and the v0.15.0 through v0.18.0 upgrade steps.
