# Host Porcupine

Start a new instance with the [quickstart](../README.md#quickstart) and install the CLI from the [current release](https://github.com/olup/porcupine/releases/tag/v0.19.2). If you already run Porcupine, check the [upgrade steps](#upgrade) first and match the CLI to your server.

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
