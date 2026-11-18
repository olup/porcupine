# Store analytical data on S3

A fresh standalone deployment can write its DuckLake data files to S3 or an S3-compatible service. SQLite and the per-project catalogs stay on persistent local storage.

Queries, ingestion, identify, corrections and deletion use the same API, CLI and MCP contracts as local storage. DuckDB reads the remote Parquet files for those queries. Query-result exports and destination pipelines are separate features.

## Configure a new deployment

Use an existing private bucket, its actual region, and a dedicated prefix. Porcupine does not create the bucket. Adapt the [VM and S3 example](../hosting.md#one-vm-with-analytical-s3). Set `ducklake.s3.bucket`, `ducklake.s3.prefix`, and `ducklake.s3.region` in your private configuration. Supply credentials through the supported AWS credential chain or the explicit analytical access-key settings. Start with a fresh data volume. An existing local archive cannot move to S3 by changing environment variables; the server rejects a storage-identity mismatch. Automatic migration from SSD to S3 is not part of this release.

The deployment operator chooses storage. Project credentials and agents cannot change the bucket, endpoint, prefix, credentials or region through the management API.

## Storage settings

| Variable | Purpose |
| --- | --- |
| `PORCUPINE_DUCKLAKE_S3_BUCKET` | Existing analytical bucket. Required when S3 is enabled. |
| `PORCUPINE_DUCKLAKE_S3_PREFIX` | Dedicated deployment prefix. Required. |
| `PORCUPINE_DUCKLAKE_S3_REGION` | Bucket region, such as `eu-west-3`. Required. |
| `PORCUPINE_DUCKLAKE_S3_ENDPOINT` | Optional full HTTP or HTTPS origin for an S3-compatible service. Empty selects the regional AWS endpoint. |
| `PORCUPINE_DUCKLAKE_S3_PATH_STYLE` | `true` for services requiring path-style addressing. Defaults to `false`. |
| `PORCUPINE_DUCKLAKE_S3_ACCESS_KEY_ID` | Optional explicit access key. Set with the matching secret. |
| `PORCUPINE_DUCKLAKE_S3_SECRET_ACCESS_KEY` | Explicit secret access key. Keep outside Git and client configuration. |
| `PORCUPINE_DUCKLAKE_S3_SESSION_TOKEN` | Optional token accompanying explicit temporary credentials. |
| `PORCUPINE_DUCKLAKE_S3_REQUEST_TIMEOUT_SECONDS` | Timeout for one native object-storage HTTP request. Defaults to `5`; range 1 through 30 seconds. |
| `PORCUPINE_DUCKLAKE_S3_CA_CERT_FILE` | Optional readable CA bundle inside the server's filesystem for a private HTTPS service. |

Prefixes are relative paths with ordinary letters, digits, dots, underscores and hyphens in their segments. Parent-directory segments, empty segments and arbitrary URLs are rejected. The configured storage identity is pinned in the local registry. Changing its bucket, prefix, endpoint or AWS partition requires an explicit migration procedure. Correcting the signing region within the same AWS partition, rotating credentials or changing request timeouts does not change the storage identity.

Without explicit analytical keys, the native AWS credential-chain provider refreshes credentials automatically. Configure the relevant AWS environment, profile or workload identity for the server. Profile and token files must be mounted where the server can read them. Explicit temporary credentials come from configuration and require rotation by the operator.

For a private CA, mount the CA file read-only and set its container pathname. Normal AWS HTTPS uses the image's system CA bundle. HTTP is useful for isolated local tests; use HTTPS for remote services. The image includes DuckLake, HTTPFS and AWS extensions, so runtime extension downloads are unnecessary.

Grant the server access to list the configured prefix and read, write and delete its objects. Include the multipart-upload permissions used by the object store. Keep bucket lifecycle rules from expiring live analytical files. Configure abort of abandoned multipart uploads separately if needed.

## Isolation and cleanup

Each project gets a prefix beneath the deployment's source identity and its hashed project ID. Native credentials and filesystem access are scoped to that project prefix. The SQL validator continues to reject arbitrary object reads, private tables, secret inspection, writes and configuration changes.

DuckLake's normal maintenance operates on remote files too. The [snapshot and obsolete-file policy](ducklake-cleanup.md) still applies, including protection of active readers. Deleting a project removes its local catalog and current objects within its exact managed prefix. A failed remote deletion remains fenced and retryable; other projects keep their own data and access.

Object versions, backups and copies already delivered elsewhere follow their own retention rules. Removing a current S3 object does not promise erasure of previous versions. This option writes data files to S3 directly; it is not an age-based SSD-to-S3 tiering policy.

Native HTTP retries are disabled so an unavailable object service does not extend a request through repeated retries. A native HTTP request already in flight cannot always be interrupted immediately by SQL cancellation. It can extend the SQL deadline by up to the configured object-request timeout. Agents can discover that bound as `limits.object_storage_request_timeout_seconds` in `get-event-schema`.

## Check the deployment

An authenticated `get-instance-status` reports `configuration.native_s3_storage=true` when configured. This is a configuration flag, not a bucket-connectivity test. Test a synthetic event, wait for delivery, run a query, then inspect the managed objects with operator credentials. [Instance status](instance-status.md)

The local-only [automatic recovery bundle](native-backups.md) is rejected with analytical S3 because it would omit remote objects. Use the [complete S3 backup and offline restore procedure](native-backup-recovery.md). It preserves operational and catalog state with the referenced remote files in an independent destination. A bucket listing alone does not recover users, grants, receipts, or catalog metadata.

## Test the integration locally

```sh
make ducklake-s3-test
```

The Go harness starts a private disposable S3-compatible Docker service. It checks real objects and native lifecycle behavior without connecting to AWS or a running Porcupine deployment. Release image validation also runs the server as its nonroot user against HTTPS object storage on an internal Docker network, with no Internet access for the tested server.
