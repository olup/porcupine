# ClickHouse hot and cold storage

S3 tiering is a global deployment setting controlled by the operator. It uses ClickHouse's native S3 disk and a MOVE TTL. Cold parts remain in the same table and use the same API and SQL queries. This is not an export pipeline, a separate archive API, or an instance backup.

Project credentials and agents cannot inspect or change storage policy, hot duration, bucket endpoint, region, or AWS credentials through HTTP or MCP. Even instance API administration does not expose deployment storage controls.

## Deployment settings

| Setting | Default | Meaning |
| --- | --- | --- |
| `PORCUPINE_CLICKHOUSE_STORAGE_POLICY` | Empty | Existing policy stays unchanged; a fresh table uses local storage |
| `PORCUPINE_HOT_STORAGE_DAYS` | `30` when enabled | Days after original receipt before parts become eligible to move |
| `PORCUPINE_CLICKHOUSE_COLD_VOLUME` | `cold` when enabled | Destination ClickHouse volume |

A nonempty storage policy enables policy and MOVE TTL configuration. Hot duration accepts 1 through 36,500 days. Policy and volume must be valid ClickHouse identifiers and must exist in the server's storage configuration.

The [Compose override](../compose.s3.yaml) selects `hot_cold`. Its mounted [ClickHouse configuration](../deployment/clickhouse/storage.xml) contains a local `default` volume and a `cold` volume backed by the native S3 disk. The existing local volume remains part of the policy so existing parts remain usable.

The override passes `PORCUPINE_S3_ENDPOINT`, `AWS_REGION`, and optional `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN` only to ClickHouse. They are not passed to the Go process or exposed to agents. The endpoint is a full bucket-and-prefix URL. Use a distinct prefix for each ClickHouse deployment. Credentials can come from the deployment's IAM provider chain or a secret manager.

## Enable S3 tiering

Use an existing bucket and credentials authorized for that deployment's prefix. Bucket creation and AWS deployment are outside Porcupine. Back up both databases before changing an existing deployment.

1. Keep the base deployment's `.env` with its bootstrap and ClickHouse provisioning secrets.
2. Copy [the S3 environment example](../deployment/.env.s3.example) to a private `.env.s3` file and set the real endpoint, region, and deployment settings. Use secret-manager or IAM credentials where available.
3. Stop the old Porcupine binary before starting the upgraded single writer.
4. Start the Compose override:

```sh
docker compose --env-file .env --env-file .env.s3 \
	-f compose.yaml -f compose.s3.yaml up -d --build
```

For an AWS Paris bucket, the endpoint has the form `https://YOUR_BUCKET.s3.eu-west-3.amazonaws.com/UNIQUE_PREFIX/` with `AWS_REGION=eu-west-3`. The endpoint must match the bucket's actual region. No bucket is created by this command.

## Movement and policy changes

The MOVE TTL uses original server receipt time, not a mutable client timestamp. ClickHouse moves parts asynchronously after all rows in a part meet the TTL condition. Thirty days is an eligibility rule, not a promise that each event moves exactly on day 30. Corrections retain original receipt metadata and the same logical event identity.

Initialization applies the policy and compares the current TTL with deployment configuration. An actual TTL change requests materialization for existing parts, including historical data. Repeated initialization with matching settings does not repeatedly request materialization. Moving data remains background ClickHouse work.

An empty policy leaves an existing policy and TTL untouched. Removing the override or increasing hot duration does not recall cold data to local storage. Tables that already contain cold parts still need the S3 disk configuration, credentials, object data, and local disk metadata. Removing these can make historical data unavailable.

No DELETE TTL is installed. Total event history remains unbounded except for explicit event deletion. ClickHouse deletion tombstones remain. Operators can keep permanent SQLite receipts or [cap the local receipt ledger](receipts.md). There is no automatic age-based loss of history.

## Migration from the first event table

The current event table is `events_v2`, using `ReplacingMergeTree(revision,is_deleted)`. The SQL table exposed to readers is still `events`. On upgrade, initialization copies the logical `FINAL` contents of the legacy table into revision-zero projections and verifies payload, identity, receipt, and original-snapshot fields.

A durable `schema_state` marker records successful verification before the exact verified legacy table is dropped. A table UUID check prevents retiring a different source table. After the marker, initialization never reimports the legacy table. A missing target after completed migration fails rather than recreating an empty replacement. Migration preserves logical events; it does not wipe data as a reset procedure.

Stop the old binary before upgrading. Migration requires one application writer; running an old writer against the legacy table during migration is unsupported. Keep a tested backup rather than relying on downgrade to an older binary after migration.

## Recovery and deletion limits

The native S3 disk relies on local ClickHouse metadata under the persistent `/var/lib/clickhouse` volume as well as S3 objects. A bucket containing objects alone is not a documented restore procedure. Protect the local metadata and preserve the mounted disk configuration. Back up authoritative SQLite state separately.

Corrections and tombstones work through the same table whether older parts are local or cold. Deleted payloads can persist in obsolete parts, object versions, and backups until their own cleanup. Logical API deletion does not synchronously erase every physical copy. Do not configure object lifecycle expiration against live ClickHouse disk objects as a substitute for retention in the application.

`make integration-s3` starts isolated real ClickHouse and MinIO services for opt-in native cold-storage and mutation tests, then removes its own test containers. It does not deploy AWS resources. Normal checks do not establish AWS connectivity, production backup recovery, or storage cost benchmarks.
