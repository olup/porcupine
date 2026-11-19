# Changelog

## 0.19.1

- Event catalogs and profiles now select event IDs before reading occurrence details and sample payloads from DuckLake. Catalog counts still cover the full delivered history. (#154)

v0.19.1 uses the v0.19.0 storage schemas. [Upgrade from v0.19.0](https://github.com/olup/porcupine/blob/v0.19.1/docs/hosting.md#upgrade-from-v0190)

## 0.19.0

- Interactive event queries accept an optional `time_range` through HTTP, CLI, and MCP. The range applies to every `events` reference and includes `from` while excluding `to`. Both bounds are RFC3339 occurrence timestamps. (#152) [Query reference](https://github.com/olup/porcupine/blob/v0.19.0/docs/reference/analytics.md#sql-request-and-result)
- Creating OAuth clients and general access keys now requires an instance administrator. Tenant administrators with `management:write` and `events:write` can still issue single-project ingestion keys for their tenant and use team service accounts for private access. (#149)
- A failed scheduled hosted recovery attempt delays the next attempt for that project by 30 seconds across workers. A new recovery signal clears the delay. (#145)
- Hosted workers verify newly appended DuckLake commit proofs when they can reuse a prior audit. New live batch IDs let DuckLake skip older proof files during lookup, and existing proofs remain readable. (#147, #150)
- Shared output writing and scheduled pipeline operations across storage profiles. Removed the superseded hosted webhook claim path. (#146, #151)
- Documented browser ingestion key limits and how to replace a key. (#148)

Stop the deployment and take a recovery point before upgrading. Hosted startup applies PostgreSQL control migration 26. Review credential provisioning automation before cutover. [Upgrade from v0.18.0](https://github.com/olup/porcupine/blob/v0.19.0/docs/hosting.md#upgrade-from-v0180)

## 0.18.0

- Mixed projects can use a retrievable browser key scoped to event capture. Tenant administrators can provision one for existing mixed projects. Existing ingestion keys continue to work.
- Hosted workers retain accepted events in memory while retrying a quarantined project. Shared quarantine blocks new ingestion for that project across workers until recovery or a flush succeeds. (#141)
- Hosted recovery discovers unpublished live reservations directly, avoiding an extra reconciliation pass after normal flushes. (#142)
- Native, PostHog, and Segment capture now allow cross-origin browser requests when `allowed_origins` is missing or empty. Set an explicit list to restrict origins. Use `server_only` projects to require private API credentials for capture. (#143)
- The CLI connects to Porcupine Cloud by default. Set `PORCUPINE_URL` or pass `--url` for a self-hosted instance. (#139)
- Shortened the onboarding, hosting, and recovery guides. (#140)

Stop the deployment and take a recovery point before upgrading. Follow the [v0.17.0 upgrade steps](https://github.com/olup/porcupine/blob/v0.18.0/docs/hosting.md#upgrade-from-v0170).

## 0.17.0

- Hosted recovery looks for pending work in PostgreSQL before opening DuckLake. Idle projects no longer create recovery snapshots or write Parquet objects. The upgrade seeds recovery for existing effect streams once. (#136)
- Hosted webhook output limits can split a flush at request boundaries. If a single request still exceeds a permanent output limit, Porcupine keeps its events and reports the refused webhook outputs. (#134)
- On loss of the PostgreSQL incarnation lease, hosted workers cancel writes, keep their in-memory events, and resume under a new lease. Readiness stays down while the worker lacks a valid lease. (#137)
- A hosted project that repeatedly fails with the same deterministic error is quarantined on the worker. The worker reports the discarded buffered copies and rejects new requests for that project while other projects can continue. (#135)
- Team service accounts and their API or MCP keys are bound to a single project. Older team credentials with multiple projects require replacements. (#133)

Follow the [v0.16.0 upgrade steps](https://github.com/olup/porcupine/blob/v0.17.0/docs/hosting.md#upgrade-from-v0160) and back up hosted control storage before the recovery queue migration.

## 0.16.0

- Added a private, opt-in Prometheus listener for each server process and a Grafana operator dashboard. They report ingestion, queue pressure, flushes, HTTP and MCP activity, and interactive query timing. Server logs use JSON on stderr. (#118)
- Added `porcupine visibility-probe` to measure the time from capture acknowledgment to the first SQL result containing a synthetic event. Run it once or scrape its metrics in periodic mode. (#118) [Monitoring guide](https://github.com/olup/porcupine/blob/v0.16.0/docs/reference/observability.md)
- Moved hosted live buffering and interactive query execution into dedicated packages. Exports and scheduled queries share the worker capacity gate with interactive queries. Standalone still acknowledges after the SQLite commit. Hosted acknowledges bounded worker memory admission, so a crash or incomplete drain can lose acknowledged events before a flush commits. (#122, #123)
- Added coverage for export jobs waiting on delivery and worker capacity, interactive queries and capture during an export, and reclamation of activity from expired hosted workers. (#119, #120, #124)
- Hosted startup now shares one PostgreSQL pipeline effects handler between ingestion flushes and pipeline jobs. It configures webhook delivery and import checkpoints when workers start. (#129, #130)
- Combined the query and export guide, clarified hosting and storage choices, and updated the architecture package map. (#121, #125)
- Made interactive query reservation cleanup release resources once and preserve the first result across repeated calls. (#127)
- Classified missing bootstrap tokens and incomplete restores by the underlying error in startup logs and metrics. (#131)

v0.16.0 reuses v0.15.1 storage without a schema migration. Stop and back up the deployment before upgrading. The [hosting guide](https://github.com/olup/porcupine/blob/v0.16.0/docs/hosting.md#upgrade-from-v0151) also covers the v0.15.0 migrations and the fresh-storage move from v0.14.0.

## 0.15.1

- Hosted shutdown starts draining accepted live events before waiting on canceled analytical calls. Flush failures apply backpressure to the affected project. Hosted output capacity is scoped to tenants and projects. (#100, #104, #105)
- Hosted maintenance reclaims obsolete DuckLake history under the write fence and recovery policy. Worker incarnation leases let deletion reclaim activity left by crashed workers. Both profiles delete operator-issued credentials owned by the deleted tenant. (#102, #103, #111)
- Hosted usage samples current Parquet bytes and marks queries lost after five minutes without an observation. External authentication accepts more provider JWKS formats and can use cached keys during a failed refresh. (#101, #106)
- A clean hosted live flush commits in one DuckLake snapshot. Fresh webhook payloads skip immediate S3 body read-back. Recovery still verifies stored bodies. (#107, #108)
- Hosted output publication reuses the reservation recorded before upload. The final deletion check releases PostgreSQL tenant and project locks before the native commit, so query admission can proceed while the commit is in flight. The project coordinator defaults to a five-second acquisition deadline. (#112, #114)
- Hosted live ingestion checks the monthly quota at admission. Already admitted batches can finish after the budget changes. (#113)
- Added a repeatable hosted flush benchmark, consolidated the hosted architecture guide, and fixed the hosted status privacy test race. (#109, #110, #115)

[Upgrade from v0.15.0](https://github.com/olup/porcupine/blob/v0.15.1/docs/hosting.md#upgrade-from-v0150) using the existing storage and a stopped, backed-up deployment. Moving from v0.14.0 [requires fresh storage](https://github.com/olup/porcupine/blob/v0.15.1/docs/hosting.md#move-from-v0140). Hosted live ingestion still acknowledges worker memory admission and can lose acknowledged events before a flush commits. Verify the recovery floor before enabling hosted file reclamation.

## 0.15.0

- Retry bounded native transaction conflicts between local projection and inline-data maintenance without serializing the default maintenance path. Cancellation, validation errors and allocation recovery keep their separate handling.
- Shared interactive queries renew execution-scoped reservations until the native call returns. Unresponsive or uncertain calls quarantine the affected project's interactive engine and degrade readiness instead of terminating the worker. Automatic reservation expiry accepts possible physical concurrency overshoot during a control-network partition.
- Tenants with team administrators, project-scoped memberships, monthly ingestion allowances, and configurable interactive query concurrency, timeout, row, and byte limits.
- Separate SQLite control and ingestion databases in standalone mode.
- Email OTP and optional trusted Google/OIDC sign-in for invited humans, with operator identity recovery and revocation of previous interactive grants.
- Read-only MCP tools `porcupine_schema` and `porcupine_query`.
- Project editors can document exact event names and top-level property keys. Catalog readers see the description, last editor ID, and edit time alongside observed data.
- Removed inert `use_cache` input and obsolete result-cache/scan-limit fields. `use_cache` is now rejected. This is a breaking API change.
- Versioned operational database schemas. Startup refuses newer and unknown databases before changing them.
- Faster local checks and parallel test execution, with complete race/integration suites retained for validation.
- DuckLake engine extracted into `internal/lake`. Canonical event types, live input normalization, historical parsing, webhook/row delivery, and S3 client construction have separate modules while durable transaction ownership stays in the service.
- SQLite-journal deployments retain retry receipts indefinitely by default (`receipt_retention: "0"`), keeping fresh installations' acceptance independent of analytical availability. Optional pruning verifies analytical heads first; after a project's first prune, producer-ID batches require DuckLake lookups. Returning to zero stops future pruning without restoring deleted receipts or disabling historical checks.
- DuckLake schema v2 stores the current event once, retains an original snapshot only when corrected, and partitions by month. It preserves source timestamp precision, rejects unsupported formats before writes, and resumes interrupted recognized upgrades. No general storage-savings or throughput guarantee follows from the schema change.
- Authentication policy, upstream provider verification, and login/email admission limits have a separate `internal/auth` package. Persistence, consent issuance, and revocation keep their existing transaction owners. No sign-in mode is removed.
- One typed `porcupine.json` server configuration with validated environment overrides and unknown-key rejection. Server commands `init`, `config check`, and atomic private `config install` support setup without extra runtime libraries. One Compose file replaces the optional overlays; the standalone API client stays separate.
- Documentation reorganized into task guides, detailed reference, and design/validation records. The agent entry point describes the current contract rather than cumulative release history.
- Hugo builds the documentation through a Go helper, using a pinned, checksum-verified binary. CLI packaging, artifact checks, and SDK identity probes also use Go; development and CI no longer require Python.

- Added an opt-in hosted profile with PostgreSQL control and DuckLake metadata, analytical S3, and replaceable workers. Live ingestion acknowledges admission to bounded worker memory. A process failure or incomplete drain can lose acknowledged events before a flush commits. The flush interval does not bound loss during an outage. Standalone still acknowledges after the SQLite commit.
- Hosted workers share resumable import uploads, durable event mutation intents, query admission, webhook and export jobs, and scheduled SQL-to-S3 pipelines. Hosted project and tenant deletion fence access first. Physical reclamation waits for the configured grace period, the operator restore floor, and reconciliation of pending work and object ownership.

Start with fresh storage when moving from v0.14.0. Keep the old image, data and backups together. Transfer the history you need explicitly and recreate access on the new instance. Remove `use_cache` from SQL requests and use matching server and CLI versions. Hosted operators must exercise PostgreSQL and S3 recovery before admitting traffic. See the upgrade and hosted setup links in the hosting guide.

## 0.14.0

- Historical native, PostHog, and Segment imports in one resumable CLI command, preserving occurrence dates and stable IDs.
- Published native producer schemas and event-input discovery through API, CLI, and MCP.
- Adaptive analytical batch reduction after typed native allocation errors.
- Complete backups to an independent S3 destination and isolated offline recovery through the standalone CLI.
- DuckDB/DuckLake as the only analytical runtime, with explicit durable acceptance/import/webhook transaction boundaries and offline CLI generation.

## 0.13.0

- Analytical S3 storage for fresh deployments.
- Event profiles with activity, recent examples, and sampled property statistics.
- Scheduled DuckLake snapshot expiration, deleted-row rewrites, and obsolete-file cleanup.
- Coordinated local backup bundles, inventory validation, retention, and isolated restore.

Earlier releases and their assets are recorded in [GitHub releases](https://github.com/olup/porcupine/releases).
