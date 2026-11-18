# Changelog

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
