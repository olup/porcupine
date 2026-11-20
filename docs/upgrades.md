# Upgrade history

These procedures keep their original target versions. For the current release, start with [hosting and upgrades](hosting.md#upgrade).

## Upgrade from v0.18.0

1. Review any automation that provisions credentials. Use an instance administrator for `create-oauth-client` and for general `create-access-key` calls. A tenant administrator with `management:write` and `events:write` can issue an ingestion key for one project in their tenant. For private team access, use [team service accounts](reference/service-keys.md).
2. Pause producers and wait for accepted events to become queryable. In hosted mode, drain every worker and check for shutdown errors. Acknowledged events can still be in memory if a drain fails.
3. Stop all v0.18.0 instances. Keep their image and private configuration. Back up standalone storage, including any analytical objects in S3, or take a coordinated hosted recovery point. [Backup and recovery methods](backup-and-restore.md#choose-a-method)
4. Start v0.19.1 with the existing configuration and storage. Update the CLI. Check readiness and confirm that a new test event becomes queryable before resuming producers.

Hosted startup applies PostgreSQL control migration 26 to store a shared retry deadline for scheduled recovery. Standalone can start directly with its v0.18.0 storage.

To roll back a hosted deployment after the migration, restore the pre-upgrade recovery point before starting v0.18.0. Reconcile any writes accepted after cutover before resuming producers.

## Upgrade from v0.17.0

1. Pause producers and let accepted events become queryable. In hosted mode, drain all workers and check for shutdown errors. Acknowledged events can still be in memory if a drain fails.
2. Stop all v0.17.0 instances. Keep the matching image and private configuration. Back up all standalone storage, including any analytical objects in S3, or take a coordinated hosted recovery point. [Backup and recovery methods](backup-and-restore.md#choose-a-method)
3. If you limit browser capture to specific websites, set `allowed_origins` before starting the new server. Start v0.18.0 with the existing storage. [Browser ingestion settings](reference/configuration.md#browser-ingestion)
4. Update the CLI. For self-hosted deployments, set `PORCUPINE_URL` or pass `--url`. Check readiness and confirm that a new test event becomes queryable before resuming producers.

Hosted startup applies PostgreSQL control migrations 23 and 24. They store the shared quarantine reason and index unpublished live reservations by project. Recovery reads the reservations directly, so the upgrade removes the trigger that queued them.

SQLite control migration 9 and PostgreSQL control migration 25 add the browser key type. Existing credentials continue to work. For an existing mixed project, a tenant administrator can provision a browser key that authorized project members can retrieve.

To roll back after the migration, restore the pre-upgrade recovery point before starting v0.17.0. Reconcile any writes accepted after cutover before resuming producers.

## Upgrade from v0.16.0

1. Pause producers and let the existing workers finish writing accepted events. Stop all v0.16.0 workers and keep the last working image and configuration.
2. Back up the stopped deployment. Use [native backups](backup-and-restore.md) for standalone or the [PostgreSQL and S3 recovery policy](reference/shared-deployment.md#recovery-and-retention) for hosted.
3. Start v0.17.0 with the existing configuration and storage. Check readiness and confirm that a new test event becomes queryable before resuming producers.

Hosted startup migrates the PostgreSQL control database and queues existing effect streams for one reconciliation pass. To roll back, restore the pre-upgrade recovery point before starting v0.16.0.

Replace older [multi-project team credentials](reference/service-keys.md) with one service account and new keys per project before restarting integrations.

## Upgrade from v0.15.1

v0.16.0 uses the same storage schemas as v0.15.1. Schedule downtime for the server or worker fleet.

1. Pause producers and wait until pending writes become queryable. In hosted mode, drain all workers and wait for shutdown to complete. Investigate a drain error before proceeding because acknowledged live events can still be in memory.
2. Stop the old instances. Take a complete recovery point with the tools for your current version. Keep the matching image and private configuration. Use [native backups](backup-and-restore.md) for standalone or the [PostgreSQL and S3 recovery policy](reference/shared-deployment.md#recovery-and-retention) for hosted. Restore the pre-upgrade recovery point if you need to roll back.
3. Start the v0.16.0 server or workers with the existing configuration and storage. Update the CLI, check readiness, and verify that a test event becomes queryable before resuming producers.

The Prometheus listener is disabled by default. If you collect server stderr, configure your log collector to parse JSON. [Monitoring setup](reference/observability.md)

## Upgrade from v0.15.0

v0.16.0 applies the numbered migrations introduced in v0.15.1 when it starts on v0.15.0 storage. Follow the [upgrade steps](#upgrade-from-v0151) with these precautions.

- For hosted, prevent old workers from restarting against the upgraded databases. Tickets from before the incarnation migration have a NULL incarnation and never expire automatically. [Legacy activity ticket procedure](design/hosted-architecture.md#upgrade-worker-activity-tickets)
- Before enabling hosted file reclamation, verify the [recovery floor and cleanup grace](reference/ducklake-cleanup.md#hosted-profile).
- Before relying on tenant deletion to erase credentials, review [legacy credentials with empty project scopes](reference/tenants.md#delete-a-tenant).

## Move from v0.14.0

Porcupine uses separate acceptance and control databases and tenant ownership. Start v0.16.0 with fresh storage. Keep the v0.14.0 image digest, complete data and backup bundles, overlay files, and private configuration together for recovery. Restore v0.14.0 bundles with the matching v0.14.0 tools. An image swap on the old volume cannot migrate it.

1. Pause producers at cutover and wait for pending writes to become queryable. Take a complete v0.14.0 checkpoint before changing credentials or routes.
2. Initialize v0.16.0 in a separate volume or directory. Transfer required history with an explicit export and [historical import](imports.md), preserving original timestamps and stable producer IDs where available. Imports carry events. Recreate accounts, grants, credentials, and destination settings separately.
3. Update clients to match the server. Remove the `use_cache` request field. Replace old Compose overlays with the [server configuration](reference/configuration.md) and review settings before enabling pipelines.
4. Verify counts, event times, identity behavior, corrections and deletes, then switch producers and readers. Keep the old instance isolated with its recovery material until the new deployment is validated. Returning to the old instance requires reconciling writes accepted after cutover.
