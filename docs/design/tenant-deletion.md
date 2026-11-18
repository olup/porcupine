# Tenant deletion

Decision record, 17 November 2026. **Unreleased.** Based on main `6cf9f4a`. The implementation provides immediate fences and resumable standalone logical cleanup. Current owner-local shared cleanup remains explicitly pending. Physical purge verification is not implemented.

Tenant deletion must stop access before cleanup starts. Cleanup can fail or need an unavailable journal owner without making the tenant accessible again. The operation must preserve other tenants, including a shared human's memberships and tool connections.

## Operator contract

`delete-tenant`, `DELETE /api/v1/tenants/{tenant_id}`, requires `instance:admin`. It durably starts deletion and returns HTTP 202 with the deletion status. Repeating the same tenant ID returns the same job. An ID that never existed returns 404. A completed tombstone remains addressable after the tenant row disappears.

`get-tenant-deletion`, `GET /api/v1/tenants/{tenant_id}/deletion`, requires `instance:admin` and returns the same status. Register both operations with Huma authorization metadata so HTTP, MCP, and generated CLI share the contract. Status must never include names, email addresses, stored credentials, SQL, storage errors, or event data.

Status fields are `tenant_id`, `state`, `started_at`, `updated_at`, `projects_total`, `projects_remaining`, `physical_purge`, and a bounded `error_code`. The same tenant ID is the idempotency key. No generic workflow engine or SaaS callback is needed.

## Durable state and transitions

Append a numbered local-control migration and shared-control migration. Do not change the version-one DDL. Add `tenant_deletions`, keyed by `tenant_id`, with no foreign key to `tenants`. Its row survives removal of the tenant. Existing `project_deletions.tenant_id` provides the project cleanup inventory and progress after project rows disappear.

| State | Meaning |
| --- | --- |
| `deleting` | The authoritative tenant and project fences committed. Credentials owned by the tenant are revoked. Cleanup remains incomplete and recovery retries it. |
| `logically_deleted` | Current managed objects, analytical metadata, payloads, jobs, and tenant control records were removed. Object versions or backup retention remain unverified or intentionally retained. |
| `purged` | Physical managed-object removal was verified, including noncurrent versions and multipart uploads within the managed namespace. The current cleanup contract cannot prove this state for versioned S3. |

Failures leave the durable state at its last successful stage and record only a constant error category. Recovery retries without removing either fence. A deletion has no cancel or restore transition.

This implementation ends at `logically_deleted` with `physical_purge=pending_verification`, including standalone local storage. It never writes `purged`. Physical purge requires a separate verifier that understands all configured storage and retention policies. A status named `complete` must not imply that historical S3 versions or PostgreSQL backups disappeared.

## Start transaction and cleanup loop

1. Serialize against tenant administration, project creation, and credential creation with the existing control metadata write lock. Lock the tenant row in PostgreSQL. Read project ownership from `projects`, never from a request body or a token's project labels.
2. Insert the tenant tombstone, set `ingestion_allowed=0`, and insert a `project_deletions` fence for every tenant project in the same transaction. Operator-issued credentials with a project set confined to one tenant record their cleanup owner at issuance, independently of tenant machine privileges. At deletion, also identify older credentials whose stored projects still identify a single tenant. Revoke those credentials, tenant machine accounts and tenant-admin consent grants. Keep global human identities, credentials with instance-wide scope, and credentials that still reach another tenant. Project and token cleanup remove access to the deleted projects from shared credentials.
3. Commit before returning 202. All new project operations consult the authoritative fences. Cached tenant budgets cannot authorize work after this transaction.
4. Extend `startDeletionRecovery` rather than adding another background lifecycle. Each current journal owner processes its existing project fences through `deleteProject`. An unavailable owner leaves the tenant in `deleting`; another worker must not fabricate or replace that owner's journal.
5. Cancel and drain project activity, clean unpublished pipeline artifacts through `deleteS3Pipeline`, stage export cleanup, purge local records, then invoke the existing native `DeleteProject`. Preserve cleanup inventory until each external operation is acknowledged.
6. In standalone mode, publish the last durable usage high-water values before removing local counters. Once every project fence is complete, remove local tenant anchors and owned control credential and membership records in foreign-key order. Remove the tenant row last, then record `logically_deleted`. In current shared mode, finalization stops with `shared_local_cleanup_pending`; project owners can continue their existing project cleanup, but no worker can prove erasure of every owner-local cache. Phase 5 replaces this backend limitation with shared operational state. Do not add worker-registration or cleanup-acknowledgment protocols for the old architecture.

Use compare-and-set updates for progress. Concurrent recovery workers can repeat native deletion and object deletion safely. Existing project and native tombstones remain authoritative. Startup must not recreate a deleted default tenant.

## Ownership and cleanup inventory

| Records or objects | Disposition |
| --- | --- |
| `projects`, `project_deletions` | Reuse project deletion. Keep permanent project ID and tenant ID tombstones. Remove active project rows only after cleanup. |
| Journal `events`, `delivery_ready`, `delivery_quarantine`, `event_heads`, `event_receipts` | Existing `purgeProjectLocalRecords`, including cascading delivery rows and queue triggers. |
| `identity_links`, `identity_profiles`, `posthog_import_person_state` | Existing project purge. Native identity projections disappear with the project catalog. |
| `mutation_outbox`, `mutation_audit` | Existing project purge after activity drains. No delayed mutation can create a project after its native tombstone. |
| `import_uploads`, `import_upload_files`, `import_upload_samples` | Existing project purge, including checkpoints, samples, hash state, and parser tails. Future hosted upload objects need durable cleanup inventory before these rows disappear. |
| `export_jobs`, `export_files`, `project_deletion_files` | Existing export inventory and retry path abort multipart uploads and remove managed output objects before checkpoints disappear. Versioned object deletion is logical only. |
| `pipelines`, `pipeline_s3_state`, `pipeline_s3_runs`, `pipeline_s3_files` | Run existing S3 pipeline deletion before local cascade. Remove unpublished artifacts and upload IDs. Published customer-destination outputs remain customer-owned, matching the existing pipeline deletion contract. |
| `webhook_deliveries`, `webhook_members`, `webhook_cooldowns`, `webhook_queue_usage` | Drain active deliveries, delete pipelines and cascading delivery state, and remove per-project accounting. Already delivered external HTTP requests cannot be withdrawn. |
| Journal `projects`, `tenants`, `shared_tenant_cache`, `tenant_usage` | Remove local anchors and tenant cache after all local projects finish and final cumulative usage publication succeeds. Cache state never authorizes a deleted tenant. |
| Journal `access_keys` | Local uncredentialed actor anchors can be shared across projects. Remove tenant-owned anchors only when no surviving job references them. Preserve global human anchors needed by another tenant. |
| `restored_artifact_holds` | Before the current offline credential reset removes export/pipeline checkpoints, commit opaque ownership holds separately. No source requests occur. These holds block tenant logical completion until source ownership and current artifacts are reconciled. |
| `managed_artifact_ownership` | Retain opaque object/upload ownership and hashed namespace after logical cleanup. Keep it outside tenant/project foreign keys until physical versions are verified absent. |
| `metadata` | Existing project-specific delivery and receipt keys disappear. Keep deployment identities, schema metadata, backup bindings, and other tenants' keys. Classify any new tenant-specific metadata before adding it. |
| `tenant_credentials`, tenant-owned `access_keys`, `oauth_clients`, `service_api_keys`, `oauth_tokens` | Revoke atomically at start. Delete tenant-owned token, key, and client records after project jobs drain, in foreign-key order. Follow explicit ownership, not the creating human. |
| `tenant_memberships`, `oauth_grant_tenants` | Remove only the target tenant's records. Revoke and remove its tenant-admin consent principals and dependent access, refresh, and code records. |
| Global `email_humans`, `oidc_identities`, browser sessions, OAuth application registrations | Preserve globally shared identity and registrations. Remove project scopes for deleted projects without revoking still-authorized B connections. |
| `oauth_interactive_grants`, `oauth_authorization_codes`, `oauth_refresh_tokens`, `oauth_interactive_access`, `oauth_authorization_requests` | Remove tenant-owned grants and their dependent records. For grants spanning A and B, prune A project scope and keep B. An already pending consent must re-evaluate current ownership and fences before issuing credentials. |
| Control `tenant_usage`, `worker_tenant_usage` | Freeze counters outside the tenant foreign key before removal. Never refund usage. `tenant_deletion_usage` preserves opaque tenant and project IDs, UTC period, and cumulative counts only, without labels, identities, or event data. Existing acceptance counters have no per-project split, so their project ID is empty. |
| `shared_project_owners`, `shared_query_permits` | Existing project FK cleanup removes ownership and permit rows after project drain. Do not alter permit lifetime behavior owned by phase 1. |
| `shared_workers`, route nonces, cookieless keys, email delivery budgets | Deployment state shared across tenants. Preserve. |
| Native `catalogs`, `cleanup_progress`, project directory and S3 project prefix | Compose native `DeleteProject`. Preserve native deletion guards in `deleting_projects` and `pc_native_projects`; remove the catalog schema and active managed files. |
| S3 noncurrent versions and PostgreSQL PITR backups | Retain or purge according to explicit backup policy. Current object-list deletion does not prove physical removal. Restore must reapply current deletion tombstones before accepting traffic. |

## Fence coverage

| Caller | Required check |
| --- | --- |
| Huma project operations, including owner-free reads and rename | Check active authoritative project and tenant before dispatch. Deletion retry has a narrow internal cleanup exception. The exception must not permit public resource access. |
| MCP and generated CLI | Use the same operation authorization and HTTP dispatch. No separately maintained allowlist. |
| Native, PostHog, and Segment capture | Resolve project from credentials, then check control fences before admission. A cached policy or an external JWT never bypasses the fence. |
| Local journal acceptance and imports | Serialize admission against deletion and recheck before commit. Drained operations cannot create new tenant mirrors afterward. |
| Shared journal acceptance | Check authoritative deletion state on every batch. The ten-second budget cache is insufficient for revocation. Already running owner work is canceled and drained before local cleanup. |
| Hosted memory admission and flush | Check the same authoritative fence at admission and inside the guarded native commit. Serialize the deletion fence with the commit guard, or retain a control row lock through commit, so a stale batch cannot commit after deletion. Phase 3 and phase 4 must implement this requirement. |
| Shared query permits and native queries | Existing permit admission locks tenant and project. Atomically installed project fences prevent new query admission. Cancel and drain existing calls before physical cleanup. |
| Export, S3 pipeline, and webhook workers | Check authoritative project activity before execution and publication, even when the actor is a global operator. Local `activity` registration alone is insufficient. |
| Tenant reads, mutations, membership, service-account and key creation | Reject tombstoned tenant IDs. Recheck inside the transaction which performs the mutation. |
| Project and tenant lists, OAuth consent and refresh | Filter fenced resources and intersect grants with current ownership. No stale all-project tenant claim can create or discover a deleted project. |
| Usage publication and startup initialization | Handle tombstones without recreating deleted tenant rows or failing the whole worker's other tenant publications. Check default-tenant tombstone during bootstrap. |
| Native maintenance, replay, project creation, restore | Preserve the native project guard and deletion registry. Restoring an old catalog must not clear a current tenant or project tombstone. |

The existing grant-pruning helpers trigger wholesale human grant revocation through `email_grant_change` and `oauth_human_client_revocation`. Tenant deletion leaves globally shared human and OAuth client parent aggregates unchanged. Those records may keep inert opaque old project IDs. Public actor, list, consent, client issuance, and refresh paths filter missing or fenced projects. Each tool principal and frozen token scope loses A, so mixed A/B grants keep B and A-only grants are revoked. Tenant memberships and owned credentials disappear. No trigger is changed or disabled. General human permission changes retain their current invalidation behavior. A normalized human-grant redesign is a separate task.

## Verification and evidence

Use real temporary SQLite databases for rollback, restart, idempotency, default-tenant deletion, mixed-tenant OAuth scope, denied stale credentials, and pending consent tests. Use two tenants with at least two A projects and one B project. Check every management operation that accepts a project ID, including owner-free rename and schema calls.

Run the existing native project deletion and pipeline cleanup tests, then add interrupted tenant cleanup with a failed managed-object deletion and restart. Verify B events, credentials, OAuth refresh, exports, memberships, and counters remain intact. A valid external-token-shaped principal must fail for a fenced target even before the external JWT feature exists.

Coordinate real PostgreSQL and S3 tests with the root test schedule. Preserve logs under `../artifacts/tenant-deletion`. Verify versioned-S3 status wording separately from current-object absence. Regenerate contracts with `make cli-generate` and prove the CLI still builds with `CGO_ENABLED=0`.

## Policy decisions

1. Does tenant deletion override backup retention, or remain `logically_deleted` until physical versions expire and are verified absent? Decision: report logical deletion and leave physical purge pending. Never bypass retention or claim that historical bytes disappeared.
2. Can cumulative usage survive as unlabeled tenant-ID and period totals outside tenant foreign keys? Decision: preserve the minimal operator-only audit. This avoids silently refunding or erasing accounting history. No prices or billing policy are introduced.
3. Does deletion preserve published objects in a customer's pipeline destination? Decision: preserve published customer artifacts, matching existing project-pipeline behavior. Clean only artifacts still owned by unfinished Porcupine jobs.
4. Is the default tenant deletable? Decision: yes, with startup honoring its tombstone and future omitted-tenant creates returning 404.

## Fact ledger

| Source | Destination | Preserved fact or decision |
| --- | --- | --- |
| User control-plane supplement, tenant deletion | Operator contract and cleanup loop | One authenticated operator operation shared by HTTP, MCP, and CLI. Immediate fence, owned credential revocation, existing project cleanup, restart recovery, and status. |
| Existing project deletion, native deletion, and pipeline tests | Ownership inventory and implementation | Compose analytical/export cleanup and unpublished pipeline cleanup. Preserve published customer outputs and native no-resurrection tombstones. |
| User hosted architecture and root policy review | States and backend limitation | No physical-purge claim without applicable version proof. Preserve PITR retention. Owner-local shared completion waits for the shared operational-state backend. |
| Existing human revocation triggers and two-tenant requirement | Scope pruning and public authority views | Preserve shared humans, B memberships, B machine credentials, and mixed-tenant B tool connections. Keep inert global IDs without effective A authority. |
| Existing cumulative acceptance accounting and usage policy review | `tenant_deletion_usage` | Keep minimal opaque IDs and numeric UTC period totals without refunds, event bodies, identifying labels, or a pricing decision. |
| Shared admission component tests and immediate revocation requirement | Authoritative project fixtures and shared deployment guide | Local-only project mirrors never authorize admission. Every batch requires an available active control fence even with fresh cached budgets and reserved IDs. After successful credential validation, an unavailable admission check refuses with retryable 503 before queue, identity or quota changes; missing or tombstoned projects retain terminal 404 at this boundary. The earlier built-in credential lookup still maps its control failures to 401. The earlier fresh-cache offline acceptance expectation is intentionally removed. |

Standalone native deletion retains a permanent `deleting_projects` fence. Successful cleanup records `cleanup_progress.state=deleted` atomically with catalog removal. Checkpoints admit terminal fences and still reject unfinished deletion, including a pending project that never had a catalog.

Managed export keys contain job and attempt IDs rather than the project ID. `managed_artifact_ownership` preserves their opaque tenant/project ownership, object and multipart-upload IDs, and a SHA-256 storage-namespace fingerprint before cleanup removes the last job checkpoint. Ordinary export expiration and unpublished pipeline cleanup preserve this evidence too, so earlier cleanup does not lose ownership needed by a later tenant deletion. Published customer pipeline outputs are excluded. No SQL, payload, credentials, cleartext bucket names, or identifying destination labels enter this inventory. A future purge verifier must match the original configured namespace and verify applicable versions and retention before removing inventory or reporting `purged`. Export checkpoints bind their storage fingerprint before the first external upload and preserve the binding when moved into the deletion queue. A changed destination or a legacy empty binding leaves cleanup pending before any Abort/Delete. Existing objects whose last ownership checkpoint was discarded by an older build cannot be reconstructed by this migration. An operator must reconcile that unknown provenance; it cannot support a physical-purge claim. Inventory presence alone is not physical-purge proof.

## Current offline restore limitation

The current offline restore resets credentials and deliberately neutralizes outbound jobs. It must not send deliveries or contact the source export/pipeline buckets. Before any reset can erase the last object reference, restore commits `restored_artifact_holds` in control storage, outside tenant/project foreign keys. The hold contains only authoritative tenant/project IDs, artifact kind, a known namespace fingerprint or an empty unknown binding, generated object keys, and upload IDs. It retains no SQL, event body, destination labels, or credentials. Numbered migrations apply before this safety capture; retries preserve existing holds without duplication.

A restore interruption after this commit cannot lose the references when reset retries. A subsequent tenant deletion blocks access and cleans its current native project state, but stays `deleting` with `restored_artifacts_pending` while holds remain. B keeps its restored data and recovery credentials. Reset and deletion never infer permission to delete the source objects.

Reconciliation must establish the original storage namespace, distinguish customer-published pipeline outputs from unfinished managed files, and prove that applicable current objects and multipart uploads are handled. No generic ignore-hold operation can declare completion. This release does not automate that reconciliation. Retain the inventory and storage evidence for the operator's recovery procedure. The phase 6 hosted target preserves operational keys and resumable cleanup instead of applying this access reset.
