# Scope and roadmap

The published release is **v0.14.0**. [CHANGELOG.md](../CHANGELOG.md) records released changes and the next version's work. This page tracks priorities and decisions rather than repeating release history.

## Stabilization in progress

The v0.14.0 [review follow-up](design/review-v0.14.md) records each finding and its status.

Completed on the development branch, not yet released:

- Tenants, team administration, project grants, monthly ingestion allowances, and configurable interactive query limits.
- Separate local SQLite control and ingestion databases, plus optional shared PostgreSQL control/catalog metadata and S3 analytical files.
- Read-only MCP discovery and query tools.
- Removal of inert analytical cache fields and obsolete backend configuration.
- Faster short tests and full race/integration checks, with versioned operational schema migrations.
- A separate `internal/lake` engine package.

Integrated on the stabilization branch and validated locally:

- Bounded local retry receipts with timestamp-independent historical deduplication, including import retries and refusal during analytical lookup failures.
- DuckLake schema v2 with monthly partitions, no full duplicated current payload, format checks, and interrupted-upgrade recovery.
- Smaller authentication, canonical-event, input, import, delivery, and S3 modules while keeping durable transaction ownership in the service.
- One typed self-host configuration file, private init/check/install commands, and a single Compose entry point. Real image tests cover nonroot setup, file overrides, custom storage paths, configuration replacement, and restart.
- Focused product guides, detailed references, and separate design records.

Authentication-mode removal remains a proposal rather than part of the extraction. The [validation record](design/stabilization-validation.md) records local results; release CI must still check the candidate.

## Validate the complete team workflow

Before building the commercial service, run the following through API, CLI, and MCP with test data:

- [ ] An operator creates a tenant with budgets and invites its first administrator.
- [ ] The administrator connects an agent, creates projects, and invites a colleague as a project reader.
- [ ] A team-owned service credential ingests events independently of its creator.
- [ ] The reader discovers events, queries them, sees usage and limits, and receives useful errors at a limit.
- [ ] Revocation affects existing connections without affecting another tenant or independent team credentials.
- [ ] Repeat in local and shared modes with retries, restart, identity changes, edits, and deletion.

Audit management coverage at the same time. No step should require SSH or database edits. Tenant deletion and replacing an existing pipeline's immutable rule remain explicit API/product gaps.

## Next operational protections

- [ ] Limit pending queue occupancy per project so one producer cannot fill the whole worker queue. Tenant monthly budgets do not provide this protection.
- [ ] Evaluate tenant-wide rate limiting across workers, separately from monthly allowances.
- [ ] Measure sustained ingestion, freshness, memory, disk growth, maintenance, and recovery with many quiet tenants and one noisy tenant.
- [x] Validate receipt pruning and retries after edits/deletes, storage failure, and restart, including person snapshots and two-project concurrency.
- [ ] Reduce and measure the admission cost after receipt pruning, particularly single-event sends and S3 lookup failures. Preserve stable IDs and atomic side effects; do not infer freshness from producer timestamps. See the [lookup benchmark](design/stabilization-validation.md#cost-of-historical-id-lookup).
- [ ] Measure DuckLake schema savings and query behavior on representative payloads and larger data sets. Profile JSON extraction before promoting common properties to typed columns. Source timestamp text remains for precision; a small fixture is not a capacity benchmark.

## Hosted architecture to evaluate

Reuse the published open-source server without private patches. A separate commercial service owns subscriptions, prices, invoices, and spending-cap calculations. It reads usage and changes technical budgets through an authenticated operator API, without direct database writes.

A possible hosting unit is a worker and its durable volume serving several tenants. Shared PostgreSQL holds control and DuckLake catalog metadata, S3 holds Parquet, and owner-local SQLite holds accepted work. Cells are a sizing option, not a decision to run one instance per customer. Capacity depends on resident engines and workload, not just tenant count.

Before offering availability guarantees, test queue-volume loss, ownership transfer, PostgreSQL and S3 outages, backup consistency, query-slot recovery, and tenant fairness. There is no automatic owner failover today. See the [shared deployment reference](reference/shared-deployment.md) and [SaaS/compute design notes](design/saas-and-compute-plans.md).

## Product work after stabilization

- Optional editable documentation for event definitions and property meanings, through API, CLI, MCP, and discovery. Record author and modification time; never gate capture on documentation.
- Expand import adapter coverage from actual export files. The native event contract stays the common format. A plugin framework and additional SDK adapters remain undecided.
- Consider scheduled SQL-to-webhook delivery with alert conditions and suppression using existing durable destination jobs.

## Deferred

Saved parameterized queries and report collections remain on hold. A server-side graph renderer is not a priority when the client can render results. Automatic event documentation and conflicting-description reconciliation also remain on hold.

Separately provisioned analytical compute needs a common task and cancellation contract before choosing Lambda, containers, or VMs. Keep bounded interactive queries and current asynchronous exports; do not add speculative query-cost prediction.

General user aliasing, identity merging, and unlinking remain outside the supported identity contract. Feature flags, session replay, logs, and tracing are outside the present product analytics scope.
