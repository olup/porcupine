# Scope and roadmap

The current release is **v0.15.0**. [CHANGELOG.md](../CHANGELOG.md) records released changes and the next version's work. This page tracks priorities and decisions rather than repeating release history.

## Stabilization

The v0.14.0 [review follow-up](design/archive/review-v0.14.md) records the findings and fixes. See the [changelog](../CHANGELOG.md) for the v0.15.0 changes.

## Validate the complete team workflow

Before building the commercial service, run the following through API, CLI, and MCP with test data:

- [ ] An operator creates a tenant with budgets and invites its first administrator.
- [ ] The administrator connects an agent, creates projects, and invites a colleague as a project reader.
- [ ] A team-owned service credential ingests events independently of its creator.
- [ ] The reader discovers events, queries them, sees usage and limits, and receives useful errors at a limit.
- [ ] Revocation affects existing connections without affecting another tenant or independent team credentials.
- [ ] Repeat in standalone and hosted with retries, restart, identity changes, edits, and deletion.

Audit management coverage at the same time. No step should require SSH or database edits. Check hosted tenant deletion and test replacing a pipeline by creating a new rule.

## Next operational protections

- [ ] Validate noisy-tenant behavior and bounded memory admission in the [hosted architecture](design/hosted-architecture.md). The SQLite project-counter experiment is stopped and unshipped; its measured admission overhead is not a hosted prerequisite.
- [ ] Evaluate tenant-wide rate limiting across workers, separately from monthly allowances.
- [ ] Measure sustained ingestion, freshness, memory, disk growth, maintenance, and recovery with many quiet tenants and one noisy tenant.
- [x] Validate receipt pruning and retries after edits/deletes, storage failure, and restart, including person snapshots and two-project concurrency.
- [x] Default standalone receipt retention to zero and document opt-in pruning's historical lookup dependency, in standalone. Hosted deduplicates at durable flush. Compact-receipt experiments remain unshipped; see the [hosted decisions](design/hosted-architecture.md) and prior [lookup measurements](design/stabilization-validation.md#cost-of-historical-id-lookup).
- [ ] Measure DuckLake schema savings and query behavior on representative payloads and larger data sets. Profile JSON extraction before promoting common properties to typed columns. Source timestamp text remains for precision; a small fixture is not a capacity benchmark.

## Hosted reliability

The hosted profile uses bounded memory for live admission. Further work includes measuring sustained visibility and loss during worker replacement, PostgreSQL or S3 outages, and contention across tenants. [Hosted architecture](design/hosted-architecture.md)

Evaluate an optional worker disk spool that replays pending work after a restart on the same surviving volume. The current hosted acknowledgment covers memory admission.

## Product work after stabilization

The historical import workflow proposed after v0.10.0 shipped in v0.14.0. The [import guide](imports.md) covers native, PostHog, and Segment formats, resumable CLI uploads, retry identity, and waiting for analytical delivery. The [analytics reference](reference/analytics.md) and [tested DuckDB recipes](reference/duckdb-recipes.md) cover SQL feedback and common analytical questions.

The [control-plane contract work](design/control-plane-contracts.md) adds operator tenant deletion, optional external JWT authentication, and precise paginated usage, in that order. Integrated self-hosted identity and service credentials remain available; commercial pricing and UI stay outside Porcupine.

- Expand import adapter coverage from actual export files. The native event contract stays the common format. A plugin framework and additional SDK adapters remain undecided.
- Consider scheduled SQL-to-webhook delivery with alert conditions and suppression using existing durable destination jobs.

## Deferred

Saved parameterized queries and report collections remain on hold. A server-side graph renderer is not a priority when the client can render results. Automatic event documentation and conflicting-description reconciliation also remain on hold.

Separately provisioned analytical compute needs a common task and cancellation contract before choosing Lambda, containers, or VMs. Keep bounded interactive queries and current asynchronous exports; do not add speculative query-cost prediction.

General user aliasing, identity merging, and unlinking remain outside the supported identity contract. Feature flags, session replay, logs, and tracing are outside the present product analytics scope.
