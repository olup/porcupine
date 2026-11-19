# Roadmap

[Current guides](README.md) · [Release history](../CHANGELOG.md)

## Validate the complete team workflow

Run this workflow with test data and builtin authentication on standalone and hosted deployments. Complete all management steps through HTTP, CLI, and MCP.

1. An operator creates a tenant, sets budgets, and invites its first administrator. The administrator connects an agent, creates a project, invites a reader, and checks usage. The reader discovers events, runs SQL, and gets an actionable error at the query limit.
2. Ingest through a team-owned credential. Revoke the human's membership and check that their connections lose access while the service credential and another tenant keep working. Exercise stable-ID retries, restarts, identity changes, edits, and deletes.
3. Replace an immutable pipeline rule by recreating the pipeline. Verify that hosted tenant deletion fences access and reports cleanup progress. [Team access](reference/tenant-access.md) · [Pipelines](pipelines.md) · [Tenant deletion](reference/tenants.md#delete-a-tenant)

Then verify that a separate service can provision tenants, set budgets, and read usage through the authenticated operator API. [Control-plane contracts](design/control-plane-contracts.md)

## Hosted reliability

- Run sustained ingestion with many quiet tenants and one busy tenant while queries and maintenance run. Measure capture-to-query delay, backpressure, memory, and storage growth. Measure recovery and acknowledged event loss during worker replacement, incomplete drains, and PostgreSQL or S3 outages. [Hosted architecture](design/hosted-architecture.md) · [Visibility probe](reference/observability.md#measure-live-query-visibility)
- Evaluate a tenant-wide ingestion rate limit across workers, separate from monthly budgets. Test whether an optional disk spool can replay pending events when a worker restarts on a surviving volume.
- Test DuckLake on larger, representative datasets. Profile JSON extraction before considering typed columns for common properties. Preserve source timestamp precision.

<a id="later-decisions"></a>

## Later improvements

- Add an optional per-project origin allowlist for browser ingestion so teams can restrict capture to their own sites on a shared instance.

Earlier proposals are in the design archive. [Imports and webhooks](design/archive/saas-and-compute-plans.md#other-planned-improvements) · [Query execution](design/archive/saas-and-compute-plans.md#separate-analytical-compute-to-validate-later) · [Documentation and reporting](design/archive/saas-and-compute-plans.md#deferred)
