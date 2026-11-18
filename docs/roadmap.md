# Roadmap

See the [product guides](README.md) for current behavior and the [changelog](../CHANGELOG.md) for release history.

## Validate the complete team workflow

Before building the commercial service, run the full workflow on standalone and hosted with test data and built-in authentication. Check management coverage through HTTP, CLI, and MCP so teams can manage their projects and credentials without SSH or database edits. [Team access](reference/tenant-access.md)

- As an operator, create a tenant, set budgets, and invite its first administrator. The administrator connects an agent, creates a project, invites a reader, and checks tenant usage. The reader discovers events, runs SQL, and gets an actionable error when a query limit is reached.
- Ingest using a team-owned credential. Revoke the human's access and verify that existing connections lose access while independent team credentials and another tenant keep working. Repeat with stable-ID retries, restarts, identity changes, and event edits and deletes.
- Test replacing an immutable pipeline rule by recreating the pipeline. Check that hosted tenant deletion fences access and reports cleanup progress. [Pipeline guide](pipelines.md) [Tenant deletion](reference/tenants.md#delete-a-tenant)

Then validate provisioning, budget updates, and usage reads from a separate service through the authenticated operator API. Keep customer provisioning, external identity, pricing, and billing in the commercial control plane. [Control-plane contracts](design/control-plane-contracts.md)

## Hosted reliability

- Measure sustained ingestion with many quiet tenants and one busy tenant while queries and maintenance run. Record capture-to-query delay, backpressure, memory, storage growth, and recovery. Measure acknowledged events lost during worker replacement, incomplete drains, and PostgreSQL or S3 outages. [Hosted architecture](design/hosted-architecture.md) [Visibility probe](reference/observability.md#measure-live-query-visibility)
- Evaluate a tenant-wide ingestion rate limit across workers, separate from monthly allowances. Evaluate whether an optional disk spool can replay pending events after a restart on the same surviving volume.
- Measure DuckLake storage and query behavior on larger, representative datasets. Profile JSON extraction before deciding whether to promote common properties to typed columns. Preserve source timestamp precision.

## Later decisions

- Extend import adapters using real export files and keep the native event contract as the common format. Decide whether more SDK adapters or a plugin framework are needed. [Import guide](imports.md)
- Consider scheduled SQL-to-webhook delivery with alert conditions and repeat suppression using existing durable jobs.
- Defer general asynchronous queries and separate analytical compute until there is a common task and cancellation contract. Keep bounded interactive queries and asynchronous exports. Choose compute provisioning later. Keep speculative query-cost prediction on hold. [Query and export guide](query-events.md) [Compute design notes](design/archive/saas-and-compute-plans.md#separate-analytical-compute-to-validate-later)
- Project editors can already describe exact event names and top-level properties. Automatic authoring and reconciliation of conflicting descriptions remain deferred. [Catalog descriptions](reference/event-discovery.md#document-an-event-or-property)
- Keep saved parameterized queries and report collections on hold. Clients can render SQL results, so server-side graph rendering remains a low priority.

General user aliasing, identity merging, and unlinking event identities remain outside the supported identity contract. Feature flags, session replay, logs, and tracing remain outside the product analytics scope.
