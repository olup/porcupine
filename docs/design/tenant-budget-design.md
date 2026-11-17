# Tenant and budget boundaries

This change adds tenant ownership and operator-configured budgets to the existing
single-process SQLite deployment. It does not add a hosted billing service,
PostgreSQL, distributed workers, or isolated analytical compute.

## Chosen structure

Tenants own projects and budgets. Producers and agents retain explicit project
grants; a person can hold grants to projects in several tenants. The deployment
operator alone creates tenants or projects, changes budgets, suspends ingestion,
and reads aggregate tenant usage. A project reader sees effective query limits
without learning activity from projects outside their grants.

Project ownership is immutable. The server resolves tenant ownership from its
project registry, never from a producer's payload. A fresh installation creates
one default tenant. Existing pre-tenant databases are rejected explicitly; this
pre-launch change provides no schema backfill or data migration.

An operator-issued wildcard remains an explicit deployment-wide project grant.
It is not a tenant-local wildcard and does not grant operator capabilities.
Delegation must remain within the issuer's permissions and project scope.

## Transactions and execution

Monthly usage is the number of new canonical rows durably accepted, including
identity controls and accepted historical/person snapshot rows. Live acceptance
and historical import transactions own both the events and their usage increments.
Invalid, ignored, duplicate, and rolled-back rows consume nothing. UTC calendar
months are attributed at acceptance, independent of historical occurrence dates.
Retries after delivery or deletion do not charge again. Deleting a project does
not erase usage. There is no manual reset endpoint.

The current SQLite implementation enforces allowances transactionally, without a
distributed cache or broker. A quota failure preserves historical upload checkpoints
for resumption after a budget increase or the next month. PostHog's intentional
quota refusal is terminal; storage failures must remain failures. Native and Segment
requests receive an explicit quota error with no fabricated event receipts.

Interactive SQL, catalog and profile operations share tenant admission and the
existing deployment-wide query safety cap. A mutex-protected count supports policy
changes without replacing channels held by active requests. All exits release their
slots. Effective query limits are the minimum of tenant budgets and configured
deployment ceilings, passed explicitly to native execution. Background exports and
pipelines keep their separate execution budgets and existing global admission.
Project schema discovery exposes configured tenant budgets, deployment ceilings
and effective query limits so an agent can explain refusals. It does not expose
tenant-wide usage or grant permission to change policy.

## Design comparison

Two independent sketches considered project-owned tenant metadata and a bound
project/tenant context. The minimal project-ownership model is the base. It keeps
the existing project-grant security model, with server-side tenant resolution and
operator-only administrative operations. A full tenant-membership and customer-admin
model would require coordinated OAuth, credential and revocation changes beyond this
scope. It is not implied by exposing tenants.

The second sketch contributed trusted ownership resolution, resumable import quota
errors, and explicit native cancellation/output checks. Blanket rejection of global
read-only wildcard grants was rejected: an operator can intentionally delegate
global read access without granting budget management. Project creation becomes
operator-only to remove the existing unbound creation capability.

Verification must cover two projects sharing one tenant, independent tenants,
atomic batch rollback, stable retries, import resumption, monthly rollover, operator
and project-grant boundaries, actual native cancellation, increased and decreased
result budgets, and generated HTTP/CLI/MCP contract parity.
