# Replaceable built-ins and the commercial control plane

**Design target, not implemented.** This complements the [hosted architecture](hosted-architecture.md). The same open-source data service serves standalone deployments and the future commercial platform. The commercial application lives outside this repository and talks to authenticated APIs, never directly to Porcupine's database.

Porcupine owns ingestion, analytical storage, queries and enforcement of project/tenant permissions. It also provides integrated identity, management and operational measurements for self-hosting. A documented external replacement can disable an integrated component without weakening self-hosting or requiring a private fork. This is a boundary for complete responsibilities, not an interface for every function or a reserved billing hook.

## Identity and authorization server

`auth.mode` will select `builtin` by default or `external` explicitly. The built-in server retains email OTP, upstream OIDC, OAuth/PKCE, consent and SMTP. Resource authorization remains in Porcupine in both modes; analytical identities from `identify` are unrelated to operator/user authentication and remain part of the event service.

In external mode, disable the built-in authorization server completely: no local human/email-account initialization or use, consent, SMTP send, authorization/token/login/registration routes or metadata claiming Porcupine is the issuer. Verify externally issued access JWTs for both API and MCP. `auth.external` needs:

- A trusted issuer and pinned JWKS URL or its OIDC discovery.
- A distinct expected resource audience for API and MCP.
- An operator-controlled algorithm allowlist and clock tolerance.
- A bounded documented mapping for tenant, explicit project IDs or all projects of that tenant, and known permissions such as `scope` values. Define accepted registered/configured claims and reject unknown or malformed authorization claims.

Require a valid signature, issuer, intended audience and expiry; do not use JWT-controlled key URLs or algorithm choices. Cache signing keys with bounded network requests and test rotation. Recommend short-lived access tokens and state their revocation window. Introspection or revocation lists remain optional future work, not a prerequisite.

Protected-resource metadata for API/MCP announces the external authorization server. External proof maps to the same `auth.Principal` and operation policies; project ownership in control data still binds each project to its tenant. A valid tenant-A token naming a tenant-B project does not grant access. A deleted-tenant fence applies regardless of token validity.

Public ingestion keys and native machine/service keys remain available in both modes. Externally issued `client_credentials` tokens are an alternative, not a reason to remove native keys. Prefer an external operator JWT with explicit `instance:admin` for the commercial service; a revocable native operator service credential is the documented alternative. Tenant service accounts cannot mint instance administration.

The design must cover queued work, not just HTTP authentication. Current exports and pipelines recheck a persisted native creator credential. Decide how external-token authority, expiry and revocation constrain deferred work without creating native human accounts or silently persisting indefinite rights. Native service credentials are suitable for autonomous durable jobs. Also adapt tenant-admin management checks that currently reload native memberships.

Tests must cover valid/expired tokens, issuer/audience/signature errors, key rotation, strict claim parsing, two tenants/projects, per-operation policy parity, external MCP discovery and absence of every built-in authorization-server route. Retain integrated tests and perform a manual integrated login before announcing it as validated.

## Operator usage contract

Add an operator-only, paginated `list-usage` operation with period, optional tenant and day/month granularity, grouped by tenant/project. Define units and precision before implementing counters:

| Measure | Required definition |
| --- | --- |
| Accepted live/import events | Separate newly retained event counts. No extra count for ingestion/checkpoint/posting retries; deletion never refunds usage. |
| Current Parquet bytes | Current referenced analytical files, excluding obsolete files. A sampled gauge needs its observation time and coverage; do not sum gauges as a cumulative counter. |
| Interactive query count/time | Define actual execution versus admission, wall time versus CPU, cancellations and crash estimates. Retrying a metering publication is distinct from executing a new query. |
| Export/pipeline bytes | Define final published object bytes and encoding. Presigned-download network traffic is not observable from publication alone. |
| Webhook deliveries | Define unique acknowledged delivery IDs separately from HTTP attempts and receiver-side duplicates. |

Use cumulative idempotent postings and a stable pagination watermark for external polling. Record accepted counts at the actual retention boundary: SQLite commit in standalone, recoverable native commit in hosted mode. Batch accounting rather than adding one database round trip or counter update per event. The [hosted effect protocol](hosted-architecture.md#d3-deduplication-and-effects-at-flush) must recover a crash between native commit and PostgreSQL posting. Prices, invoices, subscriptions and financial caps remain outside this repository.

## Resumable tenant deletion

Add operator-only `delete-tenant` and a queryable deletion status. First commit an authoritative fence blocking ingestion and resource access, then revoke tenant-owned credentials, delete projects through the existing lifecycle path, and remove tenant control state. Persist progress so retries and worker restarts converge.

Do not disable a global human or its other-tenant memberships, credentials or data. Apply the fence to valid external JWTs too. Reuse project cleanup for analytical catalogs, managed S3 objects, imports, exports and pipelines. Memory flushes must recheck the fence before committing, so old buffered work cannot recreate deleted data.

Completion needs an explicit physical-purge boundary. Versioned S3 objects and the restore window can outlive logical blocking; report that state rather than claiming all objects disappeared. Reconcile cleanup with the hosted retention policy. Keep only the minimal operator status/idempotency tombstone and any explicitly defined aggregate-usage history; do not silently retain event bodies, access secrets or identifying labels.

Test two tenants, every affected credential/resource boundary, interruption/restart, S3 objects including applicable versions, cleanup errors, terminal status and preservation of the other tenant. A status record is not proof of physical purge.

## Delivery order and deferred work

Implement one focused PR per subject: tenant deletion, external authentication, then usage. Designs can proceed in parallel, while their shared permission/fence and durable-job boundaries are reconciled before implementation. Each PR records isolation tests, measurements/definitions, remaining limits and a source-to-destination fact ledger.

Operator lifecycle notification webhooks and an administration frontend remain deferred. A future external frontend uses the same API as the CLI and agents. No commercial application, notification service, billing rules or UI is implemented by this document.

## Fact ledger

| Source | Destination | Preserved fact or decision |
| --- | --- | --- |
| User modularity supplement | This record and `AGENTS.md` | Integrated self-hosted implementations plus documented external contracts; no private fork or reserved SaaS hooks. |
| Existing built-in authentication | External-mode design above | Preserve its flows/tests; disable the authorization server only when external mode is explicitly selected. |
| Existing tenant usage and output ACKs | Usage definition table | Preserve counters' causal boundaries and define additional units before billing consumers use them. |
| Existing project deletion and hosted recovery | Tenant deletion design | Compose existing cleanup, add tenant fencing/recovery, and distinguish logical denial from physical purge. |

No current reference behavior is reclassified as shipped. The three features above remain implementation work.
