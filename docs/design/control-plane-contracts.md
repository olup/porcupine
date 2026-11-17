# Replaceable built-ins and the commercial control plane

**Design record.** External authentication and standalone tenant deletion have unreleased implementations. Expanded usage and the complete hosted architecture remain targets. This complements the [hosted architecture](hosted-architecture.md). The same open-source data service serves standalone deployments and the future commercial platform. The commercial application lives outside this repository and talks to authenticated APIs, never directly to Porcupine's database.

Porcupine owns ingestion, analytical storage, queries and enforcement of project/tenant permissions. It also provides integrated identity, management and operational measurements for self-hosting. A documented external replacement can disable an integrated component without weakening self-hosting or requiring a private fork. This is a boundary for complete responsibilities, not an interface for every function or a reserved billing hook.

## Identity and authorization server

`auth.mode` selects `builtin` by default or `external` explicitly in development builds. The [external authentication reference](../reference/external-auth.md) defines the implemented configuration, fixed claims, route matrix, and limits. The built-in server retains email OTP, upstream OIDC, OAuth/PKCE, consent and SMTP. Resource authorization remains in Porcupine in both modes; analytical identities from `identify` are unrelated to operator/user authentication and remain part of the event service.

In external mode, disable the built-in authorization server completely: no local human/email-account initialization or use, consent, SMTP send, authorization/token/login/registration routes or metadata claiming Porcupine is the issuer. Verify externally issued access JWTs for both API and MCP. `auth.external` needs:

- A trusted issuer and pinned JWKS URL. Server-side OIDC key discovery remains an optional future extension.
- A distinct expected resource audience for API and MCP.
- An operator-controlled algorithm allowlist and clock tolerance.
- A bounded documented mapping for tenant, explicit project IDs or all projects of that tenant, and known permissions such as `scope` values. Define accepted registered/configured claims and reject unknown or malformed authorization claims.

Require a valid signature, issuer, intended audience and expiry; do not use JWT-controlled key URLs or algorithm choices. Cache signing keys with bounded network requests and test rotation. Recommend short-lived access tokens and state their revocation window. Introspection or revocation lists remain optional future work, not a prerequisite.

Protected-resource metadata for API/MCP announces the external authorization server. External proof maps to the same `auth.Principal` and operation policies; project ownership in control data still binds each project to its tenant. A valid tenant-A token naming a tenant-B project does not grant access. A deleted-tenant fence applies regardless of token validity.

Public ingestion keys and native machine/service keys remain available in both modes. Externally issued `client_credentials` tokens are an alternative, not a reason to remove native keys. Prefer an external operator JWT with explicit `instance:admin` for the commercial service; a revocable native operator service credential is the documented alternative. Tenant service accounts cannot mint instance administration.

External finite exports store an audit subject digest and a project/tenant/issuer execution snapshot bounded by the original token expiry. Execution and publication stop at expiry, without storing a bearer, refresh secret, or local human. Completed results keep normal retention and require fresh caller authorization for download links. Imports reauthorize each request with a stable issuer/subject/tenant. Permanent pipelines require an authenticated native machine key; external JWT callers receive `execution_credential_required` instead of indefinite delegation. Tenant-admin transactions recheck verified claims and current ownership/deletion fences without reading native memberships. Builtin deferred work keeps current native creator reauthorization.

Tests must cover valid/expired tokens, issuer/audience/signature errors, key rotation, strict claim parsing, two tenants/projects, per-operation policy parity, external MCP discovery and absence of every built-in authorization-server route. Retain integrated tests and perform a manual integrated login before announcing it as validated.

## Operator usage contract

**Unreleased implementation:** the operator-only, paginated [`list-usage`](../reference/usage.md) operation accepts a period, optional tenant and day/month granularity, grouped by tenant/project. Its [measurement contract](usage-contract.md) fixes units, numerical retention, actual-return boundaries and replay behavior. Hosted native-commit publication remains a future phase:

| Measure | Required definition |
| --- | --- |
| Accepted live/import events | Separate newly retained event counts. No extra count for ingestion/checkpoint/posting retries; deletion never refunds usage. |
| Current Parquet bytes | Current referenced analytical files, excluding obsolete files. A sampled gauge needs its observation time and coverage; do not sum gauges as a cumulative counter. |
| Interactive query count/time | Define actual execution versus admission, wall time versus CPU, cancellations and crash estimates. Retrying a metering publication is distinct from executing a new query. |
| Export/pipeline bytes | Define final published object bytes and encoding. Presigned-download network traffic is not observable from publication alone. |
| Webhook deliveries | Define unique acknowledged delivery IDs separately from HTTP attempts and receiver-side duplicates. |

Use cumulative idempotent postings and a stable pagination watermark for external polling. Record accepted counts at the actual retention boundary: SQLite commit in standalone, recoverable native commit in hosted mode. Batch accounting rather than adding one database round trip or counter update per event. The [hosted effect protocol](hosted-architecture.md#d3-deduplication-and-effects-at-flush) must recover a crash between native commit and PostgreSQL posting. Prices, invoices, subscriptions and financial caps remain outside this repository.

## Resumable tenant deletion

The [tenant deletion implementation record](tenant-deletion.md) now describes unreleased immediate fences and standalone resumable logical cleanup. Owner-local shared finalization and physical-purge verification remain pending. The target below still defines the complete hosted contract.

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
| External-mode decisions above | [External authentication reference](../reference/external-auth.md) | Fixed access-token claims and configuration, disabled-route matrix, signing-key cache bounds, finite export delegation, import reauthorization, and native-key requirement for permanent pipelines. No builtin behavior is removed. |
| Existing tenant usage and output ACKs | Usage definition table | Preserve counters' causal boundaries and define additional units before billing consumers use them. |
| Existing project deletion and hosted recovery | Tenant deletion design | Compose existing cleanup, add tenant fencing/recovery, and distinguish logical denial from physical purge. |

No unreleased behavior is reclassified as published. External authentication and standalone tenant logical deletion require development builds. Expanded usage, complete hosted deletion, and physical-purge verification remain pending. Manual builtin login validation remains a release gate alongside the retained automated email/OIDC/OAuth journeys.
