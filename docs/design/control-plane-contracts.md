# Replaceable built-ins and the commercial control plane

See the [v0.15.0 changelog](../../CHANGELOG.md) and [hosting guide](../hosting.md) for released behavior. This complements the [hosted architecture](hosted-architecture.md). The same open-source data service serves standalone deployments and the future commercial platform. The commercial application lives outside this repository and talks to authenticated APIs, never directly to Porcupine's database.

Porcupine owns ingestion, analytical storage, queries and enforcement of project/tenant permissions. It also provides integrated identity, management and operational measurements for self-hosting. A documented external replacement can disable an integrated component without weakening self-hosting or requiring a private fork. This is a boundary for complete responsibilities, not an interface for every function or a reserved billing hook.

## Identity and authorization server

`auth.mode` selects `builtin` by default or `external` explicitly. The [external authentication reference](../reference/external-auth.md) defines the implemented configuration, fixed claims, route matrix, and limits. The built-in server retains email OTP, upstream OIDC, OAuth/PKCE, consent and SMTP. Resource authorization remains in Porcupine in both modes; analytical identities from `identify` are unrelated to operator/user authentication and remain part of the event service.

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

The operator-only [list-usage operation](../reference/usage.md) provides paginated numerical reports across projects and tenants. Both profiles count retained events at their durable commit boundary. Consumers upsert cumulative reports and reconcile the epoch after a restore. The commercial controller owns prices, invoices, subscriptions, and financial caps. [Usage boundaries](hosted-architecture.md#usage)

## Resumable tenant deletion

The operator-only `delete-tenant` operation commits an access fence and schedules resumable cleanup. It preserves other tenants and reports logical completion separately from physical-purge verification. [Hosted deletion](hosted-architecture.md#deletion)

## Deferred work

Operator lifecycle notification webhooks and an administration frontend remain deferred. A future external frontend uses the same API as the CLI and agents.

## Fact ledger

| Source | Destination | Preserved fact or decision |
| --- | --- | --- |
| User modularity supplement | This record and `AGENTS.md` | Integrated self-hosted implementations plus documented external contracts; no private fork or reserved SaaS hooks. |
| Existing built-in authentication | External-mode design above | Preserve its flows/tests; disable the authorization server only when external mode is explicitly selected. |
| External-mode decisions above | [External authentication reference](../reference/external-auth.md) | Fixed access-token claims and configuration, disabled-route matrix, signing-key cache bounds, finite export delegation, import reauthorization, and native-key requirement for permanent pipelines. No builtin behavior is removed. |
| Existing tenant usage and output ACKs | Usage definition table | Preserve counters' causal boundaries and define additional units before billing consumers use them. |
| Existing project deletion and hosted recovery | Tenant deletion design | Compose existing cleanup, add tenant fencing/recovery, and distinguish logical denial from physical purge. |
