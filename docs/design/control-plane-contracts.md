# Replaceable built-ins and the commercial control plane

Porcupine owns ingestion, storage, SQL, and project and tenant authorization. Self-hosted deployments include identity, management, and usage reporting. An external replacement can disable a complete built-in component through its documented contract. The commercial control plane belongs outside this repository and connects to the same authenticated operator API available to self-hosted deployments. [Hosted architecture](hosted-architecture.md)

## Identity and authorization server

The default `auth.mode: builtin` provides email OTP, upstream OIDC sign-in, OAuth with PKCE, and consent. Selecting `external` disables the built-in authorization server and uses access JWTs from a trusted issuer. Porcupine still binds the credential to the current project's tenant, enforces scopes, and checks deletion fences. Event identities created with `identify` remain part of the analytics service. Public ingestion keys and revocable native machine keys work in both modes. [External authentication](../reference/external-auth.md)

A commercial provisioner can authenticate with an external operator JWT carrying `instance:admin` or with a revocable native operator key. Tenant service accounts stay within their tenant. An export admitted with an external human JWT has execution authority only until the token's original expiry. Each import request needs fresh authority. Permanent pipelines require a native machine key in external mode. [Provision a tenant](../hosting.md#provision-a-tenant-from-a-private-service) [Deferred work authorization](../reference/external-auth.md#native-machine-accounts-and-deferred-work)

## Operator usage contract

The operator-only `list-usage` operation returns paginated numerical reports across projects and tenants. Both profiles count newly retained events at the durable commit boundary. The consumer replaces prior report values with the current values and reconciles the usage epoch after a restore. The commercial controller owns prices, invoices, subscriptions, and spending caps. [Usage API](../reference/usage.md)

## Resumable tenant deletion

The operator-only `delete-tenant` operation commits an access fence and schedules resumable cleanup. The operator reads logical completion separately from physical-purge verification. Other tenants retain access. [Hosted deletion](hosted-architecture.md#deletion)

## Deferred work

Operator lifecycle notification webhooks and an administration frontend remain deferred. An external frontend would use the same API as the CLI and agents. Server-side issuer discovery, token introspection, and revocation lists remain optional extensions.
