# External authentication

The default `builtin` mode keeps Porcupine's email OTP, Google/OIDC sign-in, OAuth with PKCE, and consent. External mode replaces that identity provider for the whole instance.

Porcupine remains the resource server. It verifies access tokens, binds each project to its current tenant, and enforces the same API and MCP permissions. Public ingestion keys and revocable native machine keys work in both modes.

## Configure the server

Start with fresh control storage. An existing control database cannot change authentication mode or adopt another mode's identities. Both standalone and hosted support either authentication mode.

Remove configured `bootstrap_token`, `oauth`, `email`, and `oidc` values, including matching environment overrides. External mode rejects builtin provider and SMTP settings before opening storage. Configure a trusted issuer and its signing-key endpoint:

```json
{
  "auth": {
    "mode": "external",
    "public_url": "https://data.example.eu",
    "external": {
      "issuer": "https://identity.example.eu/realms/team",
      "jwks_url": "https://identity.example.eu/realms/team/protocol/openid-connect/certs",
      "api_audience": "https://data.example.eu/api/v1",
      "mcp_audience": "https://data.example.eu/mcp",
      "algorithms": ["RS256"],
      "token_types": ["at+jwt"],
      "clock_skew": "30s",
      "max_token_lifetime": "15m"
    }
  }
}
```

`public_url` is the instance's public origin. Protected-resource metadata uses its `/api/v1` and `/mcp` URLs. The two JWT audiences default to those URLs in file configuration, but can be distinct explicit identifiers. The external authorization server must issue the correct audience for each requested resource.

The issuer may contain a path and must match `iss` exactly. `jwks_url` is explicit; startup does not discover or contact the provider. URLs require HTTPS, except HTTP loopback for development. Tokens cannot choose a key URL or redirect the key fetch.

Configuration environment equivalents use `PORCUPINE_AUTH_MODE`, `PORCUPINE_AUTH_PUBLIC_URL`, and `PORCUPINE_AUTH_EXTERNAL_` followed by `ISSUER`, `JWKS_URL`, `API_AUDIENCE`, `MCP_AUDIENCE`, `ALGORITHMS`, `TOKEN_TYPES`, `CLOCK_SKEW`, or `MAX_TOKEN_LIFETIME`. Algorithm and token-type lists are comma-separated.

## Access-token contract

Access tokens must follow the JWT profile for OAuth access tokens (RFC 9068). The JOSE header contains exactly `alg`, `typ`, and `kid`, with no other parameters such as `jku`, `jwk`, `x5c`, or `crit`. The default `typ` is `at+jwt`. Alternative access-token types require explicit configuration. Generic `JWT` and `application/jwt` types are rejected. An upstream sign-in ID token is not an API credential. Configure the provider to issue RFC 9068 access tokens for the Porcupine audiences; tokens that only follow the provider's own format are rejected at the header.

Tokens require a valid asymmetric signature, `kid`, exact issuer, one exact audience, subject, integer `iat` and `exp`, and known space-separated `scope` permissions. `nbf` is optional. Sender-constrained `cnf` tokens are rejected because this mode accepts Bearer tokens without a separate possession proof. API tokens cannot call MCP, and MCP tokens cannot call the API directly. Tokens expire at `exp`; clock tolerance does not extend their authority past expiry. The maximum accepted lifetime defaults to 15 minutes and is configurable up to one hour. Five-minute tokens reduce the revocation window.

The fixed `porcupine` object carries authorization. A selected-project token contains:

```json
{
  "iss": "https://identity.example.eu/realms/team",
  "sub": "stable-provider-subject",
  "aud": "https://data.example.eu/api/v1",
  "iat": 1800000000,
  "exp": 1800000300,
  "scope": "management:read events:read",
  "porcupine": {
    "tenant_id": "TENANT_ID",
    "project_scope": "selected",
    "projects": ["PROJECT_ID"]
  }
}
```

For all active projects of one tenant, use `project_scope: "all"` and omit `projects`. Future projects are included when the next request resolves authority. A tenant administrator additionally needs `tenant:admin` in `scope`. A tenant token cannot claim `instance:admin` or use a project from another tenant. Project IDs never establish tenant ownership.

An operator token instead has `scope: "instance:admin"` and exactly `"porcupine": {"operator": true}`. It contains no tenant or project authorization fields. Operator tokens still respect deletion fences on target resources.

Unknown permissions, duplicate JSON keys, unknown or incorrectly cased authorization fields, null values, mixed operator/tenant claims, and wildcard project IDs are rejected. Selected lists contain at most 64 distinct project IDs of 128 bytes each. The bearer token limit is 16 KiB. Identity claims such as email and groups do not grant access or create local human accounts.

Supported algorithms are RS256/384/512, ES256/384/512, and EdDSA. A usable signing key has `use` absent or `sig`, an allowed algorithm, a `kid` of at most 128 bytes, and public verification material. RSA keys must have between 2048 and 8192 bits. EC algorithms must match their curves. A present `key_ops` on a signing key must allow verification only.

Porcupine ignores keys that cannot verify an access token, so a provider can publish them alongside its signing keys: encryption keys (`use` other than `sig`, or `key_ops` without `verify` or `sign`), unsupported key types and curves, algorithms outside the configuration, and keys without a usable `kid`. A key without `alg`, as Microsoft Entra ID publishes, gets an algorithm only when exactly one configured algorithm fits its type: an RSA key when the configuration allows exactly one of RS256/384/512, an EC key on P-256, P-384, or P-521 when the matching ES256, ES384, or ES512 is configured, and an Ed25519 key when EdDSA is configured. Otherwise the key is ignored; with both RS256 and RS384 configured, an RSA key needs `alg`.

The whole key set is rejected, as during an outage, when the document is not a JWK Set, publishes private or symmetric key material, gives a parameter the wrong JSON type, or contains a usable key that is ambiguous or broken: two usable keys sharing a `kid`, an allowed `alg` the key material cannot verify, a signing key whose `key_ops` is not exactly `["verify"]`, or invalid or undersized material.

The key fetch has a three-second timeout, a 256 KiB response limit, and a 32-key limit. The cache lasts five minutes. Unknown keys can trigger one coalesced refresh at most every ten seconds. A fresh known key works during a provider outage. When refreshes fail after the cache expires, keys from the last successful fetch keep working for up to one hour; Porcupine logs the failure and the instance status reports the component as degraded. After that grace, or for keys missing from the cache, verification fails closed. Provider outages return retryable `503` responses when keys are needed, without blocking native keys or public ingestion. The grace covers failed fetches only: a key removed from a successfully fetched set stops working at that refresh, which happens by cache expiry at the latest. There is no token introspection or upstream revocation callback.

## Connect tools

The protected-resource endpoints advertise the external authorization server:

- `/.well-known/oauth-protected-resource/api/v1`
- `/.well-known/oauth-protected-resource/mcp`

Register each interactive client at that server. For CLI login, supply its public client ID:

```sh
export PORCUPINE_URL=https://data.example.eu
porcupine login --client-id YOUR_EXTERNAL_PUBLIC_CLIENT
porcupine list-projects
```

The CLI discovers the issuer, requires PKCE S256 and issuer identification in authorization responses, and keeps the API resource through token exchange and refresh. Authorization and token endpoints must belong to the discovered issuer's origin. Access-only sessions remain usable until expiry; reconnect when no refresh token is available.

An MCP client connects to Porcupine's `/mcp`, then follows the external issuer's authorization flow with the MCP resource. This replacement is separate from builtin Google sign-in: Google login alone does not provide Porcupine tenant and project grants.

## Native machine accounts and deferred work

Tenant administrators create native service accounts and scoped API or MCP keys through the existing tenant management operations. Operators can create a global machine account through `create-oauth-client`, then issue a revocable operator key through `create-service-api-key`. The existing internal client record is machine-only in external mode: no interactive registration, OAuth secret, or local token exchange is available. Tenant machine accounts cannot gain instance administration.

A finite export stores only an audit subject digest, its admitted project/tenant, issuer, and original expiry. It stores no bearer token, refresh secret, or human account. Execution and publication stop at the original expiry and recheck current project ownership and deletion state. Expired execution reports `authorization_expired` and cleans unfinished output. A retry never renews that execution authority.

The current SQLite journal keeps inert foreign-key anchors for audit subjects and tenant ownership links. These rows contain no authorization grants and are never used to authenticate. Tenant cleanup removes its links and removes an anchor only when no other tenant or job still references it. Hosted uses its PostgreSQL job stores.

A completed export keeps normal result retention. A freshly authorized user can retrieve it later, subject to current project permissions. Each download link is bounded by the current token's remaining lifetime and the object's retention.

Permanent pipelines require a request authenticated with a native machine key holding the required project permissions. An external human JWT receives `403` with `execution_credential_required` and instructions to use a team key. Porcupine never borrows another account by ID or turns a human JWT into indefinite authority. Builtin pipeline behavior is unchanged.

Historical imports reauthorize each request and check again before checkpoint commit. A refreshed token can resume the same issuer/subject/tenant's import. A different subject cannot take it over. Expiry does not undo events already accepted durably.

## Disabled builtin routes

These operations are absent from HTTP, OpenAPI, and MCP in external mode:

| Surface | Disabled routes or operations |
| --- | --- |
| Local authorization server | `/oauth/token`, `/oauth/revoke`, `/oauth/authorize`, `/oauth/consent`, `/.well-known/oauth-authorization-server` |
| Email and OIDC login | `/oauth/email/request`, `/oauth/email/verify`, `/oauth/oidc/start`, `/oauth/oidc/callback` |
| Local humans | `/api/v1/humans` and human identity linking/recovery operations |
| Local membership | Tenant member invitation, listing, update, and revocation |
| Consent grants | `/api/v1/oauth-grants` |

Tenant/project administration, native machine accounts, keys, ingestion, queries, imports, mutations, exports, and permitted pipelines remain available. External mode does not initialize or prune builtin human, browser, or consent state. Native machine-key revocation remains active in both SQLite and PostgreSQL.

The instance status reports `configuration.external_authentication` and the signing-key cache's `checks.external_authentication`. A stale cache or failed refresh degrades that component without changing `/healthz` or blocking public ingestion.
