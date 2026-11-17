# Authentication boundaries and possible simplification

The extraction keeps every existing authentication mode and permission rule.
It changes package ownership, without changing the HTTP, CLI, or MCP contract.

## Current ownership

`internal/auth` owns principals, project and tenant authorization, consent
attenuation, OAuth input validation, and upstream OIDC verification. `auth.Login`
also owns browser OTP/OIDC sign-in, temporary sessions, cookies, CSRF and provider
state, and local/shared email-delivery admission. It has no access to the event
journal or analytical store.
An upstream identity proves an email and subject; it never grants project access.

`internal/porcupine` owns account persistence, route wiring, and consent. It rechecks
current memberships and credential authority in the control database. Identity
linking, recovery, consent issuance, and token revocation retain their existing
transactions and login-version checks. A three-operation `LoginAccounts` adapter
looks up an invited email, validates a current proof, or links a verified provider
identity. It returns the account ID and proof version, never cached permissions.
Consent rereads authority and verifies the proof again when issuing a grant.

Temporary browser-session storage lives in `auth.Login`. It keeps the existing
in-memory and shared PostgreSQL behavior. Schema creation remains in the core's
numbered migrations; Login only uses the existing tables. Provider initialization
and offline configuration validation keep their original startup order.

The authorization-start gate and local email-delivery limiter own their locks.
They do not take the browser-session lock. Shared delivery budgets continue to
use PostgreSQL transactions. Operation metadata still supplies one policy to
HTTP enforcement and MCP discovery.

## Proposed product model, pending a decision

Humans authenticate with email OTP or a configured OIDC provider, then authorize
tools through OAuth with PKCE. OTP remains a complete option for installations
without an external identity provider.

Machines use team-owned service accounts and individually revocable, scoped API
keys. Public ingestion keys remain separately restricted to event submission.

The remaining simplification candidates are direct access-key administration,
persistent bootstrap credentials, and machine `client_credentials` exchange.
Removing them would change setup, recovery, and API clients. Review those callers
and agree on replacements before removing a mode. No mode is removed here.

## OAuth dependency boundary

The server pins `github.com/go-oauth2/oauth2/v4` at v4.6.0. Its manager and server
are used for machine `client_credentials` issuance. Interactive authorization,
PKCE, refresh rotation, and revocation use Porcupine's transaction code.
Upstream OIDC verification uses `coreos/go-oidc`; upstream exchanges use
`golang.org/x/oauth2`.

The pinned dependency and its protocol adapters still need vulnerability checks
and contract tests. This extraction makes no claim about upstream maintenance
activity or security. Replacing the library would be a separate change; deleting
machine OAuth would first require the product decision above.

## Verification

The existing protocol tests exercise malformed requests, redirect and PKCE
binding, consent attenuation, OTP replay, OIDC claims, project isolation, and
recovery invalidation. Pure authorization tests also check that grants from two
projects cannot combine into wider access and that callers cannot mutate the
shared permission catalog.

The browser extraction moves six session/OTP tests to `internal/auth` without
renaming them. Both shared integration runners include that package, so its
cross-worker tests still run against actual PostgreSQL. The browser and recovery
journeys remain in `internal/porcupine`.
