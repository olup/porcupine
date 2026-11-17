# Authentication boundaries and possible simplification

The extraction keeps every existing authentication mode and permission rule.
It changes package ownership, without changing the HTTP, CLI, or MCP contract.

## Current ownership

`internal/auth` owns principals, project and tenant authorization, consent
attenuation, OAuth input validation, upstream OIDC verification, and local login
admission limits. It has no access to the event journal or analytical store.
An upstream identity proves an email and subject; it never grants project access.

`internal/porcupine` owns account persistence and HTTP orchestration. It rechecks
current memberships and credential authority in the control database. Identity
linking, recovery, consent issuance, and token revocation retain their existing
transactions and login-version checks. Moving these transactions into callbacks
would conceal the authority checks, so this extraction leaves them together.
Browser-session storage remains beside those transactions, with its existing
in-memory and shared PostgreSQL behavior.

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
