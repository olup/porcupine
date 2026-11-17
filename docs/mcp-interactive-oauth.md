# Sign in with Google or another OpenID Connect provider

**Unreleased.** Use a development server and CLI from the same version. The published v0.14.0 OIDC configuration does not provide this combined login model.

Porcupine can verify an invited person's identity through Google or an operator-configured OpenID Connect provider. Email OTP remains available when SMTP is configured. Both methods use the same Porcupine human account, tenant memberships, and project permissions. The identity provider authenticates the person; it does not assign their Porcupine access.

Google sign-in requests `openid email profile`. It does not request Gmail access or permission to read mail. Tools still connect through Porcupine's OAuth server with PKCE. A Google access token or ID token is not a Porcupine API credential.

## Configure Google sign-in

1. Create a Google OAuth client for a web application in the [Google Cloud console](https://console.cloud.google.com/auth/clients).
2. Configure the consent screen and, if the application is in testing, its permitted test users.
3. Register the exact callback `https://analytics.example.com/oauth/oidc/callback`, replacing the origin with your Porcupine origin.
4. Store the client secret in a private deployment secret file.
5. Configure Porcupine and restart it.

```sh
PORCUPINE_OAUTH_ISSUER=https://analytics.example.com
PORCUPINE_OIDC_ISSUER=https://accounts.google.com
PORCUPINE_OIDC_CLIENT_ID=YOUR_GOOGLE_CLIENT_ID
PORCUPINE_OIDC_CLIENT_SECRET_FILE=/run/secrets/porcupine-google-secret
PORCUPINE_ADMIN_EMAIL=operator@example.com
```

`PORCUPINE_ADMIN_EMAIL` optionally seeds the original instance administrator. Use the intended administrator's address. Google authentication never promotes an ordinary invited account to administrator. An independent bootstrap credential remains the recovery path.

The secret file contains the Google client secret, with an optional trailing newline. `PORCUPINE_OIDC_CLIENT_SECRET` accepts the value directly instead; configure exactly one of the direct value and the `_FILE` variant. The secret is used only for the upstream code exchange and is not returned by status or management APIs.

For a development build with Compose, set these values in the private environment file, then run:

```sh
docker compose -f compose.yaml -f compose.oauth.yaml -f compose.oidc.yaml up -d --build
```

The OIDC overlay mounts the host secret file at `/run/secrets/porcupine_oidc_client_secret`. The image runs as UID 65532; that user must be able to read the mounted file. Keep its host directory private. Add `-f compose.email.yaml` to the same command if SMTP OTP and invitation delivery are configured. Do not add the v0.14.0 release image overlay for this unreleased feature.

Keep the issuer at a canonical origin: lowercase scheme and hostname, canonical IP and port spelling, and no default port such as `:443`. HTTPS is required except for loopback development. Register the exact development callback separately at the provider if it permits loopback.

For another trusted OIDC provider, replace the issuer and client credentials. The provider must support discovery, authorization code with S256 PKCE, signed ID tokens, and a verified email claim. Configure a provider you trust to verify the email addresses it asserts. Provider groups, domain names, and client-supplied claims do not grant Porcupine roles.

Discovery, authorization, token, and JWKS endpoints require HTTPS except on loopback. Discovery must succeed at startup. Back-channel requests have a ten-second timeout and do not follow redirects.

## Keep email OTP available

Configure [SMTP delivery](email-transport.md) to keep email codes alongside Google. The browser then offers both methods. A person who uses either method reaches the same Porcupine consent page and can connect another tool without another membership invitation.

With OIDC configured and no SMTP, invited people can still sign in with the provider. Porcupine authorizes their account and membership without sending an invitation notice. Give them the instance URL and the [tool connection instructions](connect-tools.md). No one-time login code is created by an invitation.

## Invite people before they connect

An instance administrator or authorized tenant administrator invites each person and assigns their tenant and project access. An uninvited account cannot obtain Porcupine access merely by signing in with Google. See [team setup](connect-your-team.md) and [human account management](email-accounts.md).

At the first provider login, Porcupine requires a verified email matching an active invited account and records the provider's stable issuer and subject against that account. Future sign-ins must match that binding and invited email. A different subject cannot claim the same person's existing provider binding. Changing an email at the provider does not move a Porcupine account or its memberships.

For Google, automatic first linking requires a Gmail address or an authoritative Google Workspace email, identified by a verified email and the signed `hd` claim. A Workspace account can use a secondary email domain; the `hd` value need not match that email domain. A Google account using an unrelated third-party email address must use email OTP instead. Google's `email_verified` claim alone is not current ownership proof for those addresses. [Google's ID-token guidance](https://developers.google.com/identity/gsi/web/guides/verify-google-id-token)

## Register the downstream client

The upstream Google client identifies Porcupine to Google. It is different from the downstream client that identifies Codex, a CLI, ChatGPT, or Claude to Porcupine. Never put the Google client secret in an agent's MCP configuration.

The local CLI and compatible local MCP tools use Porcupine's public PKCE clients. Run `porcupine login` for analysis, `porcupine login --tenant-admin` to select a tenant you administer, or `porcupine login --admin` for instance administration. Each administrative command requires that account's actual role. See [connect your tools](connect-tools.md) and [team administration](connect-your-team.md).

For a hosted client, register its exact callback with Porcupine through the OAuth client management API. Keep its requested scopes within the rights needed by that application. The registration limits what the application may request; the current human account and memberships limit what it can receive.

For example, an analysis client can request:

```json
{
  "name": "Analytics MCP client",
  "permissions": ["events:read", "management:read"],
  "projects": ["PROJECT_ID_A", "PROJECT_ID_B"],
  "grant_types": ["authorization_code", "refresh_token"],
  "redirect_uris": ["EXACT_CALLBACK_FROM_THE_CLIENT_SETUP"],
  "token_endpoint_auth_method": "none"
}
```

A public client uses `none` with S256 PKCE and has no client secret. A confidential client can use `client_secret_basic` or `client_secret_post` if supported. Do not enable `client_credentials` on a public interactive registration. Store any returned secret in the connector's secret store.

An API client requests the exact resource ending in `/api/v1`; an MCP client requests `/mcp`. After upstream login, Porcupine displays the downstream application, callback, permissions, and authorized tenant or project choices. Consent cannot add authority beyond the person's current rights and the application's registration.

## Sessions and revocation

Login and consent sessions last at most ten minutes. Cookies are HttpOnly and SameSite=Lax, with Secure and a `__Host-` name on HTTPS. State, nonce, upstream PKCE, cookie possession, and single-use CSRF checks protect the flow. Login and consent pages use `no-store` and cannot be framed.

The provider's signature, issuer, audience, expiry, authorized party, nonce, and verified email are checked before linking an account. No upstream bearer token is persisted as a Porcupine credential.

Use the human and membership APIs to revoke an account or its access to one tenant. Use `revoke-oauth-grant` to disconnect one tool, or revoke the downstream client to disable that application. Current local permissions remain authoritative even if the person can still sign in to Google. Removing a login provider does not transfer its identities to another issuer.

Pending browser sessions use the deployment's control database in shared mode, so login may complete through a different public worker. Revocation is checked against current authority on subsequent requests and token exchanges. Do not use ingestion-budget caches to decide authentication rights.

Instance operators can [list and unlink upstream identities](identity-recovery.md) for account recovery. Unlinking revokes that human's existing interactive connections and pending local sign-in proofs, while keeping their account and memberships. A fresh verified login can relink.

The automated tests use a signed mock provider to cover the browser code flow and rejection cases. A real Google login still requires operator-provided Google credentials and a correctly registered callback.
