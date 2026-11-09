# Interactive MCP authorization with upstream OIDC

Porcupine can authenticate a person through an operator-selected OpenID Connect provider before asking them to approve an MCP client. The upstream provider authenticates the person. Porcupine grants access only to projects explicitly assigned to that provider's verified subject ID.

This mode requires a public HTTPS Porcupine OAuth issuer. Use a lowercase scheme and hostname, canonical IP and port spelling, and omit default ports such as `:443`; noncanonical issuer representations are rejected so browser Origin, cookies and OAuth metadata agree. HTTP is accepted only for loopback development. Machine client credentials remain separate. There is no public client registration and no automatic access based on email domains, provider groups, or client-supplied project IDs.

## Operator setup

Register a confidential authorization-code client with the upstream OIDC provider. Set its exact callback URL to `https://analytics.example.com/oauth/oidc/callback`, replacing the origin with `PORCUPINE_OAUTH_ISSUER`. Enable the `openid` scope and S256 PKCE at the provider. Porcupine uses discovery, verifies signed ID tokens with `github.com/coreos/go-oidc/v3`, and separately checks the login nonce.

Configure:

```sh
PORCUPINE_OAUTH_ISSUER=https://analytics.example.com
PORCUPINE_OIDC_ISSUER=https://login.example.com/realms/analytics
PORCUPINE_OIDC_CLIENT_ID=porcupine
PORCUPINE_OIDC_CLIENT_SECRET_FILE=/run/secrets/porcupine-oidc-secret
PORCUPINE_OIDC_BINDINGS_FILE=/etc/porcupine/oidc-bindings.json
```

The secret file contains only the upstream client secret, with an optional trailing newline. Protect it with deployment secret-file permissions. It is read into memory and never written into the analytics database. Protect the bindings file as operator configuration too. Configuration is loaded at startup; changing either file requires a restart.

An example bindings file:

```json
[
  {
    "subject": "the-providers-stable-subject-id",
    "permissions": ["events:read", "management:read"],
    "projects": ["project-id-a", "project-id-b"]
  }
]
```

Use the provider's exact stable `sub`, not a display name or email address. The configured issuer and verified subject jointly select the account. Every binding must list explicit project IDs and non-admin permissions. Wildcard projects and `instance:admin` are rejected. An unbound subject cannot consent or obtain a grant. An empty bindings array allows no people to sign in.

Upstream discovery must succeed at startup. Discovery, token, authorization and JWKS endpoints require HTTPS except on loopback. Back-channel HTTP requests have a ten-second timeout and do not follow redirects. Deploy the provider at its final discovery URL.

## Register the downstream client

An instance administrator must pre-register each MCP client's exact callback URL and appropriate authentication method through the OAuth client management API. Authorize only `events:read` and `management:read` for the interactive MCP flow. The client registration is an application capability limit; it does not give a person access to any project.

Copy the callback shown by the actual client's connection setup. ChatGPT can use a stable callback `https://chatgpt.com/connector_platform_oauth_redirect` when the authorization server supports issuer identification; other connection modes use a callback-specific URL. The local protocol fixtures also exercise `https://claude.ai/api/mcp/auth_callback`. These are examples, not ambient allowlists or a substitute for the callback displayed by the current client setup. Register each exact URL and avoid wildcard callbacks. See the [OpenAI authentication guide](https://developers.openai.com/plugins/build/auth) and [Claude connector setup guide](https://support.claude.com/en/articles/11176164-use-connectors-to-extend-claude-s-capabilities).

For a public client, save a registration body to a private file and replace every placeholder with the operator-approved values:

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

A confidential client can use `client_secret_basic` or `client_secret_post` if the client supports it. The returned secret is displayed once; save it privately and configure the client. Public clients use `none` and receive no client secret. Do not enable `client_credentials` for a public interactive registration.

With an instance-admin API access token in a mode-600 curl config file containing an `Authorization: Bearer ...` header, create the registration:

```sh
umask 077
curl --fail-with-body --config "$PORCUPINE_ADMIN_CURL_CONFIG" \
  "$PORCUPINE_OAUTH_ISSUER/api/v1/oauth-clients" \
  -H 'Content-Type: application/json' \
  --data-binary @oauth-client-registration.json > oauth-client-created.json
```

Use the returned `client_id` in the client setup. In Claude custom connector setup, select “Use your own OAuth client” under OAuth client. The published-identity and automatic-registration options do not create an operator-approved Porcupine registration. For any client, use its supported preconfigured-client mode; if that mode is unavailable, do not enable public registration as a workaround. Keep any returned client secret out of browser JavaScript, URLs and analytics payloads. The [machine OAuth guide](oauth.md) describes the bootstrap exchange for obtaining an administrator API token. An MCP resource token cannot be used as this API administrator token.

The client connects to `https://analytics.example.com/mcp`, discovers OAuth metadata, and sends an authorization-code request with its registered client ID, exact callback, MCP resource and S256 PKCE challenge. The upstream login completes before Porcupine displays the client name, client ID, callback address and permissions. The person explicitly selects which of their assigned projects to share. An approval cannot add projects or permissions. Denial returns `access_denied` to the validated callback.

## Sessions and revocation

Login and consent state lasts at most ten minutes and is held only in bounded process memory. Cookies are HttpOnly and SameSite=Lax, and use Secure plus a `__Host-` name on HTTPS. Restarting the process ends pending browser sessions. Callback state, nonce, cookie possession and a single-use consent CSRF token protect the flow. The consent POST also requires the configured issuer as its Origin. Pages cannot be framed, do not load third-party resources, and use `no-store` and a same-origin referrer policy. Cross-origin navigation sends no referrer.

Human principals are durable access-key records linked by `oidc_humans`; they have no issued public bearer credential. Existing `access_keys.revoked_at` revocation disables the human and dependent grants. Restarting never clears that revocation. Removing a subject binding or reducing its projects or permissions also limits subsequent consent and token checks. Do not delete the principal and recreate it as a way to revoke access, since the operator binding would provision it again at startup. Use revocation and remove the binding.

To revoke a registered application, use its returned client ID:

```sh
curl --fail-with-body --config "$PORCUPINE_ADMIN_CURL_CONFIG" \
  -X DELETE "$PORCUPINE_OAUTH_ISSUER/api/v1/oauth-clients/$CLIENT_ID"
```

To revoke one human or one consent-grant principal, find its ID through `GET /api/v1/access-keys` and use the instance-admin `DELETE /api/v1/access-keys/{id}` operation. Human records have the name `OIDC human`; consent principals have `Interactive CLIENT_NAME`. When several humans exist, the operator can resolve the exact subject to principal ID through a private read-only SQLite query rather than guessing by name:

```sql
SELECT principal_id FROM oidc_humans
WHERE issuer = 'https://login.example.com/realms/analytics'
  AND subject = 'the-providers-stable-subject-id';
```

The API never returns an OIDC human bearer secret. Remove the matching binding as part of offboarding. Confirm that the old MCP access token and refresh token both fail, then confirm that a new login cannot grant the removed project. The integration tests perform token rejection after human revocation and reject attempts to select a project outside the binding.

Downstream client, consent-grant, refresh-token and human revocation are checked by the OAuth token implementation. Revocation of upstream OIDC login alone does not automatically notify Porcupine or invalidate an already issued downstream grant. Revoke the Porcupine human or grant when removing a person, and remove their operator binding. No upstream refresh or access token is used as an MCP credential.

## Local verification

The Go integration tests use a local discovery endpoint, RSA-signed ID tokens, JWKS and actual upstream code exchange with PKCE. They exercise project selection, forged state/cookie/CSRF, invalid issuer/audience/nonce, unbound subjects, denial, expiry and revocation.

```sh
go test ./internal/porcupine -run TestInteractiveOIDC -count=1
cd compat
npm ci
npx playwright install chromium
cd ..
PORCUPINE_OIDC_BROWSER=1 go test ./internal/porcupine -run TestInteractiveOIDCRealBrowser -count=1
```

The Chromium test completes login and project selection through a real browser against the local provider. These tests establish protocol behavior without connecting to a live ChatGPT, Claude or production identity provider account. Verify the operator's exact issuer, subject values, deployment cookie behavior and client callback configuration in staging before rollout.
