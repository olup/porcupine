# Recover an upstream sign-in identity

**Unreleased.** These operator operations accompany tenant access and optional Google/OIDC sign-in. Use a CLI built from the same version as the server.

An upstream identity binds a trusted provider's issuer and subject to an invited Porcupine human. The human can belong to several tenants. Changing that binding affects the person's tool connections across those tenants, so only an instance operator can perform recovery. Tenant administrators and project readers cannot list or unlink global identities.

## Find and unlink a binding

Keep an independent operator credential available, such as the deployment bootstrap credential. An interactive operator session belonging to the affected human is revoked by this operation too. The recovery API does not protect you from losing your only usable interactive login.

Authenticate the CLI as an instance operator, then find the human and its identity bindings:

```sh
porcupine login --admin
porcupine list-humans
porcupine list-human-identities --id HUMAN_ID --limit 100
```

The result contains `identities` with `id`, `issuer`, `subject`, `linked_email` and `created_at`. It contains no provider token, client secret or browser callback material. If `next_cursor` is present, pass it with `--cursor` on the next call.

Unlink the selected binding by its ID:

```sh
porcupine unlink-human-identity --id HUMAN_ID --identity-id BINDING_ID
```

The corresponding HTTP routes are:

```text
GET /api/v1/humans/{id}/identities?limit=100&cursor=BINDING_ID
DELETE /api/v1/humans/{id}/identities/{identity_id}
```

An authorized agent can discover the same operation IDs through `porcupine_docs` and call them through `porcupine_call`. HTTP, CLI and MCP enforce the same operator permission.

## What changes

One transaction removes that binding, revokes all interactive grants issued to the human and invalidates browser sign-in proofs and email codes issued before recovery. Existing access tokens, refresh tokens and unredeemed authorization codes stop working. A pending consent page cannot issue another grant from its old proof, including when another worker handles it.

The human account, tenant memberships and project permissions remain. Team service accounts and their keys are independent and keep working. Other humans' sessions and tool connections remain valid.

A successful delete returns HTTP 204. Retrying an absent binding ID also returns 204 without further revocation. An unknown human returns 404. A binding belonging to another human is treated as absent and is never removed. Every fresh binding gets a new ID, so retrying an old delete cannot remove a later relink of the same provider subject.

## Connect again

Ask the person to start a fresh CLI or agent login. A new email code can authenticate the account if SMTP is configured. A fresh trusted provider callback with the invited, verified email can link the identity again with a new binding ID. Porcupine does not accept operator-supplied provider subjects to relink accounts.

Unlinking is a recovery action, not a ban on that provider or person. An upstream sign-in that has not yet authenticated locally can complete afterward and create a fresh binding. To prevent future access, revoke the human account instead. To remove access to one team only, revoke its tenant membership.
