# Invite a person to Porcupine

With [email delivery configured](email-transport.md), an instance administrator can give a person access once. The person opens **Connect** in a supported OAuth client, enters their invited email address on Porcupine, and requests a fresh verification code. After verifying the address, they select a project and approve the client's access. They can repeat this process from another client without requesting a second invitation. The instance and client must have matching OAuth registration settings.

## Invite and manage people

Use an instance-admin credential for the appropriate resource. In MCP, use `porcupine_docs` to discover the operation and `porcupine_call` to invoke it. The HTTP API and CLI use the API audience. After `porcupine login --admin`, the standalone CLI accepts each operation name below as a command. Save the example grant in `member.json` and run `porcupine invite-human --body @member.json`.

| Operation | HTTP request | Result |
| --- | --- | --- |
| `invite-human` | `POST /api/v1/humans` | Authorize an email address and send an invitation notice. |
| `list-humans` | `GET /api/v1/humans?limit=20&offset=0` | List human IDs, email addresses, grants and revocation status. |
| `update-human` | `PUT /api/v1/humans/{id}` | Replace an active invited account's grants. A change invalidates its existing OAuth connections. |
| `revoke-human` | `DELETE /api/v1/humans/{id}` | Disable further sign-in and revoke the account’s OAuth grants. |

For example, authorize Léa to query project A and import historical events into project B:

```json
{
  "email": "lea@example.org",
  "grants": [
    {"project_id": "PROJECT_A", "permissions": ["events:read", "management:read"]},
    {"project_id": "PROJECT_B", "permissions": ["events:write", "imports:write"]}
  ]
}
```

Use the same `grants` field for updates, omitting `email`. When all assigned projects have identical permissions, the alternative body shape is `{"email":"lea@example.org","projects":["PROJECT_A","PROJECT_B"],"permissions":["events:read"]}`. Choose one shape per request. The `grants` field in the response gives the authoritative per-project rights. The `projects` and `permissions` fields summarize the union of project IDs and permissions, respectively.

To invite another instance administrator, call `invite-human` with:

```json
{"email":"colleague@example.org","projects":["*"],"permissions":["instance:admin"]}
```

Only an authenticated instance administrator can grant this role. Use this exact grant by itself. The recipient signs in with their own mailbox on the dedicated admin OAuth client and consents to administration of all projects. For an existing member, call `update-human` with the same grant and their ID. Replacing it with explicit project grants removes admin access and invalidates previous connections. The `deployment_admin` field identifies the original deployment seed; the effective administrator permission is `instance:admin`.

Project member invitations require explicit project IDs. Email domains are case-insensitive; Porcupine preserves the case of the mailbox before `@` and recognizes an exact ASCII mailbox address. Provide the invited address when signing in.

The initial invitation notice contains connection instructions. Sign-in codes are issued only when the account holder starts a login. The account exists durably before the notice is sent. If delivery fails, Porcupine returns `503`; retry the same invite after at least five seconds. If an attempt is already in flight, Porcupine returns `429`. A crashed attempt can be retried after one minute. Retrying an acknowledged delivery returns the same human ID without sending again. A crash between SMTP acceptance and recording success can lead to duplicate notices; a notice alone grants no access. Reinviting a disabled account or submitting different grants for an existing account returns `409`.

OAuth discovery recommends the read scopes for general-purpose clients. Request write scopes explicitly on a registered client that allows them. The browser shows the actual scopes and project choices; tokens issued for the API and MCP have separate audiences.

## Deployment administrator

Optionally set `PORCUPINE_ADMIN_EMAIL` to seed the original human operator. This provisions a separate email principal with `instance:admin` and `*` project access. Connect using an admin-capable OAuth client and explicitly approve the administration scope. The bootstrap machine credential remains the operator's recovery path when SMTP is unavailable.

Porcupine records this admin address once. Restarts leave the account’s ID, grants and revoked status intact. Removing the environment setting leaves the existing account in place. Setting a different admin email later, or attempting to promote an already invited address through the deployment setting, fails startup. Restore the pinned value or remove the setting. To disable the email admin, revoke the account using its human ID. Retain an independent admin credential for recovery.

Service accounts can continue to authenticate separately. Disabling a human revokes that human’s access across their clients. To disconnect just one client grant, use `list-oauth-grants` and `revoke-oauth-grant`.
