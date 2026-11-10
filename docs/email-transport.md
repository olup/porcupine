# Send verification codes through SMTP

Configure a transactional SMTP relay to send invitation notices and verification codes. An invitation tells the recipient which Porcupine instance to connect to. The person requests a separate code each time they authorize a client. Porcupine uses the [go-smtp](https://github.com/emersion/go-smtp) client and the operating system's TLS trust store. An installation using manual invitations can run without any email configuration.

1. Verify a sender address with your provider. Create SMTP credentials for it. The same transport works with [Amazon SES](https://docs.aws.amazon.com/ses/latest/dg/send-email-smtp.html), [Postmark](https://postmarkapp.com/developer/user-guide/send-email-with-smtp), [SendGrid](https://www.twilio.com/docs/sendgrid/for-developers/sending-email/integrating-with-the-smtp-api), [Mailgun](https://documentation.mailgun.com/docs/mailgun/user-manual/sending-messages/send-smtp), or a self-hosted TLS relay.
2. Save the password in a file accessible to the Porcupine process. The official image runs as UID 65532. With file-backed Compose secrets on Linux, place the file in a private host directory and grant read access to the container UID.
3. Set the OAuth issuer and SMTP configuration in your environment or Compose environment file. For example:

   ```ini
   PORCUPINE_OAUTH_ISSUER=https://analytics.example.com
   PORCUPINE_SMTP_HOST=smtp.example.com
   PORCUPINE_SMTP_FROM=analytics@example.com
   PORCUPINE_SMTP_USERNAME=porcupine
   PORCUPINE_SMTP_PASSWORD_FILE=/etc/porcupine/secrets/smtp-password
   PORCUPINE_ADMIN_EMAIL=operator@example.com
   ```

4. Add the OAuth and email overlays to the deployment and restart Porcupine:

   ```sh
   docker compose -f compose.yaml -f compose.oauth.yaml -f compose.email.yaml up -d --build
   ```

The email overlay mounts the specified host file at `/run/secrets/porcupine_smtp_password`. When running the binary directly, set `PORCUPINE_SMTP_PASSWORD_FILE` to the path on the server. Credentials are loaded at startup. Use your provider's SMTP password and username; some providers issue separate credentials for their HTTP API.

| Setting | Purpose |
| --- | --- |
| `PORCUPINE_SMTP_HOST` | SMTP hostname or IP address. The relay certificate must cover this name. |
| `PORCUPINE_SMTP_FROM` | Sender mailbox; optionally include a display name. |
| `PORCUPINE_SMTP_TLS` | `starttls` (default) requires the relay to upgrade to TLS before login or message delivery. `tls` negotiates TLS immediately. |
| `PORCUPINE_SMTP_PORT` | Defaults to 587 for STARTTLS and 465 for implicit TLS. |
| `PORCUPINE_SMTP_USERNAME`, `PORCUPINE_SMTP_PASSWORD_FILE` | Configure both for PLAIN or LOGIN authentication. For a private TLS relay that authenticates connections by network policy, omit both in a custom deployment. |
| `PORCUPINE_ADMIN_EMAIL` | Optional operator-approved mailbox for the deployment administrator. Use an address controlled by the administrator. |
| `PORCUPINE_LOCAL_INVITATIONS` | Set to `true` to offer manual invitation codes alongside email login. |

To test, invite an address you control and start an OAuth login. Check the mailbox and finish consent for the intended project. If mail delivery fails, check the relay's delivery records, sender verification, allowed recipients, and TLS configuration. Each SMTP attempt has a ten-second deadline. Code delivery to a real provider depends on its account and deliverability settings.
