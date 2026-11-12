# PostHog SDK compatibility

The v0.9.0 test matrix pins `posthog-js` 1.447.3 and `posthog-node` 5.63.1. Porcupine accepts event capture from these SDKs. Its query, management and MCP interfaces use Porcupine’s own contracts. [Tested behaviors and known gaps](../compat/matrix.json)

## Configure browser capture

Install `posthog-js@1.447.3` in your application. Create a `mixed` project and use its public ingestion key in the browser. Add the application’s exact origin to `PORCUPINE_ALLOWED_ORIGINS` on the server. Copy the [profile definitions](../compat/profiles.mjs) into your application and choose the mode you need.

```js
import posthog from 'posthog-js'
import { cookielessProfile } from './profiles.mjs'

posthog.init(projectIngestionKey, {
  ...cookielessProfile,
  api_host: porcupineOrigin,
  opt_out_useragent_filter: false
})
posthog.capture('article_opened', { section: 'news' })
```

Set `porcupineOrigin` to your instance origin, for example `https://analytics.example.com`. The cookieless profile disables autocapture and browser storage. Porcupine derives a project-specific daily pseudonym when the SDK sends the cookieless sentinel. To link a returning browser at login, choose `identifiedBrowserProfile` and call `posthog.identify()`. For backend events, use the Node profile or the [native ingestion endpoint](native-and-segment-ingestion.md). [Identity and privacy](data-and-identity.md)

## Configure backend capture

For a mixed project, install `posthog-node@5.63.1` and copy the same [profile definitions](../compat/profiles.mjs) into your application. Set `PORCUPINE_URL` to the instance origin and `PORCUPINE_WRITE_KEY` to the project's public ingestion key.

```js
import { PostHog } from 'posthog-node'
import { nodeProfile } from './profiles.mjs'

const analytics = new PostHog(process.env.PORCUPINE_WRITE_KEY, {
  ...nodeProfile,
  host: process.env.PORCUPINE_URL
})
analytics.capture({ distinctId: 'user-7', event: 'article_published' })
await analytics.flush()
await analytics.shutdown()
```

For a backend using private service credentials or a `server_only` project, use the [native ingestion endpoint](native-and-segment-ingestion.md#send-native-events).

## Cookieless product default

The supported cookieless profile is exported as `cookielessProfile` in
`compat/profiles.mjs`. The default keeps anonymous capture free of browser storage. To link activity at login, use the opt-in identified profile with `person_profiles: "identified_only"`; it requires persistent IDs.

`browser.cookieless-no-storage` runs the real SDK in isolated Chromium contexts.
It verifies successful CORS responses and stored daily pseudonyms for repeated
events, reload, fresh same-UA context, different UA and a separate project. It
also checks empty cookies/localStorage/sessionStorage and removed IP/correlation
fields. Deterministic Go tests cover next-day rotation, restart stability,
midnight transaction ordering, clock rollback, trusted proxies and atomic failure.
See the [identity design](data-and-identity.md#cookieless-by-default).

## Test coverage and limitations

The matrix covers Node retry deduplication, a browser batch with gzip, persistent-ID and reset behavior, cookie-free browser capture, and identified capture. Browser retry behavior, independent new-tab sessions, and beacon and XHR transports remain outside the tested contract. Autocapture, feature flags, replay, surveys, group updates and general aliasing are outside the supported profile. Review the [compatibility matrix](../compat/matrix.json) before changing your SDK configuration.

For fixture provenance, test commands, and SDK upgrade checks, see [SDK compatibility tests](sdk-compatibility-tests.md).
