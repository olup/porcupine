# Porcupine browser SDK

Use a public `pc_ci_` key from a Porcupine project with mixed ingestion enabled.
Set `api_host` to your Porcupine base URL. If the server restricts capture
origins, include your app's origin in its allowlist.

Choose a version from [GitHub Releases](https://github.com/olup/porcupine/releases)
and replace both instances of `VERSION` with it.

```sh
npm install https://github.com/olup/porcupine/releases/download/vVERSION/porcupine-browser-VERSION.tgz
```

```js
import { init } from '@porcupine/browser'

const analytics = init('pc_ci_YOUR_KEY', { api_host: 'https://analytics.example.com' })
analytics.capture('signup_completed', { plan: 'starter' })
```

For a classic script, serve the pinned `porcupine-browser-VERSION.js` release
asset from your own site.

```html
<script src="/assets/porcupine-browser-VERSION.js"></script>
<script>
  const analytics = Porcupine.init('pc_ci_YOUR_KEY', {
    api_host: 'https://analytics.example.com',
    capture_pageview: false,
  })
  analytics.capture('$pageview', { page: 'pricing' })
</script>
```

`init` queues one bare `$pageview` by default. Pass `capture_pageview: false`
to opt out. For a single-page app, call `capture('$pageview', properties)` on
the route changes you want to measure. `disable()` clears the queue, aborts
an in-flight request, and stops the client. Delay initialization until your
app allows capture.

The SDK holds events in memory and sends fetch requests without credentials or
a referrer. It uses no cookies, browser storage, or client-side fingerprinting,
and collects no URL, page title, campaign tags, or referrer automatically.
Porcupine estimates daily visitors using a pseudonym based on the project,
client IP, and HTTP User-Agent. Pass only the custom properties you intend to
send.

`capture` returns `true` when it queues the event locally. The SDK sends
batches of at most 20 events and 48 KiB, normally starting within a second.
When the page hides, it attempts to send a batch if the client is idle. The
queue holds up to 200 events and 256 KiB. It drops expired events after two
minutes and stops retrying a batch after three retries. A visible
`Retry-After` beyond 30 seconds ends retries for that batch. Quota and
permanent client errors also drop the affected batch.

A standalone server acknowledges after it commits to its journal. A hosted
server acknowledges admission to its in-memory buffer. Events can take
additional time to appear in SQL in either profile.
