# Porcupine browser SDK

Use a public `pc_ci_` key from a Porcupine project with mixed ingestion enabled.
Set `api_host` to your Porcupine base URL. If the server restricts capture
origins, include your app's origin in its allowlist.

## Install from a release

Choose a tagged [GitHub release](https://github.com/olup/porcupine/releases)
with a `porcupine-browser-VERSION.tgz` asset. Replace both instances of
`VERSION` with that exact version.

```sh
npm install https://github.com/olup/porcupine/releases/download/vVERSION/porcupine-browser-VERSION.tgz
```

## Build from source

You can build the SDK from the commit that added it. Run these commands from
your app directory with Git and npm installed.

```sh
sdk_source="$(mktemp -d)"
git clone https://github.com/olup/porcupine.git "$sdk_source"
git -C "$sdk_source" checkout --detach 26bf2d858c2579117aae954e26eb8b86c62f68ad
npm ci --prefix "$sdk_source/sdk/browser"
npm --prefix "$sdk_source/sdk/browser" run build
npm pack "$sdk_source/sdk/browser/dist" --ignore-scripts
npm install ./porcupine-browser-0.20.0.tgz
```

Keep the tarball with your app so that future installs can resolve the local
dependency. For a classic script, the build writes `sdk/browser/dist/porcupine.js`
in the source checkout.

## Capture an event

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

`init` queues one bare `$pageview` in the browser by default. Pass `capture_pageview: false`
to opt out. For a single-page app, call `capture('$pageview', properties)` on
the route changes you want to measure. `disable()` stops the client and clears
the queue. It attempts to abort any in-flight request, which the server may
have already received. Delay initialization until your app allows capture.

The SDK holds events in memory and sends fetch requests without credentials or
a referrer. It uses no cookies, browser storage, or client-side fingerprinting,
and collects no URL, page title, campaign tags, or referrer automatically.
Porcupine estimates daily visitors using a pseudonym based on the project,
client IP, and HTTP User-Agent. Pass only the custom properties you intend to
send.

`capture` returns `true` when it queues the event locally. The SDK sends
batches of at most 20 events and 48 KiB, normally starting within a second.
When the page hides, it attempts to send a batch if the client is idle. The
queue holds up to 200 events and 256 KiB. An event expires after waiting two
minutes in the queue. The SDK stops retrying a batch after three retries.
For retryable responses, a visible `Retry-After` delays all batches. If that
wait exceeds 30 seconds, the SDK also drops the active batch. A quota or
suspension response drops the active batch and pauses the client for at least
one minute. Permanent client errors drop the active batch.

A standalone server acknowledges after it commits to its journal. A hosted
server acknowledges admission to its in-memory buffer. Events can take
additional time to appear in SQL in either profile.
