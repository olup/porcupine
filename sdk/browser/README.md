# Porcupine browser SDK

Use a public `pc_ci_` key from a Porcupine project with mixed ingestion enabled.
If the server restricts capture origins, include your app's origin in its
allowlist.

## Install from a release

The SDK is available starting with [v0.21.0](https://github.com/olup/porcupine/releases/tag/v0.21.0).
Install the release tarball from your app directory.

```sh
npm install https://github.com/olup/porcupine/releases/download/v0.21.0/porcupine-browser-0.21.0.tgz
```

To install a later release, update the version in the tag and filename in the URL.

## Build from source

To try the SDK from the current main branch, run these commands from your app
directory with Git and npm installed.

```sh
sdk_source="$(mktemp -d)"
git clone --depth=1 --branch main https://github.com/olup/porcupine.git "$sdk_source"
sdk_version="$(cat "$sdk_source/VERSION")"
npm ci --prefix "$sdk_source/sdk/browser"
npm --prefix "$sdk_source/sdk/browser" run build
npm pack "$sdk_source/sdk/browser/dist" --ignore-scripts
npm install "./porcupine-browser-${sdk_version}.tgz"
```

Keep the tarball with your app so that future installs can resolve the local
dependency. For a classic script, the build writes `sdk/browser/dist/porcupine.js`
in the source checkout.

## Capture an event

On Porcupine Cloud, use `initCloud` with your Cloud project's key. It sends
events to `https://api.getporcupine.eu`.

```js
import { initCloud } from '@porcupine/browser'

const analytics = initCloud('pc_ci_YOUR_KEY')
analytics.capture('signup_completed', { plan: 'starter' })
```

For a self-hosted instance, use `init` with its key and base URL.

```js
import { init } from '@porcupine/browser'

const analytics = init('pc_ci_YOUR_KEY', { api_host: 'https://analytics.example.com' })
analytics.capture('signup_completed', { plan: 'starter' })
```

For a classic script, download `porcupine-browser-0.21.0.js` from the release
and serve it from your site. To verify the script in the browser, set the
script's `integrity` attribute to the value in `porcupine-browser-0.21.0.js.sri`.

```html
<script src="/assets/porcupine-browser-0.21.0.js"></script>
<script>
  const analytics = Porcupine.initCloud('pc_ci_YOUR_KEY', { capture_pageview: false })
  analytics.capture('$pageview', { page: 'pricing' })
</script>
```

Both initializers queue one bare `$pageview` in the browser by default. Pass `capture_pageview: false`
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
Analytics and exceptions have separate queues that together hold up to 200 events
and 256 KiB. When the page hides, the SDK tries to send from either queue if it is
idle. An event expires after waiting two minutes. The SDK stops retrying a batch
after three retries. A visible `Retry-After` delays the affected queue and drops
the active batch if the wait exceeds 30 seconds. A quota or suspension response
drops the affected batch and pauses its queue for at least one minute. Permanent
client errors drop the active batch.

A standalone server acknowledges after it commits to its journal. A hosted
server acknowledges admission to its in-memory buffer. Events can take
additional time to appear in SQL in either profile.

## Capture exceptions

In builds that include error tracking, set `capture_exceptions: true` to capture
uncaught errors and unhandled promise rejections. Use `captureException(error)`
to report an error from your own handler. Set `build_id` to match the ID used to
upload your source maps. See the [error tracking guide](../../docs/error-tracking.md).
