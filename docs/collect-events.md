# Choose an event input

Porcupine writes events from the SDKs and native HTTP to the same project event table.

## Choose the producer

| Your application | Start with |
| --- | --- |
| Already uses PostHog in the browser | [PostHog JS](#posthog-js) |
| Already uses Segment on the backend | [Segment](#segment) |
| Needs cookieless browser capture | [Porcupine JS](#porcupine-js) |
| Sends JSON from a backend or custom client | [Native event format](reference/native-and-segment-ingestion.md#send-native-events) |
| Has historical events | [Native, PostHog, or Segment imports](imports.md) |

PostHog compatibility covers ingestion. Queries and management use Porcupine's own API. The [compatibility guide](reference/compatibility.md) lists the tested SDK versions and supported behavior.

## Choose project credentials

Create or select a [project](reference/projects.md) before configuring the producer.

- For browser capture, use a `mixed` project's [public ingestion key](reference/projects.md#get-a-browser-ingestion-key). Browser ingestion accepts any origin by default. Public ingestion keys permit event submission only. [Restrict browser origins](reference/configuration.md#browser-ingestion)
- For native backend capture, use an API-audience [service key](reference/service-keys.md) or OAuth token with `events:write` for the project. A `server_only` project requires private credentials.

Keep private credentials on the backend. Use the adapter-specific authentication described in the PostHog or Segment guide when configuring those SDKs.

## PostHog JS

Install `posthog-js@1.447.3` and set `api_host` to your Porcupine URL. Use a mixed project's public ingestion key and the tested cookieless profile. It disables autocapture and browser storage. Porcupine derives a daily pseudonym for anonymous visitors. [Browser setup and capture example](reference/compatibility.md#configure-browser-capture)

## Segment

On your backend, install `@segment/analytics-node@3.1.0`. Set `PORCUPINE_URL` to your Porcupine origin and `PORCUPINE_WRITE_KEY` to the mixed project's write key in your backend environment. The SDK can send `page`, `track`, and `identify` calls. [Segment setup and capture example](reference/native-and-segment-ingestion.md#point-a-segment-producer-at-porcupine)

For a `server_only` project, use [native HTTP with private credentials](#send-a-native-backend-event).

## Porcupine JS

The Porcupine browser SDK captures events without cookies or browser storage. It queues a bare pageview on initialization unless you set `capture_pageview: false`.

The SDK is available as a package tarball or a standalone script starting with v0.21.0. [Install from a release](../sdk/browser/README.md#install-from-a-release) or [build from source](../sdk/browser/README.md#build-from-source).

For Porcupine Cloud, use `initCloud` with a mixed project's public ingestion key. The SDK sends events to `https://api.getporcupine.eu`.

```js
import { initCloud } from '@porcupine/browser'

const analytics = initCloud('pc_ci_YOUR_KEY')
analytics.capture('signup_completed', { plan: 'starter' })
```

For a self-hosted instance, pass the project's key and the instance URL to `init`.

```js
import { init } from '@porcupine/browser'

const analytics = init('pc_ci_YOUR_KEY', { api_host: 'https://analytics.example.com' })
analytics.capture('signup_completed', { plan: 'starter' })
```

[Browser SDK options and delivery behavior](../sdk/browser/README.md)

## Send a native backend event

Set `PORCUPINE_URL` to the instance origin. Load an API-audience service key with `events:write` into `PORCUPINE_API_KEY` through your private environment configuration. The writer must target one project.

```sh
curl --fail-with-body "$PORCUPINE_URL/ingest/v1/events" \
	-H "Authorization: Bearer $PORCUPINE_API_KEY" \
	-H 'Content-Type: application/json' \
	--data-binary '{"type":"track","name":"article_published","message_id":"article-42-published","actor_id":"user-7","properties":{"article_id":"42","category":"engineering"}}'
```

Reuse `message_id` when retrying the original event. A standalone retry in the same project and logical source returns the same `event_id` with `duplicate: true`. Hosted receipts contain `message_id` and `uuid`. Use a new message ID for each new event.

Native requests can contain one object or a batch of up to 1,000 objects, within a 1 MiB request limit. See the [native reference](reference/native-and-segment-ingestion.md) for pageviews, identify messages, batches, and logical sources.

## Check delivery and identity

For native capture, standalone returns `durability: "durable"` after SQLite commits. Hosted returns `durability: "buffered"` after admitting the event to bounded worker memory. A worker failure before the DuckLake commit can lose a buffered event. [Acknowledgment contract](reference/native-event-contract.md#acknowledgment-metadata)

To check delivery on standalone, wait for `delivery.pending` to reach zero. On hosted, `delivery.pending_unknown` can be true across workers. [Query for the expected event](query-events.md) to confirm delivery.

Choose the identity behavior your product needs. The recommended PostHog browser profile uses a daily cookieless pseudonym. The opt-in identified profile can link anonymous activity at login. See [users and sessions](data-and-identity.md) before interpreting visitor counts or retention across days.
