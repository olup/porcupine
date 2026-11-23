# Native and Segment ingestion

The [native event contract](native-event-contract.md) defines message fields, IDs, server-managed values, and adapter mappings. It also covers schema discovery and the [historical message format](native-event-contract.md#historical-records).

Native, Segment and supported [PostHog inputs](compatibility.md) write to the same project event table. Reuse the same producer message ID when retrying an event.

## Send native events

`POST /ingest/v1/events` accepts one JSON object or an array of up to 1,000 objects. Use `Content-Type: application/json`. The request limit is 1 MiB, including after gzip decompression. For a mixed project, use its public write key in `X-Porcupine-Key`. Browser ingestion accepts any origin by default. [Restrict browser origins](configuration.md#browser-ingestion)

For backend ingestion, send an API-audience OAuth token or [service key](service-keys.md) with project-scoped `events:write` in the bearer header. A `server_only` project requires one of these private API credentials. [OAuth guide](oauth.md)

```sh
curl --fail-with-body "$PORCUPINE_URL/ingest/v1/events" \
  -H 'Content-Type: application/json' \
  -H "X-Porcupine-Key: $PORCUPINE_WRITE_KEY" \
  --data-binary @journey.json
```

Save the following as `journey.json`. A timestamp can be supplied in RFC 3339 format. When omitted, the server uses receipt time.

```json
[
  {
    "type": "pageview",
    "message_id": "msg-001",
    "actor_id": "browser-7",
    "context": {
      "page": {"url": "https://example.com/pricing", "referrer": "https://example.com/"},
      "locale": "fr-FR"
    },
    "properties": {"experiment_variant": "short-copy"}
  },
  {
    "type": "identify",
    "message_id": "msg-002",
    "user_id": "user-42",
    "anonymous_id": "browser-7",
    "set": {"plan": "pro"},
    "set_once": {"signup_channel": "organic"}
  },
  {
    "type": "track",
    "message_id": "msg-003",
    "actor_id": "user-42",
    "name": "order_completed",
    "properties": {"order_id": "ord-813", "amount_minor": 2900, "currency": "EUR"}
  }
]
```

The pageview and purchase become two queryable rows. The identify operation updates the profile and links the earlier visit. Subsequent trait updates can omit `anonymous_id`. A conflicting link is ignored. In standalone mode it contributes to the response's `identity_ignored` count. See the [identity rules](../data-and-identity.md). Native ordinary messages may omit `actor_id`. An inventory update can therefore contribute to event totals without creating a user. Use `count(DISTINCT person_id) FILTER (WHERE person_id != '')` when counting known or pseudonymous actors.

To correct clock skew on live native requests, include `?sent_at=` with the
producer's send time in RFC 3339 or Unix epoch milliseconds. URL-encode an
RFC 3339 value that includes a `+` timezone offset. Porcupine applies the same
correction as [PostHog live capture](compatibility.md#event-timestamps) and records
the original client and send times in `context.client_timestamp` and
`context.sent_at`. Without a valid `sent_at`, Porcupine uses the supplied
timestamp, or the receipt time when the timestamp is absent.

### Receipts and logical sources

Native receipts follow input order. In standalone, `durability: "durable"` confirms the SQLite commit. Each receipt includes `message_id`, the decimal-string `event_id`, the `uuid` used in event management routes, and `duplicate`. In hosted, `durability: "buffered"` confirms admission to memory. Each receipt includes `message_id` and `uuid` only. See the [acknowledgment contract](native-event-contract.md#acknowledgment-metadata).

All three live adapters return HTTP 200 on success at the profile's acknowledgment boundary. PostHog's response omits native receipts. A stable message ID lets you retry a whole batch after a lost response. An explicit native message ID can contain up to 1,024 bytes. Omitting it requests a new ID on each submission.

For a named source, set `source_id` on the single-project writer when [provisioning a service credential](service-keys.md). The writer needs `events:write`. The credential associates messages with one project and one logical producer. Give a replacement credential the same source to preserve retries across rotation. A native and a Segment credential can share the same source when both deliver events from that producer. The first accepted combination of project, source and message ID wins, including if a retry changes the payload.

Unbound native and Segment credentials use `native` and `segment` as their respective sources. Unbound PostHog clients keep their existing UUIDs and use the reserved source `posthog`. An explicitly source-bound PostHog writer uses the same source-scoped ID scheme as the other adapters. Switching an existing writer from an unbound to a bound source changes its deduplication namespace, so finish outstanding retries first. [Credential API](management-api.md)

### Send an event when a page closes

To track page exit from a native browser integration, send a small `track` event named `pageleave` on `pagehide`. Use `fetch` with `keepalive: true` and the usual `X-Porcupine-Key` header. A browser's `sendBeacon` API cannot set this header. Preserve the same `message_id` if you retry. Segment producers can send the same event name through their usual track call.

### Base64 form encoding

All three live inputs accept the same form transport. Set `compression=base64` in the URL and send the usual JSON as the standard Base64 value of a single form field named `data`. Use `Content-Type: application/x-www-form-urlencoded`. Credentials and message schemas follow the selected endpoint. The encoded request and decoded JSON must satisfy their normal size limits.

## Point a Segment producer at Porcupine

Set the SDK's tracking host to the Porcupine origin. A Segment Node SDK configured with the project write key can use its standard HTTP batch transport. The supported routes are `POST /v1/page`, `/v1/track`, `/v1/identify`, and `/v1/batch`. Use HTTP Basic auth with the project write key as username and an empty password, or supply `writeKey` in the JSON envelope. Individual messages are limited to 32 KiB and batches to 500 KiB after decoding. A batch may mix the three supported call types. Unsupported call types reject the batch before anything is accepted. [Segment HTTP Tracking API](https://www.twilio.com/docs/segment/connections/sources/catalog/libraries/server/http-api)

The compatibility suite runs the official `@segment/analytics-node` 3.1.0 client. For a backend using a mixed project's write key:

```js
import { Analytics } from '@segment/analytics-node'

const analytics = new Analytics({
  host: process.env.PORCUPINE_URL,
  writeKey: process.env.PORCUPINE_WRITE_KEY
})
analytics.track({
  messageId: 'order-813-completed',
  userId: 'user-42',
  event: 'order_completed',
  properties: { order_id: 'ord-813', amount_minor: 2900, currency: 'EUR' }
})
await analytics.flush({ timeout: 10000 })
```

The adapter accepts Segment destination routing options without storing credentials from those options or dispatching to Segment destinations. Configure outbound delivery through [Porcupine pipelines](../pipelines.md).

Porcupine corrects live Segment event time using its own receipt time minus the elapsed time between capture and send. It uses a valid `originalTimestamp` as capture time when supplied and otherwise uses `timestamp`. A valid event `sentAt` takes precedence over the batch `sentAt`. With no valid send time, Porcupine uses the supplied `timestamp` or defaults to receipt time. The source clock values used for correction are available in `context.client_timestamp` and `context.sent_at`. [Segment timestamp fields](https://www.twilio.com/docs/segment/connections/spec/common#timestamps)

A forwarded Segment event with a valid `receivedAt` and `timestamp` keeps its already corrected `timestamp`. Porcupine records its own `received_at` for the request. Historical Segment imports preserve the supplied `timestamp`.

| Segment input | Porcupine result |
| --- | --- |
| `page` | A `pageview`. Page URL, referrer and supplied page metadata are available in `context.page`. |
| `track` | A `track` with the producer's event name and business properties. `userId` takes precedence over `anonymousId` as the actor. |
| `identify` | Both IDs link an anonymous visitor to the user. A user ID alone updates that user's traits. An anonymous ID alone updates the anonymous profile without promoting it to an identified account. Segment traits use `set`. |

For example, these three members of a Segment batch describe the same two analytics rows as the native journey above. A Segment Identify call has no set-once operation.

```json
{"batch":[
  {"type":"page","messageId":"msg-001","anonymousId":"browser-7","context":{"locale":"fr-FR"},"properties":{"url":"https://example.com/pricing","referrer":"https://example.com/","experiment_variant":"short-copy"}},
  {"type":"identify","messageId":"msg-002","userId":"user-42","anonymousId":"browser-7","traits":{"plan":"pro"}},
  {"type":"track","messageId":"msg-003","userId":"user-42","event":"order_completed","properties":{"order_id":"ord-813","amount_minor":2900,"currency":"EUR"}}
]}
```

## Query any input

```sql
SELECT id, source_id, message_id, event_type, event, distinct_id, person_id,
       json_extract_string(context, '$.page.url') AS page_url
FROM events
WHERE person_id = $user
ORDER BY id
```

Bind `user` to `user-42`. The pageview keeps its original actor `browser-7` and resolves to the same person as the purchase. The `event` column keeps `$pageview` for compatibility. `event_type` gives agents the normalized category. `context` is a JSON object encoded as a string and holds fields supplied at capture time. Browser, session and page data can be absent on backend events. `person_properties` represents the current projected profile. For read-after-write checks, wait for asynchronous delivery. [Analytics API](analytics.md)
