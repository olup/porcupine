# Native and Segment ingestion

These endpoints are available in source builds containing this change. The published v0.2.0 image supports the [PostHog capture profile](compatibility.md).

## Send native events

`POST /ingest/v1/events` accepts one JSON object or an array of up to 1,000 objects. Use `Content-Type: application/json`. The request limit is 1 MiB, including after gzip decompression. For a mixed project, use its public write key in `X-Porcupine-Key`. Configure the exact allowed browser origin before using browser capture. For backend ingestion, use a project-scoped API OAuth bearer token with `events:write`. A `server_only` project requires OAuth. [OAuth guide](oauth.md)

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

The pageview and purchase become two queryable rows. The identify operation updates the profile and links the earlier visit. Subsequent trait updates can omit `anonymous_id`. A conflicting link is ignored and contributes to the response's `identity_ignored` count. See the [identity rules](data-and-identity.md). Native ordinary messages may omit `actor_id`. An inventory update can therefore contribute to event totals without creating a user. Use `uniqExactIf(person_id, person_id != '')` when counting known or pseudonymous actors.

### Receipts and logical sources

Each native receipt includes the producer's `message_id`, the server's `event_id` as a decimal string, the `uuid` for the event management routes, and a `duplicate` flag. Receipts follow input order. The server returns success after the SQLite transaction commits. A stable message ID lets you retry a whole batch after a lost response. Omitting the message ID requests a new ID on each submission. An explicit native message ID can contain up to 1,024 bytes.

Configure `source_id` when creating an ingestion credential. For example, issue a key with `{"name":"orders writer","permissions":["events:write"],"projects":["PROJECT_ID"],"source_id":"orders-service"}`. The credential associates messages with one project and one logical producer. Give a replacement credential the same source to preserve retries across rotation. A native and a Segment credential can share the same source when both deliver events from that producer. The first accepted combination of project, source and message ID wins, including if a retry changes the payload.

Unbound native and Segment credentials use `native` and `segment` as their respective sources. Unbound PostHog clients keep their existing UUIDs and use the reserved source `posthog`. An explicitly source-bound PostHog writer uses the same source-scoped ID scheme as the other adapters. Switching an existing writer from an unbound to a bound source changes its deduplication namespace, so finish outstanding retries first. [Credential API](management-api.md)

## Point a Segment producer at Porcupine

Set the SDK's tracking host to the Porcupine origin. A Segment Node SDK configured with the project write key can use its standard HTTP batch transport. The supported routes are `POST /v1/page`, `/v1/track`, `/v1/identify`, and `/v1/batch`. Use HTTP Basic auth with the project write key as username and an empty password, or supply `writeKey` in the JSON envelope. The source requires JSON. Individual messages are limited to 32 KiB and batches to 500 KiB. A batch may mix the three supported call types. Unsupported call types reject the batch before anything is accepted. [Segment HTTP Tracking API](https://www.twilio.com/docs/segment/connections/sources/catalog/libraries/server/http-api)

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

The adapter accepts Segment destination routing options without storing credentials from those options or dispatching to Segment destinations. Configure outbound delivery through [Porcupine pipelines](pipelines.md).

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
SELECT event_id, source_id, message_id, event_type, event, distinct_id, person_id,
       JSONExtractString(context, 'page', 'url') AS page_url
FROM events
WHERE person_id = {user:String}
ORDER BY id
```

Bind `user` to `user-42`. The pageview keeps its original actor `browser-7` and resolves to the same person as the purchase. `event_id` is an alias for the existing SQL `id`. The `event` column keeps `$pageview` for compatibility. `event_type` gives agents the normalized category. `context` is a JSON object encoded as a string and holds fields supplied at capture time. Browser, session and page data can be absent on backend events. `person_properties` represents the current projected profile. For read-after-write checks, wait for asynchronous delivery and request `"use_cache": false`. [Analytics API](analytics.md)
