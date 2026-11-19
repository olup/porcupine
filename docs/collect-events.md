# Choose an event input

Porcupine accepts native HTTP events, supported PostHog SDK requests, and Segment HTTP requests. All three inputs write to the same project event table, so the query workflow stays the same.

## Choose the producer

| Your application | Start with |
| --- | --- |
| Already uses PostHog | [PostHog SDK configuration and tested profiles](reference/compatibility.md) |
| Already uses Segment | [Segment tracking routes and SDK example](reference/native-and-segment-ingestion.md#point-a-segment-producer-at-porcupine) |
| Sends JSON from a backend or custom client | [Native event format](reference/native-and-segment-ingestion.md#send-native-events) |
| Has historical events | [Native, PostHog, or Segment imports](imports.md) |

PostHog compatibility covers ingestion. Queries and management use Porcupine's own API. The [compatibility guide](reference/compatibility.md) lists the tested SDK versions and supported behavior.

## Choose project credentials

Create or select a [project](reference/projects.md) before configuring the producer.

- For browser capture, use a `mixed` project's public ingestion key and configure the application's exact origin in `PORCUPINE_ALLOWED_ORIGINS`. Public ingestion keys allow event submission, never analytics reads.
- For native backend capture, use an API-audience [service key](reference/service-keys.md) or OAuth token with `events:write` for the project. A `server_only` project requires private credentials.

Keep private credentials on the backend. Use the adapter-specific authentication described in the PostHog or Segment guide when configuring those SDKs.

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
