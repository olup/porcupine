# Native event contract

The native producer format is independent of PostHog and Segment. Their live adapters translate messages into the same acceptance path, which assigns IDs, applies identity rules, and commits the durable queue.

This reference describes version 1 of the native message format. The native capture endpoint already exists in v0.13.0. Schema discovery is new in development builds. Development also adds native and Segment historical imports alongside PostHog. These additions are not in the published v0.13.0 image.

## Discovery

| Interface | Contract |
| --- | --- |
| Public HTTP | `GET /ingest/v1/schema` returns the live message JSON Schema without credentials. It contains no project data. |
| Project HTTP | `GET /api/v1/projects/{id}/input-contract` returns the contract version, live schema, historical message schema, and server-managed fields. Requires `events:read` on that project. |
| CLI | `porcupine get-event-input-contract --id PROJECT_ID` uses the same project operation. |
| MCP | `porcupine_docs` with `operation_id: get-event-input-contract` discovers the contract. `porcupine_call` executes it with `path: {"id":"PROJECT_ID"}`. |

The running instance's contract takes precedence over website examples. This input schema describes what a producer sends. `get-event-schema` separately describes the SQL table exposed to readers.

The [live JSON Schema](https://olup.github.io/porcupine/schemas/native-message-v1.schema.json) and [historical message schema](https://olup.github.io/porcupine/schemas/native-history-message-v1.schema.json) are generated from the files embedded in the server. Their `$id` values identify the contracts. The `/ingest/v1/events` path selects version 1; messages have no `schema_version` field.

## Versioning

Version 1 gives each field one meaning across native capture and adapter normalization. Incompatible meanings require a new major contract and input path. Producers discover a running instance before relying on new optional fields, since older servers reject unknown top-level fields.

The historical message schema requires stable producer IDs and original timestamps. Separate native person snapshot records are not defined.

## Live messages

`POST /ingest/v1/events` accepts one JSON object or an array of 1–1,000 messages. Authentication selects one project and a logical source. A project write key uses `X-Porcupine-Key`. Authenticated API writers are also subject to the [ingestion rules](native-and-segment-ingestion.md).

The schema describes the canonical encoding. Absent optional fields are omitted. Existing decoding also tolerates some empty or null optional fields; producers do not need to rely on that behavior. Unknown top-level fields are rejected. Arbitrary business fields belong in `properties` or `context`.

| Field | Meaning |
| --- | --- |
| `type` | Required. `track`, `pageview`, or `identify`. |
| `message_id` | Producer's stable ID, 1–1,024 UTF-8 bytes, with at least one non-whitespace character. If omitted in live capture, Porcupine creates a new ID on each submission. |
| `timestamp` | Occurrence time in RFC 3339, including a timezone. Porcupine normalizes it to UTC. If omitted in live capture, it uses receipt time. |
| `actor_id` | Original actor for `track` or `pageview`, at most 1,000 UTF-8 bytes. Optional. An actorless event does not create a user. |
| `name` | Required for `track`, at most 500 UTF-8 bytes. Must contain a non-whitespace character and must not start with `$`. Absent for other types. |
| `properties` | Business values for `track` or `pageview`. A JSON object with nested JSON values, including null. |
| `context` | Capture-time metadata as a JSON object, at most 64 KiB when encoded. |
| `user_id` | Required for `identify`. A non-blank application user ID of at most 1,000 UTF-8 bytes. |
| `anonymous_id` | Optional for `identify`. Supplies explicit evidence to link an anonymous actor to `user_id`. |
| `set` | Optional trait assignments for `identify`. Replaces existing values for the supplied keys. |
| `set_once` | Optional initial traits for `identify`. Existing values are preserved. |

Each trait object is limited to 128 keys and 32 KiB of encoded JSON. Identity policy can ignore a conflicting or invalid identity operation; inspect `identity_ignored` in the receipt. Schema validation alone does not grant permission or establish an identity link.

JSON Schema character limits do not measure UTF-8 bytes. The `x-porcupine-max-utf8-bytes` and `x-porcupine-max-encoded-bytes` annotations document the server's additional budgets. Timestamp parsing, reserved identity IDs, request size, queue capacity, and configured privacy rules are enforced at runtime. Generic JSON Schema validators may treat `format: date-time` as an annotation.

### Pageview

```json
{
  "type": "pageview",
  "message_id": "visit-001",
  "timestamp": "2026-01-10T09:00:00+01:00",
  "actor_id": "browser-7",
  "context": {
    "page": {"url": "https://example.com/pricing"},
    "locale": "fr-FR"
  }
}
```

A pageview has no top-level `name`. An optional page label belongs in `context.page.name`. The SQL event name is `$pageview`.

### Identify

```json
{
  "type": "identify",
  "message_id": "login-001",
  "timestamp": "2026-01-10T09:01:00+01:00",
  "user_id": "user-42",
  "anonymous_id": "browser-7",
  "set": {"plan": "pro"}
}
```

This message requests a link between `browser-7` and `user-42`, and updates current traits. It does not rewrite the original actor in earlier events. A target-only identify can omit `anonymous_id`. Arbitrary identified-person merges are unsupported. See [identity rules](data-and-identity.md).

An identify is a control message, not an ordinary analytics occurrence. It is excluded from ordinary event counts. The native endpoint does not accept anonymous-only trait updates; the Segment adapter separately supports Segment's anonymous identify semantics.

### Backend event

```json
{
  "type": "track",
  "message_id": "order-813-completed",
  "timestamp": "2026-01-10T09:02:00+01:00",
  "actor_id": "user-42",
  "name": "order_completed",
  "properties": {
    "order_id": "ord-813",
    "amount_minor": 2900,
    "currency": "EUR"
  },
  "context": {"library": {"name": "checkout-backend", "version": "1.0"}}
}
```

An event such as `stock_updated` can omit `actor_id`. Porcupine does not invent a person from a message ID. Opaque external IDs are best represented as strings, since a producer's JSON library may round large numbers before sending them. Server decoding preserves numeric JSON tokens.

### Context conventions

`properties` describes the event's business data. `context` describes the circumstances of capture. Recommended keys include `page.url`, `page.referrer`, `page.title`, `page.path`, `page.name`, `locale`, `browser.name`, `browser.version`, `device`, `os`, `session_id`, and `library`.

These are conventions, not required fields or an automatic enrichment service. Native producers supply their own browser and session values. Extensions are preserved unless the configured privacy policy removes them. Cookieless capture removes known identity and address fields; arbitrary properties are not a complete personal-data scrub.

## IDs and retry semantics

The producer's `message_id` and Porcupine's `event_id` have different roles. The first identifies a source message. The second identifies the accepted event inside Porcupine.

For native and Segment messages, replay identity is the tuple of authenticated project, logical source, and producer message ID. A credential can bind a logical `source_id`. Replacement credentials with the same binding preserve retries across rotation. Unbound native and Segment credentials use `native` and `segment` respectively, so they are separate namespaces.

A source-bound PostHog writer joins the same scheme using its event UUID as the producer message ID. Unbound PostHog ingestion retains the legacy UUID namespace and reserved source `posthog`. It does not automatically deduplicate with unbound native or Segment messages.

The first accepted message wins. A retry with changed properties does not edit the event. Corrections use [event management operations](events.md). Native receipts return `message_id`, a decimal-string `event_id`, an opaque `uuid` used by management routes, and `duplicate`. Clients preserve these values exactly and do not decode the UUID's internal format.

A missing message ID means a new event on every submission, even for identical payloads. A converter preparing a historical file must assign missing IDs once and persist them in that file. Hashing only the payload can collapse legitimate repeated events.

Success confirms the SQLite acceptance transaction. It does not mean analytical delivery has completed. Readers check delivery progress before validating a complete batch.

## Server-managed representation

Producers cannot choose `project_id`, `source_id`, `event_id`, `uuid`, `received_at`, `revision`, or the historical import marker. Authentication and server processing supply them. A top-level attempt to send these fields through native capture is rejected.

The internal model distinguishes original occurrence data from current identity projections. Existing storage and SQL column names remain:

| Meaning | Stored or SQL representation |
| --- | --- |
| Producer name | `event`; pageviews use `$pageview` |
| Original actor | `distinct_id` |
| Native message kind | `event_type` |
| Producer ID | `message_id` for normalized inputs |
| Server ID | Receipt `event_id`; SQL `id` |
| Management locator | `uuid` |
| Occurrence time | `timestamp`, with derived SQL time columns |
| Acceptance time | `received_at` |
| Current resolved person | `person_id` in the analytical projection |
| Current person traits | `person_properties` in the analytical projection |
| Correction state | Server-assigned revision and deletion state |

Current person traits are not a snapshot of what was true at the event's occurrence time. Changes to traits or links can change the identity projection of earlier activity without changing the original actor. Edits and deletion also leave the event's replay identity intact.

## Adapter mapping

| Meaning | Native | PostHog live input | Segment live input |
| --- | --- | --- | --- |
| Producer ID | `message_id` | `uuid` | `messageId` |
| Occurrence time | `timestamp` | `timestamp` | `timestamp` |
| Pageview | `type: pageview` | `event: $pageview` | `type: page` |
| Custom event | `type: track`, `name` | `event` | `type: track`, `event` |
| Original actor | `actor_id` | `distinct_id` at the supported envelope or property position | `userId`, otherwise `anonymousId` |
| Identify target | `user_id` | `$identify` with `distinct_id` | `identify` with `userId` |
| Explicit anonymous link | `anonymous_id` | `$anon_distinct_id` | `anonymousId` alongside `userId` |
| Trait assignments | `set`, `set_once` | `$set`, `$set_once` | `traits` maps to `set`; no set-once equivalent |

The adapters preserve business values, but their metadata layouts are not identical. PostHog browser, page, and session fields remain supported reserved properties. Native uses `context`. Segment keeps `context` and copies supported page properties into `context.page`. Porcupine does not yet flatten all three into a universal browser object. Agents inspect the stored schema before assuming where a property lives.

The equivalent live journey is available as [native JSON](../examples/events/native.json), [Segment JSON](../examples/events/segment.json), and [PostHog JSON](../examples/events/posthog.json). Credentials are absent from these files. The test suite sends each journey through its real adapter, verifies the original times and identity link, then retries through all three with the same configured source.

## Historical records

A native historical message uses the live shape with mandatory `message_id` and `timestamp`. Files are UTF-8 JSONL, one message per line. Historical `track` and `pageview` records become analytics events. An `identify` contributes explicit historical identity evidence; its `set` and `set_once` fields are never replayed as current traits.

Use `porcupine import --project PROJECT_ID --format native FILE_OR_DIRECTORY` for historical data. A backdated message sent to the live endpoint still follows live identity and pipeline behavior. It is not a substitute for historical import. [Import guide](imports.md)

Historical import distinguishes these record semantics:

| Record | Role |
| --- | --- |
| Ordinary event | An occurrence with its original timestamp, actor, and properties. |
| Identity evidence | An explicit historical relationship between IDs, subject to ownership and conflict checks. |
| Person snapshot | Traits and mappings from a point in source history, with version and provenance information. |

A person snapshot is not a live `identify` command. Replaying an old `set` could overwrite a newer plan, while a source's internal person UUID may not be its application user ID. Native and Segment identify messages carry explicit evidence under the historical ownership rules. A separate native person snapshot encoding remains undefined.

The existing [PostHog importer](posthog-import-format.md) already distinguishes event rows, explicit identity evidence, and person snapshots. It protects live-owned profiles, tracks unresolved identity, marks imported data, and suppresses historical delivery to automatic pipelines. Native and Segment imports share those guarantees through the common import engine.
