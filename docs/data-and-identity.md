# Events, identity, and sessions

Porcupine’s recommended browser profile sends a cookieless event that the server assigns a daily pseudonym. The opt-in identified profile supports anonymous-to-user linking through `posthog.identify()`.

## Event identity and immutability

Within a project, the unbound PostHog input uses the original event UUID. Native and Segment inputs use the logical source and producer message ID to derive the stored UUID. Reuse the same source and message ID on a retry to receive the same durable event ID. Without a stable producer ID, a retry can create another event. [Receipts and logical sources](native-and-segment-ingestion.md#receipts-and-logical-sources)

Each ordinary event retains its accepted `distinct_id`. SQL exposes `person_id` to group that event under the currently resolved identity, including events accepted before login. `person_properties` contains the current profile as a JSON object encoded in a string.

[Event corrections and deletions](events.md) are implemented with `events:edit`
and `events:delete`. A correction creates a higher revision and preserves the
private original accepted snapshot. A deletion blanks the current and original
payload in its tombstone. Durable event ID, project, UUID, original receipt
metadata, revision, and deletion state remain. These changes do not merge people.
SQL shows the latest projected revision; the current-event API also reflects
accepted pending corrections.

Tombstones prevent late original deliveries from resurrecting deleted events
and remain for the life of the project. Old parts, S3 object versions, free pages, WAL, and
backups have separate cleanup lifecycles. Deletion does not promise immediate
physical erasure from every copy.

## Link an anonymous browser at login

Use the identified browser profile from [the SDK compatibility examples](../compat/profiles.mjs). It enables browser persistence and uses `person_profiles: 'identified_only'`. The cookieless profile deliberately uses `person_profiles: 'never'`; with that setting, the SDK suppresses identification.

```js
posthog.init(projectKey, { ...identifiedBrowserProfile, api_host: porcupineOrigin })

posthog.capture('signup_started')
posthog.identify('user-42', { plan: 'pro' })
posthog.capture('signup_finished')

// On logout, reset the SDK before the next visitor starts using this browser.
posthog.reset()
```

Once delivery catches up, the two ordinary events keep their original IDs and share a person:

```sql
SELECT event, distinct_id, person_id, JSONExtractString(person_properties, 'plan') AS plan
FROM events
WHERE person_id = {user:String}
ORDER BY received_at_ms, id
```

Bind `user` to `user-42`. Both rows resolve to `user-42`, including `signup_started`, whose `distinct_id` is still the anonymous browser ID. Use `uniqExactIf(person_id, person_id != '')` to count distinct resolved IDs on events that carry an actor. An unlinked event falls back to its own `distinct_id`. Queries that request identity columns (including `SELECT *`) join the project's delivered links and profiles at read time. Select explicit event-only columns to avoid those joins. Profile properties reflect the latest delivered profile; send a property on the business event itself when you need its value at the time the event occurred.

### What capture accepts

- The project comes from the credential on the capture request. Browser identification uses the same public, project-scoped ingestion key as other browser events. Treat supplied IDs and traits as analytics data; authenticate logins and authorize access to application data separately. A `server_only` project still requires an authorized backend.
- `$identify` records the target user's ID and links a supplied `$anon_distinct_id` to that user. A call with no anonymous ID can still update the target profile. Multiple anonymous browsers may each link to the same identified user.
- `$set_once` fills missing traits and `$set` supplies current values, including updates made by a subsequent identify on the same user. Control records travel through the durable queue and stay out of analytics event counts and webhook payloads.
- A link belongs to one project and cannot be reassigned to another user. An already identified user cannot become another user's anonymous predecessor. Cookieless sentinels and server-generated daily cookieless IDs cannot participate. If a control has invalid traits or a conflicting link, Porcupine accepts a durable no-op and increments `identity_ignored` in the capture response. Other valid events in the batch still go through. A profile accepts up to 128 top-level traits, 200 bytes per key, and 32 KiB of encoded JSON.
- SQLite commits identity changes and their projection records in the same capture transaction. The usual project/UUID receipt deduplicates retries. ClickHouse resolves links on reads after delivery, without rewriting historical events. A capture acknowledgment can precede the identity projection.
- General aliasing, merging two identified users, unlinking, and person-profile deletion do not have API operations in this implementation. Event corrections and deletions apply to ordinary events, and supported restores include identity state. The reserved `identity:write` permission does not grant a management API.

## Session semantics

For events carrying a real SDK `$session_id`, the PostHog adapter exposes it in `context.session_id`. In SQL, use `JSONExtractString(context, 'session_id')` and exclude empty values before grouping. Backend events can join the same session when the application propagates the ID. The cookieless profile strips browser session IDs.

The span between the first and last event measures observed activity. A quiet reader produces no new timestamp. If your product needs a live visitor count, instrument activity or heartbeats and choose an explicit time window. The [compatibility matrix](../compat/matrix.json) documents the browser session and reset behavior exercised by the SDK tests.

## Cookieless by default

The supported browser profile uses `cookieless_mode: 'always'` and
`person_profiles: 'never'`. The real SDK tests prove that it writes no cookies,
localStorage, or sessionStorage. The server resolves its `$posthog_cookieless`
marker before committing the event. Explicit distinct IDs remain available for
server integrations and the opt-in identified diagnostic profile.

The daily visitor pseudonym is HMAC-SHA256 over length-prefixed project ID,
canonical client IP, and the HTTP User-Agent, using a random 32-byte daily key.
IPv4-mapped IPv6 is normalized to IPv4. Hostname is not an additional scope:
projects are the isolation boundary. The service never writes the network IP to
its identity table, events, or application logs as part of this calculation.

The identity day is the UTC date when the ingestion transaction holds the SQLite
write lock. It is not the client event timestamp. `received_at` still records
arrival time; a request crossing midnight while queued can therefore carry a
later `identity_day`. Key selection and rotation serialize with event acceptance.
A clock rollback earlier than the latest stored key day causes an explicit 503
instead of recreating an expired identity key. Operators need an accurate clock.

Daily keys are stored in SQLite so a restart does not change identities within a
day. The first successful cookieless transaction on a new day creates a fresh,
independent key and logically deletes older rows. Rotation is lazy, not a timed
background eraser. SQLite WAL, free pages, and backups can retain old key material;
logical deletion is not a claim of secure physical erasure. Protect backups and
the database with the same care as other authoritative state.

The stored event receives the resolved `distinct_id`, `properties.distinct_id`,
`cookieless: true`, and `identity_day`. Client-supplied mode/day markers cannot
override the server. Known IP properties (`$ip`, `ip`, `ip_address`, `client_ip`,
`remote_addr`, `x-forwarded-for`) and browser correlation IDs (`$device_id`,
`$user_id`, `$anon_distinct_id`, `$session_id`, `$window_id`) are removed from
cookieless event properties. Arbitrary nested custom text is not a PII scrubber:
applications must not send personal information there.

This resolver provides a daily estimate, not a durable person identity. Users
sharing a network IP and User-Agent can count as one; a changed IP or browser can
count as a new visitor. Different days and projects deliberately get different
pseudonyms. Multi-day unique users and retention must not be presented as exact
person counts. Cookieless session statistics are not implemented; shared SDK
session sentinels are not retained as real sessions. Identified browsing uses the separate opt-in profile above; a cookieless visitor cannot be retroactively linked to a login.

### Reverse proxies

By default, identity uses the direct socket peer and ignores forwarded headers.
Behind a proxy, configure `PORCUPINE_TRUSTED_PROXIES` with only the proxy IPs or
CIDRs you control. The resolver accepts X-Forwarded-For only from a trusted peer,
validates at most 16 addresses, and walks from the nearest proxy backward to the
first untrusted address. Invalid addresses fail ingestion rather than using an
empty identity input. Other forwarding headers are ignored. Without forwarded
addresses it uses the peer, so a misconfigured reverse proxy can merge visitors;
verify the setup before production use. CORS origin configuration is independent.

Reference: [PostHog cookieless protocol](https://posthog.com/tutorials/cookieless-tracking).
