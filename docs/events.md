# Event correction and deletion API

Event mutations use project-scoped permissions and durable SQLite acceptance. ClickHouse projection is asynchronous. A `202` response means the mutation is queued, not that analytical results have changed.

## Operations

| Operation ID | Endpoint | Permission | Success |
| --- | --- | --- | --- |
| `get-event` | `GET /api/v1/projects/{id}/events/{uuid}` | `events:read` | `200` |
| `edit-event` | `PATCH /api/v1/projects/{id}/events/{uuid}` | `events:edit` | `202` |
| `delete-event` | `DELETE /api/v1/projects/{id}/events/{uuid}` | `events:delete` | `202` |
| `get-event-mutation` | `GET /api/v1/projects/{id}/event-mutations/{mutation_id}` | `events:read` | `200` |

All calls use an API-resource OAuth access token and the path's authorized
project. Exchange client credentials as described in [OAuth](oauth.md). Static
API Bearer keys apply only to legacy mode. HTTP and [MCP](mcp.md) expose the same operations. SQL remains read-only even for administrators; corrections and deletions use these endpoints.

## Current accepted event

`get-event` returns `event`, `revision`, and `pending`. The event contains its durable ID, project, UUID, event name, distinct ID, properties, timestamp, and receipt metadata. `revision` is a decimal string; the original revision is `"0"`. Later revisions are positive SQLite mutation audit sequence numbers and can have gaps for an individual event.

`pending: true` means the current mutation awaits ClickHouse acknowledgment. This endpoint reflects an accepted pending correction immediately. The initial ingestion queue's delivery state is separate. A deleted event returns `410`, including while its tombstone is pending. Unknown events return `404`.

SQL and catalog discovery read the latest projected ClickHouse state. They can therefore return an older revision until mutation delivery completes. Delivery status adds `pending_mutations` alongside `pending` original events. No cross-database snapshot joins these states.

## Full event replacement

`edit-event` accepts this JSON body:

```json
{
	"idempotency_key": "edit-turn-42",
	"expected_revision": "0",
	"snapshot": {
		"event": "conversation_started",
		"distinct_id": "visitor-1",
		"properties": {"client": "acme", "conversation_id": "conversation-42"},
		"timestamp": "2026-11-07T10:00:00Z"
	}
}
```

`snapshot` replaces all four editable fields; properties are not merged. Event name and distinct ID must be nonblank and fit 500 and 1,000 bytes respectively. `properties` must be an object, and `timestamp` must be RFC3339. The timestamp normalizes to UTC. Project, UUID, durable event ID, and original receipt metadata remain unchanged.

Edits preserve the private original accepted snapshot. Readers cannot select it through SQL or discover it in the schema. Corrections do not create identified-person links. A cookieless event's resolved distinct ID cannot change; server-owned cookieless markers and identity day remain enforced, and the usual cookieless property removal still applies. Other events cannot acquire those markers through an edit. The `token` property is removed.

Mutations require configured ClickHouse delivery. A disabled delivery configuration returns `503`. Availability also depends on the event being recoverable from the local queue or ClickHouse.

## Deletion

`delete-event` requires a JSON body even though its method is `DELETE`:

```json
{
	"idempotency_key": "delete-conversation-2026-11-07-1",
	"expected_revision": "1"
}
```

The supplied revision must come from the current event; `"1"` is illustrative. Deletion queues a higher-revision tombstone with blank current and original payloads. It preserves project, UUID, durable event ID, original receipt metadata, and revision. If the original event is still queued, the acceptance transaction removes its SQLite payload and records a receipt atomically. A later revision-zero delivery cannot resurrect the tombstoned event.

After projection, SQL and catalog results exclude the event. Tombstones remain indefinitely; Porcupine does not run `FINAL CLEANUP`. Old ClickHouse parts, S3 object versions, SQLite free pages or WAL, and backups can retain prior payloads until their own asynchronous cleanup or retention processes complete. API deletion is not a guarantee of immediate physical erasure from every copy. See [storage](storage.md) for S3 behavior.

## Idempotency, concurrency, and audit

`expected_revision` is required and must be a nonnegative decimal string within the supported signed 64-bit range. A stale revision returns `409`. Only one mutation per event can be pending; another mutation returns `409` until acknowledgment.

An idempotency key contains 1 through 200 bytes and cannot be blank. Its scope is the project. SQLite stores its hash and a request digest, not the raw key. Repeating the same key, actor credential, operation, event, expected revision, and normalized request returns the existing mutation. Reusing the key for a different actor or request returns `409`. Keep the same key when retrying an uncertain response.

The mutation outbox is bounded instance-wide by the smaller of the ingestion queue event limit and 1,000 entries, plus a 64 MiB payload budget. A full outbox returns `503`. Ordinary request bodies and edited snapshots remain subject to the 1 MiB limit.

Mutation responses and `get-event-mutation` contain `mutation_id`, `project_id`, `uuid`, decimal-string `revision`, `operation`, `state`, `created_at`, and optional `applied_at`. State is `pending` or `applied`. Applied means ClickHouse acknowledged projection. A lost acknowledgment can leave a visible correction marked pending until retry succeeds.

The audit retains actor, operation, revision, time, idempotency hash, request digest, and status metadata. It retains no before-and-after values. Full pending projection payloads leave the SQLite outbox after ClickHouse acknowledgment in the same transaction that marks the mutation applied. The separate private original snapshot remains in ClickHouse through corrections and is blanked by deletion's newest projection.
