# PostHog JSONL import semantics

The importer reads the events and persons JSONLines models from
[PostHog S3 batch exports](https://posthog.com/docs/data-pipelines/batch-exports/s3).
Upload decompressed UTF-8 JSONL through the resumable import API. This endpoint
does not read Parquet, fetch remote URLs, or accept compressed upload chunks.

## Event rows

An event requires `uuid`, `event`, `distinct_id`, and `timestamp`. Properties may
be a JSON object or a string containing a JSON object. Missing properties become
`{}`. ISO 8601 timestamps retain their fractional precision in the `timestamp`
column; UTC ClickHouse timestamp text is also accepted. The existing millisecond
SQL columns retain their normal precision.

The export's `team_id` never chooses the destination project. The authenticated
import job determines project scope. The job freezes its creator's authenticated source binding. An unbound creator
uses the default PostHog namespace, where imported UUIDs deduplicate with live
unbound PostHog UUIDs. A bound creator uses the existing source-scoped event ID
derivation, matching live PostHog and native messages with that same source and
message ID. Different source namespaces remain independent. Row fields cannot
choose a source or project, and resume does not change the job namespace. The first
accepted record wins; importing a duplicate cannot change a live record, and a
later live duplicate cannot change an imported record.

`elements_chain` is retained as `$elements_chain` unless that property already
exists. Historical reserved analytics names, including `$autocapture`, are
preserved. Exported `person_id` and `person_properties` are not treated as current
application identity: PostHog documents these event fields as ingestion-time
snapshots.

A previously resolved `cookieless_…` daily ID is retained. The live cookieless
request sentinel is rejected because an import cannot reconstruct its original
day, browser, or IP. Known session, user, device, and address fields are removed
from archived cookieless properties; ingestion credentials and address fields
are not imported as event properties. Arbitrary business properties are not a
complete personal-data scrub.

## Persons and explicit identity evidence

The persons model has one row per `distinct_id`, with `person_id`, `properties`,
optional `person_version`, `person_distinct_id_version`, `created_at`, and
`is_deleted`. Its internal PostHog person UUID is not an application user ID.

A valid persons row hydrates a profile on its original distinct ID without
inventing an anonymous-to-user link. Such a profile remains unidentified until
explicit identity evidence arrives. The importer records an unresolved identity
count for these associations. A higher archive version from the same frozen source can update an import-owned
profile. Versions from different PostHog sources are not compared. Without version fields, only the initial profile snapshot is applied.
A subsequent live profile update or identity link prevents later archive
writes. Stale versions and protected live profiles are ignored. A deleted-person
snapshot can clear only a profile still owned by this importer; it cannot delete
live person state.

An archived `$identify` can establish its explicit `$anon_distinct_id` link only
when both sides are absent or still import-owned and no existing link conflicts.
A target-only `$identify` can identify its stated ID without creating a link.
Incremental `$set` and `$set_once` arguments are not replayed. Stored PostHog
exports can omit those arguments; treating their absence as current traits would
be incorrect. Such controls are retained as hidden, marked no-ops and reported
as unresolved. Unsupported alias and group controls are hidden no-ops; this
import does not reconstruct group identity. Persons snapshots provide the
supported trait hydration path.

## Delivery, progress, and storage

Imported records use honest local acceptance time and `is_imported=1`. Their
original event time is preserved. Interactive SQL and on-demand downloads include
imported analytics history; automatic receipt-window S3 queries exclude it.
Imports do not enqueue individual or batch webhooks. Hidden identity controls do
not increase ordinary analytics event counts.

Each upload checkpoint commits its accepted records and byte offset together.
Status counts distinguish accepted, duplicate, ignored, invalid, and unresolved
rows. Unresolved is an identity diagnostic and can overlap accepted or ignored.
Accepted means durable queue acceptance, not completed analytical delivery; check
the project's delivery status before querying a complete import. Cancellation or
a failed file digest does not remove records accepted by earlier chunks. Invalid-row
samples contain line numbers and static reasons, never row values.

The upload is bounded to 256 KiB and 1,000 rows per chunk, with a 1 MiB maximum
row. Queue limits apply to imports too. Imports initially consume hot storage
and follow the deployment's existing receipt-time cold-storage policy; this
feature does not backdate receipts or change TTLs. Plan SSD capacity for the
archive and import in batches if needed. Queue backpressure limits pending work,
not the final size of retained historical data.
