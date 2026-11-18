# Disabled native live publication adapter

> Historical component handoff. See [the active publication protocol](../hosted-architecture.md#flush-and-publication).

`hostedLiveFlusher` has no Service constructor caller. Buffered acceptance remains
rejected by configuration. This component exercises the real control PostgreSQL
pipeline participant, actual usage store, native transaction and immutable S3
objects. It does not activate a public PipelineJobs provider, scheduler, assembler
or HTTP sender.

The common project session owns the entire native callback, including existing
scope discovery, native fencing, complete-history reads, commit and ACK. Recovery
fences before reading high-water. A single unresolved reservation is reconciled
before another native batch. Same-batch proof lookup verifies the original input
fingerprint before policy or capacity admission. All-known UUIDs under a new
batch ID bypass the selected stager too. New identity controls call the stager
even when they produce no ordinary output events.

The routing plan comes from real PG definitions, immutable rule configuration
and current saved-creator authority. Explicit native service keys lock their
current key, parent, tenant associations and OAuth client, then use the common
current-key/parent containment checks and `auth.Authorize`. Human consent is not
implemented by this adapter. No producer token enters the plan.

Preparation uses the native-selected canonical events and encodes the direct
bodies or batch fragments before capacity acquisition. A short PG transaction
rechecks the actual plan and owns numeric capacity and the disjoint acquisition
prefix before the first PUT. Content key/size/SHA determine the prepared digest;
actual returned optional S3 version IDs remain in the native manifest and jobs.
The manifest reserves a bounded allowance before PUT, including escaped optional
version identifiers. A selected empty output set creates no plan/body/manifest
objects. Its real frozen PG plan survives until publication settlement.

The publisher reads and verifies bounded immutable manifest, pinned plan and
body references before its PG transaction. Pipeline output conversion, the
contiguous full-scope cursor, reservation settlement and
`usage.Store.PublishSourceTotalsTx` commit together. Native StreamID is usage
SourceID. Original native UTC buckets, revisions and cumulative Live/Import
numbers remain unchanged. Commit-response loss requires an exact checkpoint
reread. Only confirmed PG coverage releases volatile input. Native ACK failure
leaves the publication committed and recovery retries ACK without recounting.

Failed/unknown native results keep their reservation until a committed successor
fence and complete native history establish proof or absence. Proven-absent
attempts retain charged cleanup under their original acquisition ID and prefix.
A same-batch retry uses another acquisition namespace while old uncertain
artifacts stay charged. Publication cleanup remains owned by its batch ID. No
cleanup adapter or physical deletion/drain proof is provided here.

Monthly ingestion policy uses one grouped shared control read cached for about
ten seconds. It is a soft admission policy, with no per-event control writes or
monthly permit. The selected stager checks it once before reservation/upload.
The native transaction supplies AccountedAt. Production admission still requires
the remaining shared providers and full lifecycle integration.

Native history now uses the common `NativeCommitEvidence` envelope with explicit
live and import kinds. Import dispatch validates the retained original outcome,
advances the same cursor and publishes cumulative usage without a live plan,
reservation or output job. Metadata-only transitions advance sequence and
revision with zero accepted events. Pending live reservations cannot be skipped.
The [native import staging record](hosted-native-imports.md) records
schema 5, original outcomes, parser references and the remaining upload boundary.

Remaining activation gates include the full encrypted public PipelineJobs API,
builtin revocable human consent, cross-fragment frozen assembly transactions and
the native-fenced final send gate. ACK does not authorize history pruning or
catalog removal. No worker owner routing or persistent worker volume is needed
by this adapter; existing durable standalone acceptance is unchanged.
