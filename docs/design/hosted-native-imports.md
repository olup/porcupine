# Native historical import outcomes

This internal, disabled component commits historical chunks and reconciles their
usage through the existing hosted publication cursor. It has no upload HTTP,
CLI, Service constructor, scheduler, or production acceptance caller. Standalone
imports continue to use the durable SQLite transaction.

## Native transaction

`lake.CommitImportChunk` consumes validated, source-bound rows and parser failure
positions. `importer.ReduceHistorical` supplies the same historical decisions as
SQLite. `ParseChunk.Failures` retains every invalid or oversized row position
within the existing chunk row limit. Native heads deduplicate each UUID before reduction, including deleted
heads. Each following row reads the transaction's preceding profile, link and
person changes. Person and mapping versions use native `UBIGINT`; decimal SQL
parameters preserve the full unsigned range.

The chunk key contains the complete source/project/catalog/stream scope, job,
file, start, expected end, and `bytes` or `finalize` phase. The logical fingerprint
also binds the manifest digest, source, format, previous parser generation and
content reference, incoming byte digest, ordered candidates and parser failures.
Candidate event IDs, receipt clocks and optional S3 version IDs are excluded.

A per-file native head binds the exact original predecessor to its retained
outcome sequence. A second range from that predecessor conflicts even when its
end creates a different chunk key. An exact retry returns the original proof
before classifying rows again. New chunks cannot follow a completed file.

One native transaction commits canonical imported records, identity state,
PostHog person provenance, cumulative usage, the original outcome and the file
head. The common canonical insertion reads each event's explicit imported bit.
Occurrence timestamps remain historical; receipt and accounting times come from
the original native commit. Historical control snapshots contain sanitized
identity evidence. Archived incremental sets never become current profile sets.

The outcome retains every nonblank row's position, result, safe reason, UUID and
original event ID where present. It also retains count deltas, resulting file
counts, bounded samples and both parser checkpoint references. `Unresolved`
overlaps accepted and ignored counts. Samples are limited to 20 across the job;
row outcomes remain complete up to the 1,000-row chunk limit.

Invalid-only, duplicate-only, ignored-person, tail-only, blank and empty EOF
transitions still commit outcomes. Each new transition advances the common
sequence and usage revision with zero event increments when appropriate. Exact
retries advance neither. Zero-candidate callers skip event ID reservation.

## Shared native evidence

`NativeCommitEvidence` has implemented `live` and `import` kinds and one retained,
contiguous sequence in `native_commit_proofs`. The envelope binds full scope,
tenant, original input and proof fingerprints, accounting time, accepted counts,
and cumulative live/import totals. Import evidence embeds its original outcome
and requires an empty live-effects field. No mutation kind is implemented.

`PendingNativeEffects`, explicit live/import lookups, sealing, and
`AcknowledgeNativeEffects` use this same history. They never prune it. Original
outcome lookup and publication remain available after sealing; new native writes
require the open guard. A missing sequence, changed fingerprint, unknown kind,
or mismatched file head fails closed.

The publisher dispatches by kind before live reservation validation. Live proofs
retain actual output plans, immutable body verification and reservation
settlement. Import proofs verify their original parser reference and use
`PublishNoOutputEffectsTx` to advance the same cursor. This participant refuses
any pending live reservation. It creates no plan, proof reservation, webhook
references, output bodies or output jobs. A newly created common cursor charges
one durable metadata row; an existing cursor adds no output capacity charge.

The cursor and `usage.Store.PublishSourceTotalsTx` commit in one PostgreSQL
transaction. Usage SourceID remains the native stream ID. Recovery uses original
UTC accounting buckets and cumulative live/import values, including zero-count
revisions. An uncertain commit response requires an exact checkpoint reread.
Native ACK follows confirmed PostgreSQL coverage and can be retried independently.
Recovery borrows no producer token.

## Schema and parser objects

Native schema step 5 replaces the unpublished live-only prototype proof table
and adds the common history, import file heads and person provenance. Steps 3
and 4 remain unchanged. Empty prototype catalogs have an explicit numbered
upgrade. A populated hosted-v4 prototype is refused before schema alteration
with a rebuild requirement; its existing rows remain intact. Published
standalone schema 2 remains supported. There is no compatibility codec or Go
forwarding API for the unpublished prototype.

`PutImmutableImportCheckpoint` and `ReadImmutableImportCheckpoint` store bounded
private parser objects beneath the exact project stream and hashed job/file
namespace. They use content-addressed immutable keys, verify size and SHA-256,
and preserve optional physical version pins. These objects can contain raw tails
and serialized partial SHA blocks. PostgreSQL receives only metadata and refs.
The original native outcome binds the actual physical refs even when a retry
presents another candidate version for the same logical content.

## Remaining upload boundary

The hosted upload manager, durable PostgreSQL job/file checkpoint store, retained
input objects, authority refresh, manifest order, whole-file digest, cancellation,
and terminal cleanup are separate work. Their controller must hold one common
project session around native fencing, original lookup, commit and publication.
It must reconcile pending evidence before admitting another native transition.
This component does not prove upload authority or change the public import API.

Real isolated PostgreSQL 17, two-engine DuckLake and S3 fixtures cover mixed
history and usage, original native and PostgreSQL response loss, ACK retries,
sealed recovery, zero-accepted outcomes, conflicting successors, unsigned person
versions, ordered identity changes, scoped version pins and prototype refusal.
