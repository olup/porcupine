# PostgreSQL native effect metadata participant

> Historical component handoff. See [the active publication protocol](../hosted-architecture.md#flush-and-publication).

This component is disabled until an integration adapter uses it. It implements
control migration 8; existing migration 7 is unchanged. No production provider,
HTTP sender or native ACK path is activated.

`PGPipelineEffects` has no Service pointer or native runtime dependency. Its
methods operate on the caller's `*sql.Tx` and never commit. The publisher calls
`PublishReservedEffectsTx`, the real `usage.Store.PublishSourceTotalsTx`, and
commits once. The publication participant owns the contiguous full-scope stream
checkpoint. A rollback reverses output rows, reservation settlement, capacity
and checkpoint together. Only confirmed control commit permits native ACK.

## Admission and settlement

Acquire the common project coordinator session before beginning transactions.
Mutations lock instance, tenant and project capacity in that order, then stream,
reservation, jobs/targets and current authority. All API mutation adapters must
use that same session. No row-locked transaction waits for a project session.

Freeze a real plan, prepare canonical native-selected output metadata, then
reserve exact matching references and conservative byte/row/artifact/proof demand
before uploading any object or committing native. `ReserveEffectsTx` takes the
prepared outputs and verifies their digest, exact member count, concrete object
charges and plan/manifest object allowance before admitting the reservation.
The binding carries an explicit conservative manifest-byte limit; publication
requires the verified immutable manifest reference to fit that limit. Plan IDs include detached rule
references and creator/incarnation/destination revisions. The binding also records the pre-PUT plan content reference, prepared-output
content digest, batch, epoch, acquisition and attempt artifact prefix. Prepared
body digests exclude the optional version ID which storage returns after PUT;
publication retains actual version pins and the integration verifies them.
Verified zero selected outputs may omit plan/manifest objects entirely, while
the real frozen PG plan survives until settlement. Their transient metadata is
released without creating artifact cleanup, retaining the charged stream cursor. A reservation stores ownership before external I/O.
The prefix must be an actual disjoint attempt namespace, never a shared bucket
root. Storage/version/multipart enumeration and immutable-object verification
belong to the integration adapter, which must preserve the original namespace.

There is at most one unresolved reservation per full source/project/catalog/
stream. Every reservation consumes one proof slot even with zero matching
outputs. No API releases capacity based on age or lease expiry. A trusted native
adapter must fence a successor and provide matching proof or proven absence.
`PipelineSettlementDecision` returns unresolved otherwise; the module does not
invent native absence. Cold recovery reads `ReservationTx` without producer data.

Configure positive finite capacity at instance, tenant and project scopes before
admission. Existing webhook defaults are 10000 references/64 MiB instance and
1000 references/16 MiB project; tenant and metadata/artifact/proof caps must be
chosen explicitly by the integration config. No implicit free allowance exists.
All scopes share the same aggregate counters. Fragment capacity includes at
least 1024 bytes of singleton envelope headroom per canonical member. Actual
object bytes and member metadata must fit the reservation. Conversion requires
no new free space after policy reduction. Remaining artifact/metadata allowance
moves to cleanup ownership, so uncertain upload history continues to backpressure.

Publication deletes settled reservation and frozen-plan rows. The cumulative
stream checkpoint remains; there is no eternal per-batch accounting ledger.
Jobs and cleanup inventory retain charges until verified cleanup CAS removes
them. A permanent stream checkpoint retains one metadata-row charge after batch
cleanup; later reservations on that stream do not recharge it. Proven-absent cleanup uses the original acquisition ID and disjoint prefix.
A same-batch retry may reserve a new acquisition while the old uncertain
artifacts remain charged. Publication cleanup retains its batch owner and keeps
referencing jobs protected. Cleanup CAS binds owner, generation and exact prefix. Native seal/history/restore barriers must govern eventual stream checkpoint
removal; this component supplies no unsafe checkpoint-pruning operation.

## Authority, destination changes and claims

Construction requires a real `PipelineAuthority`. It resolves the saved creator
in the current transaction after locking. The included
`PipelineNativeCredentialAuthority` supports explicit tenant native credentials,
with locked current project, credential and lifecycle rows plus shared
`auth.Authorize`. Human consent and derived service-key authority fail closed in
this implementation and need the shared auth adapter. Producer JWTs are never
execution authority.

`CompareAndSwapDefinitionTx` is an unactivated metadata API participant. Pause
retains backlog. Destination revision includes URL, authentication and account
identity; replacement or creator/incarnation change retires pending/running old
jobs with a version increment. Old immutable bodies keep their original target
revisions. Rule edits affect future selection, preserving previously frozen
rules. Full encrypted destination persistence and existing public API migration
remain outside this scoped component.

Claims use database time, attempt nonce, version, full scope, incarnation and
frozen generation. Expired claims can be taken over; generation survives.
Renew/progress/success revalidate current target and creator. Stale attempts
cannot checkpoint, finish or remove cleanup ownership. Completion retains
capacity until cleanup. Cleanup claims cannot cover a prefix while jobs from
that batch still reference it. Cleanup proof must cover all versions and
multipart uploads; DeleteObject alone is insufficient. Unknown storage or failed
cleanup leaves ownership charged.

## Remaining integration

This is not a complete hosted PipelineJobs provider. The existing API/CLI/MCP
adapters, shared builtin consent/service credential resolver and encrypted
configuration have not migrated. Cross-fragment frozen assembly claim/result
transactions still need implementing before hosted batch assembly activation.
Per-fragment progress CAS here does not itself freeze cross-fragment membership.
There is no HTTP sending/frozen gate: native fencing, reconciliation through the
cut and durable DELETE-intent ordering must precede a future sender transition.
S3 immutable read-back and cleanup proof verification remain adapter operations.
Real native/PG/S3 crash/lost-response integration is required in Unit C. No S3 or
native test was run for this metadata-only package.

Monthly ingestion limits remain the existing grouped cached policy, roughly ten
seconds. No billing escrow/month permit is added. Already admitted native work
may finish after policy reduction.
