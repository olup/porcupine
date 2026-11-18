# Hosted native recovery and seal

Unit A, unreleased. This component does not enable hosted buffered acceptance,
production flush publication, pipeline jobs, or webhook sending. Standalone
journal acceptance and the shared native v2 contract remain unchanged.

`ScanHostedProjects` reads at most 256 authoritative registry records in project
order. It retains deletion tombstones so a recovery inventory cannot lose pending
cleanup truth. `LookupHostedProjectExisting` uses the existing bounded resident
engine slots and validates the source, ready registry record, native catalog ID,
stream, schema, and identity authority. It permits a sealed stream. Unknown,
unready, deleted, missing, or unavailable metadata fails closed. Recovery never
calls registry `ensure` or `markReady`, acquires an initialization lock, creates a
native catalog, applies schema DDL, or changes native catalog options. The ATTACH
uses `CREATE_IF_NOT_EXISTS false`. The worker directory remains scratch storage.

A recovery caller first acquires the existing project coordinator session. It
runs cold lookup, fence advancement, native reads, and ACK inside
`ProjectWriteSession.RunNative`. That guard registers actual calls, refuses new
calls during release, and joins their returns before unlocking the physical
control connection. Normal release leaves accepted calls running; session loss
cancels their contexts but still waits for their actual return. A callback that
ignores cancellation keeps release waiting. Callers must give native work bounded
contexts and must not invoke `Release` from inside its own callback. There is no
second coordinator acquisition in native recovery.

`AdvanceRecoveryFence` accepts an open or sealed stream and commits the higher
native epoch/nonce before recovery reads. It never reopens a stream. Ordinary
`AdvanceWriteFence` and `CommitLive` still require an open stream. All three use
existing-only native opens, including on cold workers. Old epochs cannot commit
or acknowledge a successor's stream.

`SealHostedProject` requires the current native epoch/nonce. It takes the same
local writer guard as live commits and writes the same native guard row used by
other engines. It freezes `sealed_effect_seq` at the committed high-water. A
same-engine active commit finishes its native transaction before seal acquires
that guard. A conflicting
other-engine transaction must either commit before the seal cut or retry and
observe the sealed state. A retry returns the same final sequence. Sealing does
not claim that another process's canceled or blocked call has already returned.

`AcknowledgeLiveEffects` requires the current fence and exact stream scope. It
records monotone confirmed publication coverage up to native high-water. The
caller must commit its output references, source usage totals, and control
checkpoint before ACK. A lost ACK response can be retried, and an older cursor
cannot lower coverage. ACK changes no proof, event, head, or tombstone. There is
no proof pruning. Numeric history validation requires exactly one proof for every
sequence from one through high-water, including when the requested cursor already
covers the stream. Pages also validate each returned proof fingerprint. Future
cursors, missing leading/interior/tail history, duplicate sequences, and invalid
coverage fail explicitly with `ErrNativeEvidenceGap`. The numeric validation
scans retained proof metadata and uses constant result memory; paged proof reads
remain bounded to 1,000 records.

The control epoch migration is step 7, after released auth step 5 and usage step
6. Native catalog steps 3 and 4 retain their original ordering and contents.

## Remaining lifecycle work

The existing shared v2 mutation/import, maintenance, and direct deletion entry
points do not own hosted epoch or durable effect outcomes. They explicitly refuse
hosted catalogs with `ErrHostedNativeGuardRequired`. This includes compaction
CALLs, which DuckLake cannot combine with the native guard row mutation in one
transaction. The refusal keeps those callers from bypassing a hosted seal. It is
a temporary activation gate until those callers use the common coordinator and
recoverable native outcome protocol.

`MaintainHostedProject` now supplies a separate explicit guarded compaction path
under the caller's existing session. Its [native snapshot fence](hosted-maintenance-fence.md)
reads the epoch in the CALL's transaction, disables native commit retries on a
dedicated bounded engine, and revalidates the held session for external retries.
The old entry points remain closed. No background maintenance activation or
cross-worker reader-drain protocol is claimed here. Physical hosted deletion
must retain control intent, seal and publish through the final sequence, join actual writers and readers across
workers, preserve restore/object holds, then prove catalog/object removal. The
legacy direct hosted deletion path refuses the operation before modifying native
or registry deletion state. Registry tombstones remain authoritative.

Destructive history pruning additionally needs an explicit retained-history floor,
a floor-aware recovery reader, proven restore horizon and immutable-object
ownership. A control checkpoint restored behind unavailable proof must fail;
ACK alone cannot authorize pruning, source-key replacement, or recharging old
history. Production providers and every remaining Phase 5 caller migration are
separate units.

## Hosted project deletion staging

Hosted startup can bind the final-seal callback to the existing deletion
recovery loop once all hosted providers participate. Direct project deletion
commits the control tombstone before returning that cleanup is pending.
Tenant deletion uses the existing transaction to fence all of its projects.
Recovery enumerates those fences in PostgreSQL without claiming a worker
journal. The hosted capability remains unbound in ordinary service startup.

Hosted work that can touch project files or send external requests registers
an opaque, unique activity ticket in PostgreSQL before dispatch. The
transaction locks the active tenant and project and then checks both
tombstones in a new statement. This serializes ticket registration with
deletion, including readers which waited for a row lock. Operation owners
remove their ticket only after dispatch has returned. Hosted query permit
expiry can free concurrency capacity while the ticket still blocks cleanup.
Hosted live commits also recheck the control fence and hold the same row
locks across the native commit. Independent PostgreSQL session failure can
interrupt this protection. A higher native fence and the immutable seal cut
still determine which native writes committed.

Each ticket references the registering process's worker incarnation. At
start, a hosted process inserts a random incarnation row with `expires_at`
(lease, 30 seconds) and `reclaim_after` (lease plus a 45-second grace). It
renews both every 5 seconds. Renewal cannot revive an expired row. The
process measures its local deadline from before each successful grant or
renewal statement, so the deadline never falls after the PostgreSQL expiry.
Registration is counted locally before its transaction commits and requires
a current lease, both locally and in PostgreSQL. When the local deadline
passes, the process refuses new tickets. With write-capable activity still
running, the keeper exits with status 75 so the supervisor replaces the
worker. Otherwise, it rejoins under a new incarnation. A read-only query
alone does not stop the worker. It can fail if it still needs data after
cleanup starts.

Seal and cleanup first delete incarnations past `reclaim_after`. The
foreign key cascades to their tickets. If the worker loses its lease
during a DuckLake write, the sealer acquires the project write session
and advances the native epoch. An older native transaction can win before
that advance and become part of the seal cut, or the new epoch can win
and reject the stale commit. The control project tombstone continues to
fence new work.

Raw object writes and external deliveries have no provider-side fence.
Automatic reclamation assumes the supervisor and operating system let
the worker exit at its deadline, and that requests already accepted by an
external service settle within the grace. A client timeout bounds its
wait, while a provider can continue processing an accepted request. A
worker suspended past the grace or a provider that completes a write after
it can leave an object outside the cleanup inventory. An object alone
cannot publish an event or a result. Hosted publication still requires
the corresponding fenced native proof or the current durable output
claim. A webhook already accepted by its recipient cannot be recalled.

A returned call whose acknowledgement failed is retried with the original
ticket while the process lives. Schema step 20 leaves existing tickets
with a NULL incarnation. Their age does not establish that the operation
stopped. Drain workers before the upgrade. If a legacy ticket survives,
stop all pre-upgrade executors and block their restart. Verify any
outstanding uploads and external deliveries have settled at their providers.
An operator with database access can then remove only the verified legacy
ticket by its project and activity ID, with `incarnation_id IS NULL`.
No public endpoint clears activity tickets.

Once all actual-return tickets clear, the deletion worker can acquire a new
native write fence, verify the seal cut, publish the final committed
effects and require a native acknowledgement through that exact sequence.
It records the project, tenant, source, catalog, stream and final proof
fingerprint in PostgreSQL. An unavailable catalog or missing proof keeps
deletion pending. Sealed recovery can settle a committed import or mutation
only against the exact pending tenant/project tombstone and existing stream
cursor. It updates the original import checkpoint or mutation receipt in
the publication transaction, and preserves webhook cancellation for
committed deletes. A mutation still staging or ready after that replay
keeps the control seal pending.

A recorded seal leaves the project and tenant tombstones pending while
shared job inventory, retention and physical object reconciliation are
outstanding. The seal path leaves catalogs and objects readable for restore
and makes no physical purge claim.
