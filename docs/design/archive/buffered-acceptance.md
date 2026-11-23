# Buffered acceptance component

> Historical record. For current behavior and release status, see [Hosted architecture](../hosted-architecture.md).

> Historical Phase 3 component notes. For the released configuration and behavior, see [hosted setup](hosted-profile-composition.md).

## Shared input and distinct outcomes

Native, Segment, and PostHog live handlers share authorization, project/source binding, validation, timestamp normalization, and source-scoped UUID calculation. `liveAcceptor` accepts a private validated batch and reports its durability. A closed result interface has separate durable and buffered variants. The transport rejects unknown result variants.

Pending identity inputs contain a parsed operation, target, anonymous ID, and trait updates. Their event fields contain no identity commands. SQLite still applies those inputs after UUID deduplication in the transaction that writes its canonical identity snapshot, consumes usage, and schedules webhooks. Invalid SDK controls remain accepted no-op markers; malformed outer batches still fail atomically.

Standalone responses retain every current field and add `durability: "durable"`. Buffered native responses use HTTP 200 and contain `status`, `durability: "buffered"`, `received`, and ordered receipts with only `message_id` and `uuid`. `received` counts validated memory-admitted input rows, including retries. Committed unique counts, identity outcomes, original event IDs, and duplicate status remain unknown. The [native contract](../../reference/native-event-contract.md#acknowledgment-metadata) describes discovery and schemas.

Historical uploads retain their existing SQLite transaction and durable checkpoint path. They never enter live memory admission.

## Buffer ownership and limits

`bufferedAcceptance` owns cloned JSON, strings, and parsed identity inputs. It resolves cookieless pseudonyms before queueing, using a durable shared-key resolver and a frozen UTC admission day. No raw peer address, user agent, or daily secret enters a pending batch. An unavailable resolver or a daily key whose length is not exactly 32 bytes rejects cookieless admission. State and capacity checks precede provider calls, and a final locked check reserves capacity after resolution.

Global and per-project byte/event limits cover both pending and in-flight batches. A separate project-count limit bounds empty map and queue overhead. The byte charge includes payload bytes plus 1,024 bytes per row and 512 bytes per active project. These conservative allowances cover structures, allocation metadata, and spare slice capacity. They are accounting limits, not a hard Go heap or process-RSS guarantee. Request handlers, response receipts, native memory, caches, and garbage collection also need container headroom.

The complete request reserves capacity under one lock. Rejection never queues part of a request. A full, draining, or flush-failed worker rejects admission with HTTP 503 and `Retry-After: 1`. Failed batches remain accounted and keep their original batch ID and contents through bounded backoff retries. One failed project blocks new worker admission until its flush recovers.

The component defaults are a two-second timer, 5 MiB flush threshold, 64 MiB global charge, 8 MiB per-project charge, 100,000 global rows, 10,000 per-project rows, 1,024 active projects, and four concurrent flushes. They are provisional component settings, not available production configuration or capacity claims. A timer does not bound pending-event age or crash-loss age during an outage.

`Drain(ctx)` stops admission and flushes pending work. Capacity is released only after writer success. A deadline returns an error with outstanding work still retained. Lifetime cancellation stops dispatch but continues processing completions until every active writer returns, including writers that ignore cancellation. The component reports abandoned rows separately and marks itself stopped only after those returns. A drain deadline never proves that native calls stopped. The hosting process must drain before closing the native store, and must provide adequate termination grace. Phase 3 tests this lifecycle with controlled writers; it does not replace the real service shutdown sequence yet.

## Durable writer and shared-state dependencies

`liveBatchFlusher.FlushLive` receives a stable batch ID, project, validated inputs, per-row admission times, oldest time, and byte charge. The writer must preserve batch identity across retries and must not modify or retain the input after return. A nil writer is rejected by the constructor.

Writer success requires a guarded durable native commit and recoverable effect evidence. Phase 4 supplies authoritative UUID/head/tombstone deduplication, IDs, identity, and replay-safe usage/output scheduling. The write guard must also recheck authoritative project and tenant deletion fences. A lost response after commit must be safe to retry under the same batch identity.

Phase 5 supplies durable shared cookieless keys, authoritative admission policy/fences, operational state, and removal of owner-local shadows. The current private component has no SaaS-specific callback, event queue in PostgreSQL, disk spill, or DuckLake data inlining. A replacement writer must satisfy the same commit contract for any deployment.

`acceptance.mode` defaults to `durable`. The reserved `buffered` value fails validation before opening storage. Shared PostgreSQL does not select it automatically. Only test fixtures attach a buffer to `Service`; there is no fake production writer or healthy hosted mode that silently discards data.

## Admission-only measurement

On an Apple M1 Pro, darwin/arm64, two 100 ms exploratory samples measured the following single-project batches after validation. These samples preceded the final lifecycle and admission-precheck review fixes. They do not describe final implementation throughput. Each sample clones a fixed prepared request and constructs receipts. An attached no-op writer stays idle because thresholds are raised. Fixtures discard pending memory outside the timer after every 128 admissions. Duplicate producer IDs are admitted as input rows.

| Input rows per admission | Time per admission | Allocated bytes per admission | Allocations per admission |
| --- | --- | --- | --- |
| 1 | 762 to 774 ns | 1,659 | 15 |
| 100 | 52 to 53 µs | 218,507 to 218,565 | 1,104 |
| 1,000 | 435 to 495 µs | 2,473,690 to 2,511,280 | 11,005 |

The corresponding 128-project samples measured 1.74 to 1.75 µs, 33.5 to 34.1 µs, and 268 to 280 µs per admission. Each fixture charges approximately 1,433 retained bytes per input row, plus project overhead. Allocated bytes include temporary and replaced slices; the charge describes retained buffer accounting and does not measure RSS.

Before Phase 3 there was no memory admission component to compare. An initial implementation copied the full growing pending slice on every request. The measurements exposed that cost, and admission now appends into owned storage with bounded spare-capacity accounting. The figures above do not measure durable commits, storage failures, S3, or hosted throughput.

Run the current component benchmark with:

```sh
CGO_ENABLED=0 go test ./internal/acceptance -run '^$' \
  -bench '^BenchmarkBufferedMemoryAdmission$' -benchtime=100ms -count=2
```

The benchmark runs without the native engine or a network connection. Correctness tests cover capacity atomicity, concurrent admission, in-flight retention, retry identity, failed-flush backpressure, timers, flush concurrency, deadline expiry, immutable command ownership, cookieless day/privacy, native/vendor contracts, project scope, historical bypass, and startup availability. Full architecture validation remains separate.
