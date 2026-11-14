# Large-event delivery

Development work after v0.13.0. This behavior is not in the published v0.13.0 image.

Porcupine accepts events into its bounded SQLite queue before replying to the producer. A background worker projects them into DuckLake. An acknowledgment from the ingestion API therefore means durable acceptance, rather than immediate analytical visibility.

The native worker normally selects up to 1,000 events with an estimated 4 MiB memory budget. The estimate includes properties, snapshots and other strings; it is not the HTTP payload size. Large properties can reach this budget before the row limit.

## Recover from a native memory error

Each project initially receives one transaction for the selected events. If DuckDB returns its typed out-of-memory error, Porcupine retries that transaction with a smaller chunk. A successful chunk commits before the next chunk starts. The smaller size applies to the rest of that project batch.

Porcupine checks the operation deadline between attempts and permits at most 16 chunk-size reductions in one project batch. It does not split batches for permission, network, storage or other unknown errors. Those errors use the normal durable retry path.

SQLite acknowledgment happens only after the entire selected batch succeeds. A failure can therefore leave some events visible in analytics while all selected events remain queued. Replaying them preserves their event IDs and revisions. An older replay cannot undo an edit or revive a deleted event.

If even one event cannot fit, it remains queued. Memory errors never quarantine accepted events. The delivery worker backs off and continues its queue scan; operators should inspect delivery status and provision enough memory for that event.

## Resource limits

The default DuckDB memory setting is 512 MiB per open project engine. This is not a cap on the server's total memory. Go allocations, DuckDB allocations outside its managed budget, concurrent queries and other open projects also consume memory. Use a container or host limit with headroom for the complete process.

Automatic chunk reduction improves recovery from a reported DuckDB memory error. It cannot recover work inside a process killed by the operating system; durable SQLite acceptance provides restart recovery in that case.

## Verify the behavior

Run the native race suite:

```sh
make ducklake-test
```

The tests distinguish the selected delivery batch from HTTP request batches and direct storage calls. They check native memory failures, durable retries, stable IDs and mutation behavior. Local fixture timings do not establish a hosting throughput guarantee.

With 15,391-byte properties and two native threads, the raw transaction probes produced these results:

| Native memory | Delivery limits | Selected rows | Raw transaction | Adaptive delivery |
| --- | --- | --- | --- | --- |
| 512 MiB | 1,000 rows / 4 MiB estimated | 62 | Passed | Passed |
| 512 MiB | 100 rows / 8 MiB estimated | 100 | Typed out-of-memory error | Drained without changing the configured limits |
| 512 MiB | 10 rows / 4 MiB estimated | 10 | Passed | Passed |

These probes test one selected raw transaction per configuration. They do not establish that every transaction in a prolonged workload will avoid memory pressure.

A separate HTTP journey accepts 1,000 wide events, verifies every analytical ID, then kills the process with originals and two mutations still queued. Restart recovery checks the edit, deletion, identify links and a second project. The tests also cover a 3,000-event delivery selection and 100 wide mutations at 128 MiB, where raw transactions fail and adaptive delivery drains the queues.
