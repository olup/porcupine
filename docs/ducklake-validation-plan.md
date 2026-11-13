# DuckLake architecture validation

This is an experiment on `experiment/ducklake`. It changes neither the released backend nor the existing deployment.

## Work list

- [x] Read the experiment workflow principles and inspect the current storage and query boundaries.
- [x] Build a repeatable, bounded Go test runner and record the baseline before changing storage behavior.
- [x] Exercise public query paths at 10,000, 100,000 and 1,000,000 historical events.
- [x] Measure arrival-paced ingestion, accepted-event durability, backlog, eventual analytical visibility and query contention. Per-event visibility latency remains open.
- [x] Measure bounded active catalogs with more projects than the resident working set.
- [x] Verify process-kill recovery before projection and after analytical commit before SQLite acknowledgment.
- [x] Test larger custom properties and resource-exhausting queries.
- [x] Record CPU, whole-container memory, managed disk space and file count alongside performance.
- [x] Review the evidence and state what is verified, not verified or inconclusive.

## Predicates and boundaries

The integrity gates are exact accepted/projected counts after drain, unchanged IDs on retry, no cross-project reads, no partial batch after failure, and a responsive service after a rejected query. An integrity violation is a test failure, regardless of throughput.

The initial service target is 1,000 offered events/s for ten seconds with two analytical workers on a one-million-event history, no rejected requests, a bounded queue and complete drain within 30 seconds. A separate 3,000 events/s run is a stress target. Report request rejections, latency samples, eventual projection counts and remaining backlog. Transient background delivery-failure telemetry remains open; this round does not claim to count those failures. Missing a performance target is NOT VERIFIED, not a reason to hide the result or lower the offered rate.

The larger historical fixture is trusted synthetic setup for analytical measurements, distinct from timed HTTP ingestion. Query results must match known fixture counts. Five queries cover event and user counts, custom dimensions by day, sessions, an ordered pageview/signup/purchase funnel and next-day retention. The fixture repeats complete three-event journeys for stable users across several days. Only product analytics is in scope; logs, exceptions and traces are excluded.

Each scenario runs in a disposable Linux container with two CPU cores and 2 GiB RAM, using the current experiment's 512 MiB engine profile and explicitly stated batch settings. Runtime resource accounting includes native allocations. Resource peaks are container-level measurements; Go allocation counts are not a substitute. Synthetic event data has no customer content.

The first round targets local SSD files, a local catalog and one writer process. It does not establish S3 performance, a remote catalog, horizontal writers, replication, disaster recovery, physical deletion within 30 days, or SaaS tenant containment. Those remain separate gates before a hosted offering.

The runtime profile uses a 1,000-event row cap, the existing 4 MiB conservative byte budget, two native threads and a 512 MiB per-project engine budget. The same total two-CPU/2 GiB container budget and row/byte caps apply to the ClickHouse control. Its existing query memory limit remains 256 MiB per query. These are application profiles within a shared total budget, not identical internal engine limits.

No storage rewrite or default change is required to run this validation. The deliverable is a runnable experiment and a decision report, with negative results preserved. The prior small-fixture and batching measurements remain the baseline.
