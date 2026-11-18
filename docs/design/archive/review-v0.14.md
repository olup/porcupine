# v0.14.0 review follow-up

> Historical record. For current behavior and release status, see [Hosted architecture](../hosted-architecture.md).

The review covered the published v0.14.0 tree. The implementation now combines the earlier merged work and the stabilization commits on a review branch. [Local validation](../stabilization-validation.md) covers the combined branch. CI and release checks remain required. No newer release or deployment is implied.

The review found sound acceptance and SQL-isolation boundaries alongside an oversized operational and documentation footprint. Faster CI was the first response because every later refactor needs frequent feedback. It improves the cost of checking changes. It does not establish throughput, availability, or production maturity.

## Findings and disposition

| Finding | Response | State |
| --- | --- | --- |
| Slow local feedback | Parallel tests and a short local loop; complete race and integration suites remain available | Merged, #46 |
| Tenant budgets absent or query limits fixed | Tenant administration, monthly allowances, and configurable query ceilings | Merged, #43 |
| One producer can fill the shared queue | Per-project queue occupancy and rate control need design and tests | Todo, explicitly deferred |
| MCP forces destructive-tool approval for SQL | Read-only schema and query tools | Merged, #44 |
| Inert backend/cache settings | Removed obsolete inputs and fields; `use_cache` is rejected | Merged, #45 |
| Dispersed schema alterations | Numbered schemas; refuse unknown/newer databases before writes | Merged, #47 |
| Large mixed package | Lake, authentication policy, canonical events, input/import parsing, delivery, and S3 construction have separate packages; service retains durable transactions | Integrated; local validation passed; unreleased |
| Permanent SQLite retry receipts | Retention window plus historical deduplication that preserves edits, tombstones, and stable IDs | Integrated with timestamp-independent retry checks; local validation passed; unreleased |
| Duplicate analytical payload and missing time partitioning | One current event representation, original snapshot only after correction, monthly partitions | Integrated with format checks and interrupted-upgrade recovery; local validation passed; unreleased |
| JSON parsing and repeated time representations | Properties remain flexible JSON; source timestamp text is retained for precision alongside query columns. Profile extraction/scan costs before promoting common properties or changing native types | Further optimization is open, not a shipped performance claim |
| Large authentication surface | Extract code and document simplification options without silently removing login modes | Extraction integrated; mode removal remains a proposal |
| Many environment variables/Compose overlays | Typed JSON, init/check/atomic install, validated overrides, and one Compose entry point | Integrated; real nonroot image setup/custom-path/restart tests passed; local validation passed; unreleased |
| Product guides mixed with experiments | Task guides, detailed reference, separate design records, changelog, shorter agent guide | Integrated; strict HTML/Markdown build and link tests passed |
| Hosted sizing assumption | Shared cells are an option; use measured active-engine/workload limits, not a promised tenants-per-process figure | Architecture to validate |

## Verification required before release

The combined branch adds receipt-pruning regression tests for historical imports and storage failures, as well as schema format and restart tests. The combined race, SDK, S3, restore, and documentation checks passed locally. The [validation record](../stabilization-validation.md) lists the commands and measured limits; release CI must still validate the candidate.

Receipt pruning must never reaccept an old stable ID because its current event was edited or deleted. A failed historical lookup cannot mean "new event". Test retries with changed timestamps, crash recovery, imports, identity controls, and storage unavailability.

Schema changes need recognized version checks, safe interruption/restart behavior, correct identity and mutation projections, and matching backup/restore behavior. Compare actual bytes and query results on representative payloads before generalizing a small fixture's savings.

Run the full [team workflow](../../roadmap.md#validate-the-complete-team-workflow) in standalone and shared modes. Keep full SDK, S3, restoration, race, and authorization tests even when the default local check is shorter. The [design records](../README.md) preserve earlier failed load profiles; refactoring does not turn those measurements into passing capacity claims.

## Scope kept out of this stabilization

No billing engine, new analytical backend, graph-rendering service, logs/traces product, automatic event documentation, or private fork is needed to apply this review. The future commercial controller remains separate and uses the open-source server's authenticated API.
