# v0.14.0 review follow-up

The review covered the published v0.14.0 tree. Work listed below is on the development branch or under review; no newer release or deployment is implied.

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
| Large mixed package | DuckLake extraction; authentication and delivery boundaries follow | Lake merged, #48; remaining work under review |
| Permanent SQLite retry receipts | Retention window plus historical deduplication that preserves edits, tombstones, and stable IDs | #49 under review |
| Duplicate analytical payload and missing time partitioning | One current event representation, original snapshot only after correction, monthly partitions | #50 under review |
| Large authentication surface | Extract code and document simplification options without silently removing login modes | In progress; API removals need a decision |
| Many environment variables/Compose overlays | One configuration file, init/check, validated overrides, and consolidated Compose entry point | In progress |
| Product guides mixed with experiments | Task guides, detailed reference, separate design records, changelog, shorter agent guide | This documentation change |
| Hosted sizing assumption | Shared cells are an option; use measured active-engine/workload limits, not a promised tenants-per-process figure | Architecture to validate |

## Verification required before release

Receipt pruning must never reaccept an old stable ID because its current event was edited or deleted. A failed historical lookup cannot mean "new event". Test retries with changed timestamps, crash recovery, imports, identity controls, and storage unavailability.

Schema changes need recognized version checks, safe interruption/restart behavior, correct identity and mutation projections, and matching backup/restore behavior. Compare actual bytes and query results on representative payloads before generalizing a small fixture's savings.

Run the full [team workflow](../roadmap.md#validate-the-complete-team-workflow) in standalone and shared modes. Keep full SDK, S3, restoration, race, and authorization tests even when the default local check is shorter. The [design records](README.md) preserve earlier failed load profiles; refactoring does not turn those measurements into passing capacity claims.

## Scope kept out of this stabilization

No billing engine, new analytical backend, graph-rendering service, logs/traces product, automatic event documentation, or private fork is needed to apply this review. The future commercial controller remains separate and uses the open-source server's authenticated API.
