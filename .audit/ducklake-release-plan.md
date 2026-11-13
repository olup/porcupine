# DuckLake release validation

1. Read the engineering principles and preserve the ClickHouse reference and unrelated worktree edits. Done.
2. Test bounded native compaction repeatedly for at least three minutes at 1,000 offered events/s with two analytical readers. Record actual rate, queue/freshness, maintenance progress and exact eventual data. A failed or inconclusive capacity predicate is not a pass.
3. Exercise compaction without the projection mutex against ingestion, identify, edits, deletion and retry/restart. Keep normal domain projection serialized. Adopt concurrent maintenance only if it both progresses and preserves exact results.
4. Prepare a native, nonroot server image with the compatible DuckLake extension prebundled; retain six CGO-free standalone CLI archives. Cover default local deployment and previously shipped exports/pipelines, or explicitly identify any remaining release blocker.
5. Update released-service documentation and generated CLI contracts. Run real artifact journeys, full checks and an independent review.
6. Merge the verified release change, publish v0.11.0, verify image architectures and six CLI assets, then deploy to Lightsail. Preserve an offline rollback checkpoint and credentials; use synthetic project-scoped ingestion/query/identity/mutation/pipeline tests. Do not connect production producers.
