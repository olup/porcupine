# DuckLake release validation

1. Read the engineering principles and preserve the ClickHouse reference and unrelated worktree edits. Done.
2. Test bounded native compaction repeatedly for at least three minutes at 1,000 offered events/s with two analytical readers. Record actual rate, queue/freshness, maintenance progress and exact eventual data. A failed or inconclusive capacity predicate is not a pass.
3. Exercise compaction without the projection mutex against ingestion, identify, edits, deletion and retry/restart. Keep normal domain projection serialized. Adopt concurrent maintenance only if it both progresses and preserves exact results.
4. Prepare a native, nonroot server image with the compatible DuckLake extension prebundled; retain six CGO-free standalone CLI archives. Cover default local deployment and previously shipped exports/pipelines, or explicitly identify any remaining release blocker.
5. Update released-service documentation and generated CLI contracts. Run real artifact journeys, full checks and an independent review.
6. Merge the verified release change, publish v0.11.0, verify image architectures and six CLI assets, then deploy to Lightsail. Preserve an offline rollback checkpoint and credentials; use synthetic project-scoped ingestion/query/identity/mutation/pipeline tests. Do not connect production producers.

## Verification checkpoint

- Native COPY/FIFO export, project-window isolation, revocation and cancellation tests passed. Exact native JSON numbers are preserved.
- Stopped full-volume restore, backend-mistake reopening and private identify-control guards passed.
- Size-filtered concurrent maintenance preserved all 180,000 IDs and six lifecycles in each controlled comparison. Removing the maintenance projection lock did not improve overall throughput on the tested host.
- Actual Open defaults now use 512 MiB and up to 1,000 rows / 4 MiB. The final configured run preserved all 90,000 IDs, six lifecycles and 291 exact query results and drained its queue. Both 1,000/s and 500/s performance predicates remain NOT VERIFIED; see docs/ducklake-maintenance-soak.md.
- PR #27 merged and v0.11.0 published. Both Linux image architectures, six standalone CLI archives and their checksums were verified. Public hosting and agent docs were deployed.
- Lightsail now runs the official v0.11.0 image. Existing access was retained and all 16,076 prior test/research events were restored with matching event/day counts. Live HTTP, published CLI and connected MCP checks passed, including native/Segment retry IDs, identity, mutations, isolation, batch webhook and S3 exports/pipelines.
- The S3 pipeline initially failed because the test credentials lacked ListBucket for missing-manifest lookup. After granting the documented permission, retry succeeded and the downloaded three-row output matched its SHA-256. Temporary receiver removed and synthetic pipelines paused.
- Production producers remain disconnected. Native analytical data stays on local SSD; S3 destination exports do not provide native analytical tiering. See docs/ducklake-maintenance-soak.md for the completed release checkpoint.
