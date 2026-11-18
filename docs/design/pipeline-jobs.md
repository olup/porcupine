# Pipeline jobs

`internal/output.PipelineJobs` owns the SQLite pipeline domain. The service constructs it with `NewSQLitePipelines` and narrow dependencies for current authority, native reads, admission and project activity. The domain has no `Service` pointer. Existing HTTP, CLI and MCP operations share the existing Huma registration and authorization policy.

## Ownership moved

| Previous owner | Current owner |
| --- | --- |
| `internal/porcupine/pipelines.go` | `pipeline_sqlite_runtime.go`: encryption, key fingerprint, endpoint allowlist, receipt admissions and worker lifetime |
| Pipeline SQL and destination encoding in `pipeline_api.go` | `pipeline_sqlite_operations.go`: define, read, pause, resume, replace, delete, run pages and retry |
| `pipeline_s3_types.go`, `pipeline_s3_runs.go`, `pipeline_s3_worker.go`, `pipeline_s3_sink.go` | Corresponding `internal/output/pipeline_sqlite_s3_*` files: rules, cursors, claims, coherent native query attempts, multipart checkpoints and winning manifest publication |
| `internal/delivery/webhook_{enqueue,batch,queue,queue_types}.go` | `pipeline_sqlite_webhook_*`: queue capacity, members, freeze, cancellation, claims, attempts, cooldown, retry and history |
| `project_pipeline_cleanup.go` and pipeline SQL in `project_purge_sqlite.go` | `pipeline_sqlite_cleanup.go` and `pipeline_sqlite_artifacts.go`: project cleanup and guarded final metadata removal |
| Pipeline artifact SQL in `native_restore_artifacts.go` and `native_restore.go` | `PipelineRestoreArtifacts` and `ResetRestoredPipelinesTx`: original ownership holds and paused, credential-free restored state |
| `managed_artifact_ownership.go` | The shared helper in `internal/output/export_artifacts.go` retains export and pipeline artifact ownership without a Service receiver |
| Service worker construction and lifecycle | `pipeline_dependencies.go` supplies narrow dependencies. `PipelineJobs` starts, cancels and drains its workers |

`internal/delivery` retains pure payload and row encoders, bounded HTTP responses and SSRF-safe transport. `internal/exportstore` retains S3 protocol operations. Export and pipeline cleanup share the existing artifact ownership helper.

## Transactions and worker lifetime

The transaction-only enqueuer joins the same SQLite transaction as live acceptance, event identity and quota changes. Event mutation joins that transaction for webhook member cancellation. A batch remains open across acceptance calls until its ReceivedAt window closes or the chunk reaches 100 events or 1 MiB. An oversized singleton is retained. Once attempted, its body and delivery ID stay frozen across retries. Deleted members of an attempted body create a new survivor ID.

Pause keeps existing backlog. Current creator authority and project containment are checked before delivery. Builtin human consent remains revocable. External mode requires a saved native execution credential. Webhook ACK usage and S3 encoded data byte usage commit only with their winning state transition in the same SQLite transaction.

Each webhook claim has a fresh nonce. S3 claims also have a fresh nonce, separate from the immutable data attempt prefix retained during manifest recovery. Stale claims cannot checkpoint, finish or record usage for a newer claim. S3 uses the shared batch admission and retains native and project activity until the native call actually returns. Shutdown cancels the context and waits for that return.

Every S3 query attempt uses the current coherent native read view. A retry before manifest publication may observe newer corrections. A persisted manifest is reloaded and compared with the exact immutable remote manifest before finalization. No SQL rerun replaces that artifact. A late durable commit in an already published ReceivedAt window does not rewrite the old manifest or cursor. There is no new time travel or rerun API.

## Credential replacement and cleanup

Credential replacement intentionally changes the previous retargeting behavior. Replacing a webhook URL, headers or authentication retires old queued, failed and sending work. An HTTP request already in flight may reach its receiver, but its late ACK cannot restore the canceled delivery or add usage. New events use the replacement destination. Replacing credentials at the same URL has the same rule.

S3 replacement rejects an actual running local attempt with a retryable conflict. It atomically retires old queued or failed windows and advances past them. If an attempt prefix, manifest or file checkpoint remains, journal migration 8 copies their original destination configuration, encrypted credentials, manifest, immutable attempt prefix and multipart evidence into independent retirement records before replacing the live secret. It does not require the old credentials or network to work. These records have no foreign keys to live projects, actors or pipelines. Live pipelines and retired destination records share the existing limits of 64 per project and 512 per instance.

Cleanup uses only the retained old identity and namespace. A missing upload ID triggers paginated reconciliation for the exact owned key, covering a crash between remote Begin and upload-ID persistence. Prefix-only attempt ownership is retained independently of run history and reconciled within its immutable attempt directory. Neighboring keys are excluded. A matching committed manifest transfers ownership to the customer and its data is retained. Unknown, conflicting or unavailable commit state leaves cleanup pending. Before deleting private checkpoints, cleanup preserves the existing opaque managed ownership inventory. Deleting an object does not prove removal of noncurrent versions.

Canceled run history retains the latest 1000 windows per pipeline, with the original completion time. Rotation copies every remaining prefix, manifest and file owner before pruning that history. Successful run retention remains unchanged.

DELETE commits a paused state before signaling local cancellation or waiting for cleanup exclusion. If a native call has not actually returned, DELETE reports `503` and asks the caller to retry cleanup. It retains the activity and native permit, and no new run starts while paused. A failed pause transaction sends no cancellation. Project deletion waits for retired cleanup. Offline restore enumerates live and retired ownership before reset, then clears outbound credentials and pauses pipelines without contacting source destinations. The original encryption key remains required while live or retired encrypted destinations exist.

## Hosted webhook provider

The hosted PostgreSQL provider stores the routing rule and encrypted destination separately. The ciphertext is bound to the tenant, project, pipeline incarnation and destination revision. Pausing stops new selection and retains queued deliveries. Replacing credentials retires jobs for the old destination. A live commit can publish a body reference or a batch fragment under its original immutable object prefix.

Workers reconcile the native cut and check durable deletion intents and creator authority before freezing the send. The send transaction holds the project coordinator session. The HTTP attempt begins after that session releases, with a durable project activity ticket and a finite attempt lease. The winning result transaction records delivery status and usage. An expired attempt can be reclaimed using the same body and receiver delivery ID.

Batch assembly reserves capacity and freezes its fragment membership before uploading a body. A deletion can recompose an unattempted batch under its existing delivery ID. When an earlier attempt froze the original membership, the surviving batch has a new delivery ID. Artifact ownership and capacity attribution follow their original immutable namespaces until authorized cleanup proves removal.

Hosted startup supplies the mutation gate and activity callbacks. The flusher, pipeline jobs, and webhook worker share one PostgreSQL pipeline effects handler. Hosted recovery reclaims eligible managed artifacts under the restore policy. Scheduled SQL-to-S3 jobs pin a reconciled snapshot before streaming. [Hosted architecture](hosted-architecture.md#query-permits-and-output-jobs)
