# Durable webhook module

> Historical record of the initial webhook extraction. The refactor moved durable webhook jobs into `internal/delivery` before the release of v0.15.0. The later [pipeline jobs extraction](pipeline-jobs.md#ownership-moved) moved those jobs to `internal/output`. [Current architecture](../architecture.md)

## Ownership

The event acceptance owner opens the journal transaction. `WebhookEnqueuer.Enqueue` adds planned outbound work through that transaction and cannot open or commit another transaction. Event mutation similarly calls `CancelWebhookEvent` through its existing transaction. A rollback therefore removes both the event change and its webhook effects.

`WebhookQueue` owns attempt selection, conditional acknowledgment, retry cooldowns, failed-payload retention, metadata pruning and recovery of interrupted sends. Its `Step` method obtains a project activity lease, decrypts the destination, rechecks current creator authorization and pipeline state, then sends through the existing bounded HTTPS transport. Canceling an HTTP send requeues its attempt before releasing the project lease. Successful delivery clears the payload; temporary failures keep the delivery ID for receiver-side deduplication.

The module's host contract has three capabilities: authorize the current creator for this project, open the encrypted destination, and begin a project activity lease. The concrete adapter stays in `internal/porcupine`. The module does not import the server or receive its `Service` type. API routing, definition management, public DTOs, scoped run-history queries, shared project ownership and cross-control authorization remain in the service. S3 pipelines and exports are separate future extraction candidates.

The core pipeline loop still starts and stops its webhook and S3 tasks together. This change moves the webhook step and journal state machine; it does not introduce another worker process or a generic job framework.

Webhook limits are a copy of the normalized startup configuration. Live configuration replacement is not supported. The capacity test reconfigures its attached test runtime before accepting events; this is fixture setup, not a production reload mechanism. The enqueuer retains only those limits and the shared clock, with no database, transport or host handle.

## Source to destination

| Responsibility or fact | Source | Destination |
| --- | --- | --- |
| Transaction-bound destination selection and enqueueing | `internal/porcupine/pipeline_webhook.go`, `webhookAcceptance` | `internal/delivery/webhook_enqueue.go`, `WebhookEnqueuer` |
| Batch payloads, byte/event limits and member identity | `internal/porcupine/pipeline_webhook_batch.go` | `internal/delivery/webhook_batch.go` |
| Event deletion and surviving batch-member reconstruction | `Service.cancelWebhookEvent` | `delivery.CancelWebhookEvent`, using the caller's transaction |
| Claim, send, conditional finish, cooldown, manual retry and history pruning | `Service` methods in `pipeline_webhook.go` | `internal/delivery/webhook_queue.go`, `WebhookQueue` |
| Interrupted-send recovery and accounting initialization | `Service.initializeWebhook` | `delivery.InitializeWebhooks` |
| Current authorization, destination decryption and deletion lease | Direct calls inside `Service.stepWebhook` | Explicit `WebhookHost` calls with a core adapter |
| Module responsibility summary | `docs/architecture.md`, boundaries table and transaction paragraph | Updated in place, linked to this ownership record |

No guide was deleted or merged. The table records code ownership changes and the one updated architecture fact, rather than treating link validation as proof that documentation retained every fact.

## Structural measurements

The comparison starts from `a34027a`, the merge of PR #51. Production methods declared on `*Service` fall from **304 to 297**. Seven webhook methods now belong to the delivery module. This is an ownership measurement, not an execution-speed claim.

The public operation contracts remain checked by `make api-check`. Existing real SQLite webhook, authorization, HTTP/MCP batch, mutation and recovery tests continue to exercise the same scenarios through the new owner. New module tests cover transaction rollback, revocation between claim and send, project-scoped retry, cooldown, cancellation, interrupted-send recovery and acknowledgment rollback.

Verification results are recorded in the PR after the focused and complete checks finish. No runtime performance improvement is claimed for moving code between packages.
