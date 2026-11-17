# Deliver events and scheduled reports

A pipeline sends project data to another system. Use a webhook for accepted events or an S3 query pipeline for recurring reports. For a single download, use an [export](exports.md).

The operator must configure a stable pipeline encryption key and permitted destinations. Your credential needs both `pipelines:write` and `events:read` on the project. Destination credentials are write-only inputs; keep them out of prompts and source control.

## Create a pipeline

Ask an authorized agent to create the pipeline for your project, showing the event filter or SQL and destination before creation. It uses the same management operations as HTTP and the CLI.

For the CLI, save the request in a private `pipeline.json` file:

```sh
porcupine create-pipeline --id PROJECT_ID --body @pipeline.json
porcupine list-pipelines --id PROJECT_ID
```

The [pipeline reference](reference/pipeline-api.md) contains complete request bodies, destination restrictions, schedules, retries, and limits.

## Choose what gets delivered

**Event webhooks** send newly accepted original events, individually or in time-based batches. They do not backfill history or send later corrections. Private identify controls are excluded.

**Scheduled S3 queries** run project-scoped SQL over closed receipt-time windows and publish their results in a destination bucket. The destination is separate from Porcupine's analytical storage and backups. Use the recorded receipt time for incremental delivery; occurrence time can be historical.

## Watch and stop delivery

Inspect the pipeline and its runs through API, CLI, or MCP. List responses contain metadata; fetch the individual pipeline to inspect its nonsecret rule.

```sh
porcupine get-pipeline --id PROJECT_ID --pipeline-id PIPELINE_ID
porcupine list-pipeline-runs --id PROJECT_ID --pipeline-id PIPELINE_ID
porcupine pause-pipeline --id PROJECT_ID --pipeline-id PIPELINE_ID
```

Pause before replacing destination credentials or investigating failures. SQL, filters, and schedules are immutable; delete and recreate the pipeline to change them. Credential rotation has its own operation. Replacement retires pending work for the old destination, including an account change at the same URL. New events use the replacement. Unavailable cleanup keeps the old encrypted S3 identity and can delay project deletion.

Deleting a project cancels pending local delivery. It cannot remove copies already received by a webhook or external bucket. Retain the encryption key with the instance's private recovery material.
