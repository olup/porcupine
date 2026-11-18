# Import historical events

Install matching server and CLI releases before importing.

## Import a file or directory

Use an API-audience credential with `imports:write` and `events:write` on the destination project. Set `PORCUPINE_URL` and obtain a [service key](reference/service-keys.md), or use `porcupine login --admin` as an instance administrator. The default read-only CLI login does not permit imports.

Choose one format and run one command:

```sh
porcupine import --project PROJECT_ID --format native ./events.jsonl
porcupine import --project PROJECT_ID --format posthog ./posthog-export/
porcupine import --project PROJECT_ID --format segment ./segment-events.jsonl.gz
```

The command validates JSONL structure and required IDs and dates, computes checksums, creates or resumes a job, uploads bounded chunks, and finalizes the files. The server performs format conversion and semantic validation. Progress goes to stderr. The final JSON summary goes to stdout.

The default command waits up to five minutes for analytical delivery. Its final status distinguishes durable acceptance from data already available to SQL. To allow more time, add `--wait-timeout 20m`. To stop after acceptance, add `--no-wait`; a completed upload alone does not confirm queryability.

An agent with local file access can run this command directly. No import script, hand-written manifest, or manually split upload chunks are required.

## Choose the format

| Format | One JSONL record |
| --- | --- |
| `native` | A Porcupine `track`, `pageview`, or `identify` message with `message_id` and the original `timestamp`. [Native contract](reference/native-event-contract.md) |
| `posthog` | One exported event with `uuid`, `event`, `distinct_id`, and `timestamp`. Persons files use PostHog's separate persons model. [Export formats](reference/posthog-import-format.md) |
| `segment` | One Segment `track`, `page`, or `identify` message with `messageId` and the original `timestamp`. HTTP batch envelopes and destination-specific warehouse transformations are not file records. |

Every event needs its original occurrence time and a stable producer ID. The import does not substitute today's time or generate a new ID on each run. The same authenticated logical source and producer ID deduplicate with live capture. Unbound native, Segment, and PostHog sources are separate namespaces. [Retry identity](reference/native-event-contract.md#ids-and-retry-semantics)

For custom data, convert each source row to the native format. A file can contain a backend event with no user:

```json
{"type":"track","message_id":"stock-movement-813","timestamp":"2020-01-02T03:04:05.123456789Z","name":"stock_updated","properties":{"product_id":"sku-42","quantity":3}}
```

Put business values in `properties` and capture metadata in `context`. If the source has no ID, generate one once and save it in the converted file. Do not regenerate IDs for retries or deduplicate repeated occurrences by hashing only their payload. CSV and JSON arrays need conversion to one JSON object per line before import.

## Arrange the files

A directory is scanned recursively for `.jsonl`, `.ndjson`, or `.json` files, optionally followed by `.gz`. Each selected file must contain JSONL, even when its extension is `.json`. Other extensions are skipped. Symbolic links and non-regular input files are rejected. Imports contain at most 4,096 files and each row can occupy at most 1 MiB.

Gzip is decompressed as a stream. Checksums and upload offsets describe the uncompressed bytes. The CLI does not load the whole file into memory or create a second copy of the dataset.

For PostHog exports, use an `events/` folder and a `persons/` folder. The server processes persons files first. Automatic detection also recognizes filenames starting with `persons.` or `persons-`. For differently named files, pass `--kind persons` or `--kind events` explicitly. A native or Segment job accepts only the events kind.

Keep the input files unchanged until the import finishes. Resume verifies the format, file labels, sizes, and checksums against the stored manifest. The server can retain records already accepted before a later file error or final checksum failure.

## Resume after interruption

Rerun the same command with the same files and credential. The stable manifest identifies the existing server job. The CLI resumes from its committed byte offsets, including when the last response was lost. It can stream past the acknowledged offset in a gzip file.

You can also select the job explicitly:

```sh
porcupine import --project PROJECT_ID --format native --resume IMPORT_ID ./events.jsonl
porcupine get-import --id PROJECT_ID --import-id IMPORT_ID
```

Uploading, completing, or cancelling an existing job requires its creator or an instance administrator. A revoked credential cannot resume. If you change credentials, an administrator can resume the original job with `--resume`; a different non-admin creator cannot take over that job.

Network errors and temporary server pressure trigger bounded retries. Redirects are rejected. If retries are exhausted, the command prints the import ID and exits with an error. Repeating the command preserves already acknowledged progress. Concurrent writers can cause an offset conflict; rerun after the other writer stops.

In the development server, each chunk commit is bounded to 30 seconds, or the caller's earlier deadline. A shared worker validates its ingestion policy before the atomic commit. Crossing the policy refresh interval during that commit does not invalidate already processed rows. Every new chunk checks policy again, and a failed commit preserves the previous checkpoint for retry.

## Check the result

The summary contains accepted and duplicate counts, invalid-row counts, and bounded diagnostic samples with file IDs and line numbers. It does not print event contents. CLI preflight errors identify the filename, line, and missing field.

An invalid row discovered by the server makes the CLI exit with an error after the upload summary. Correct the source and rerun. The changed file creates a new manifest; already accepted events deduplicate when their producer IDs and logical source remain unchanged. Accepted records are not rolled back automatically.

Historical identify messages can contribute explicit identity evidence. Their incremental traits are never replayed onto current profiles. PostHog persons files provide the supported profile snapshot format. Native and Segment do not yet have a separate person snapshot encoding. Inspect ignored and unresolved counts before relying on reconstructed identity.

`delivery.ready: true` means the upload completed, an analytical backend is configured, and all project events through the import's acceptance cursor have been delivered. Earlier pending work can delay this check. Later live traffic does not. Quarantined events produce an explicit failure instead of a false success. This confirms delivery, not that a future SQL call can never encounter a storage outage.

Imported events retain their original timestamps and are marked as history. They appear in ordinary analytics and on-demand exports. They do not trigger individual or batch webhooks, and automatic receipt-window S3 queries exclude them. Identity, corrections, and the project's normal privacy rules still apply.

For clients implementing their own uploader, see the [HTTP upload protocol](reference/posthog-import.md). The API remains the source of operation contracts used by the CLI.
