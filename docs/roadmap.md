# Scope and roadmap

## Available in v0.10.0

- **Capture.** Send events through the [native or Segment interfaces](native-and-segment-ingestion.md), or point a supported [PostHog SDK profile](compatibility.md) at Porcupine. Stable producer IDs support retries. Historical PostHog events and persons can be [imported from JSONL](posthog-import.md).
- **Analytics.** Discover event names, observed properties and the latest example UUID. Run read-only ClickHouse SQL with typed parameters. The [query reference](analytics.md) describes the supported SQL and the 60-second cache. [Identified capture](data-and-identity.md) links anonymous activity to a user. Cookieless capture estimates daily visitors.
- **Delivery.** [Export a query once](exports.md), send events to a webhook individually or in batches, or [schedule a query pipeline](pipelines.md) to publish time windows to S3.
- **Management.** Manage projects, including rename and deletion, through [HTTP](management-api.md), the [CLI](../cli/README.md) or [MCP](mcp.md). Humans can authorize tools using email verification or configured OIDC. Service accounts can use scoped credentials.
- **Operations.** Inspect the [instance status](instance-status.md) through HTTP, CLI, or MCP. The single-host deployment uses [bounded durable queues](reliability.md), optional [cold storage](storage.md), and a [backup and restore procedure](online-backup.md).

## Planned work

This backlog records the product brainstorming after v0.10.0. Unchecked items are proposals, not available commands or implementation commitments. Start with the complete import workflow and SQL feedback, then consider the query catalog and scheduled actions. Keep the single-host deployment lightweight.

### Historical imports

The existing API accepts resumable PostHog JSONL uploads. The CLI exposes its individual operations. A caller still has to prepare the manifest, calculate hashes, upload chunks, finalize files, and check analytical visibility.

- [ ] Extend the shared import job and upload protocol with an explicit format: Porcupine native, PostHog, or Segment. Keep transport, checkpoints, progress, and authorization common to all adapters.
- [ ] Define native archives for arbitrary custom events using the existing native event envelope. Require the original occurrence timestamp on imported events, with an explicit time zone. Custom properties remain application-defined; an actor is optional for events that do not represent a person.
- [ ] Preserve occurrence time separately from the actual receipt and import times. Never replace a missing historical occurrence time with the import time.
- [ ] Specify retry identity for each format. Reuse stable producer message IDs and the credential-bound logical source so a retry or overlapping import does not create another event. Decide the policy for native rows without a message ID before implementation.
- [ ] Reuse the supported PostHog events and persons models. Define the exact Segment archive shapes and supported call types, and verify them with fixtures before claiming export compatibility.
- [ ] Specify historical identity and profile behavior for each adapter, including protection of newer live state. Preserve application-defined IDs without guessing their business meaning.
- [ ] Keep imports marked as historical. Preserve the existing default that importing history does not trigger live webhook delivery or automatic receipt-window S3 pipelines.

The file format and the logical producer are separate concepts. Selecting PostHog or Segment determines how to parse the archive; it must not silently change the credential's `source_id` or project.

### Open question: connector strategy

This direction is undecided. The goal is to let people move an existing frontend or backend to Porcupine with few code changes.

- [ ] Document the native ingestion and historical import contracts precisely, with examples for authors of custom adapters or import scripts.
- [ ] Review the separation between input parsing, native event normalization, and shared ingestion and import processing. Connectors must preserve timestamps and retry identity, and use the same authorization and durable acceptance rules.
- [ ] Compare two approaches: maintain connectors for selected SDKs and export formats in the repository, or make the native contract sufficient for external adapters. A small set of maintained connectors alongside external adapters may be enough. Do not commit to a plugin framework or compatibility with every analytics SDK.
- [ ] Evaluate additional SDKs, such as Plausible, only after checking their transport, configuration options, and event semantics. Document required client changes and test each supported profile before claiming compatibility.

### A complete Go CLI import command

The proposed user experience is one command for a file or directory. This example is a design sketch and does not run in v0.10.0:

```sh
porcupine import ./exports --format posthog --project PROJECT_ID --wait
```

- [ ] Implement orchestration in the standalone Go CLI using the shared API. Users and their agents should not need Python or a custom script to import an archive.
- [ ] Handle file discovery, supported gzip inputs, validation, hashes, manifests, bounded streaming uploads, retries, and finalization automatically. Define directory ordering and the treatment of unsupported files.
- [ ] Resume after interruption from server-confirmed offsets. Detect changed source files and keep local checkpoint and credential data private.
- [ ] Show distinct upload, acceptance, and ClickHouse availability phases. Define a delivery watermark or equivalent acknowledgment before making `--wait` promise that all imported data is queryable. A queue count observed after a query is not a snapshot guarantee.
- [ ] Finish with accepted, duplicate, ignored, and invalid row counts, safe file and line diagnostics, and a usable exit status. Support checking a file before upload.
- [ ] Test interrupted uploads, lost responses, overlapping imports, changed files, format errors, project isolation, revocation, and eventual query visibility through the compiled CLI.

The existing HTTP upload protocol remains available to agents and applications that manage their own files. MCP can discover import instructions and inspect progress where supported; a hosted client cannot assume access to the user's local filesystem.

### SQL feedback and examples

- [ ] Return safe machine-readable error reasons and corrective hints. Distinguish unsupported SQL from invalid argument types, resource limits, and unavailable storage without exposing internal database details.
- [ ] Add tested SQL examples for trends, sessions, ordered funnels, retention, custom properties, and local time zones. State identity assumptions, units, and date boundaries. Agents continue to write and adapt SQL through the existing query endpoint.
- [ ] Audit common ClickHouse expressions against the supported grammar and function list. Document equivalent supported forms and prioritize compatible additions from real query failures.

The research import exercised counts, hourly grouping, custom dimensions, latency percentiles, tokens, sessions, and an ordered funnel. It exposed SQL feedback and import orchestration work, rather than a need for a new analytics query language. Application-specific identifier conventions are not product defects. Production records remain outside the repository; use synthetic fixtures for committed examples and tests.

### Reusable queries

- [ ] Add a project-scoped catalog for agent-written SQL with typed parameters, versions, and optional descriptions of output units and time zones.
- [ ] Expose create, discover, and execute operations through the shared API contract, CLI, and MCP. Execution must retain project isolation, current permissions, and query budgets.
- [ ] Let scheduled jobs reference an explicit query version and parameter set, so an edit cannot silently change an existing delivery rule.

### Scheduled query results and alerts

- [ ] Extend delivery beyond current event webhooks and scheduled SQL-to-S3 pipelines: send a scheduled SQL result to a webhook.
- [ ] Define optional result conditions, cooldowns, and repeat suppression for alerts. Reuse durable run state, retries, delivery identifiers, and scoped destination credentials.

## Later candidates

- Bounded asynchronous analytical jobs with cancellation and progress for queries that exceed interactive limits. Reuse background execution controls where practical.
- Explicit session and live-presence conventions, with documented limits for daily cookieless identities.
- Broader identity administration, including person deletion, unlinking, and merging identified users.
- Operational metrics, setup diagnostics, and measured hosting profiles.
- A configurable external queue and multiple ingestion workers if deployment needs justify them. This also requires shared control and deduplication state, not just another writer.

The [compatibility matrix](../compat/matrix.json) remains the authority for supported PostHog SDK behavior. Feature flags and session replay remain outside the current product scope. The query API uses ClickHouse SQL.
