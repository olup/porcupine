# Porcupine

Porcupine is a headless product analytics service. Use README.md for the quickstart and
docs/README.md for the guides to the released service. Do not describe planned APIs as shipped.
Agents connecting to a running instance should start with the
[agent guide](https://olup.github.io/porcupine/llms.txt).

- Use Go and Git. The default server embeds DuckDB/DuckLake using the official Go driver and requires CGO. Keep the standalone CLI CGO-free and its dependencies small.
- The standalone CLI defaults to the public Porcupine Cloud API at
  `https://api.getporcupine.eu`. Support self-hosted targets through `--url` or
  `PORCUPINE_URL`. Require one of these explicit settings when using
  `PORCUPINE_API_KEY`.
- DuckDB/DuckLake is the only analytical runtime. Server builds always require CGO; no backend selector or ClickHouse fallback remains. Historical comparisons are preserved on `reference/clickhouse-before-ducklake`.
- Register management operations with Huma. Set the operation ID and authorization
  metadata so HTTP and MCP share their contract. After an API change, run
  `make cli-generate`; it regenerates the offline OpenAPI snapshot and standalone CLI catalog. Commit both generated artifacts. The snapshot is never an authorization registry.
- Change a database schema only by appending a numbered step in its schema
  owner's migration registry. Core journal/control migrations belong to
  `internal/porcupine/schema_migrations.go`; native catalog and registry
  migrations belong to `internal/lake`. Never edit a released step or add DDL
  to startup code. Refuse unknown unversioned formats and future versions.
  Each owner's schema tests compare initialization with its numbered steps.
- Keep instance administration separate from project-scoped event permissions.
- Resolve project scope from credentials, never trust an ingestion payload's project ID.
- Preserve durable acceptance in standalone mode and all currently implemented journal-backed paths: acknowledge only after the bounded SQLite queue commits. The explicit hosted profile acknowledges bounded memory admission on a best-effort basis. Follow [the hosted architecture](docs/design/hosted-architecture.md). Preserve the profile-specific acknowledgment in HTTP, CLI and MCP discovery.
- Hosted target: durable state in PostgreSQL metadata and S3, without fixed project owners or persistent worker volumes. Do not stage event payloads in PostgreSQL or enable DuckLake data inlining there. Deduplication and identity/usage/output effects need a replay-safe flush protocol. Keep imports resumable and explicit mutations durable; do not infer a hard loss bound from a flush timer.
- Keep complete replaceable subsystems: provide an integrated self-hosted implementation and a documented contract for an external replacement, with the built-in component disabled when replaced. Porcupine remains the data service; the commercial control plane owns customers, external identity, payments and provisioning outside this repository and uses the same authenticated operator API. No private SaaS-only hook or restricted feature may weaken self-hosting. See [control-plane contracts](docs/design/control-plane-contracts.md) for implementation status. External authentication has a [configuration reference](docs/reference/external-auth.md): resource verification stays separate from the optional builtin authorization server, native keys work in both modes, and no external JWT creates indefinite human authority.
- Keep one-time credential secrets out of logs and source control. Store server-side
  access secrets as hashes and encrypt recoverable pipeline credentials. Public
  `pc_ci_` browser keys are the exception. Keep the raw value of an active browser
  key for authorized retrieval. Authenticate the key by hash and restrict it to
  capture for one `mixed` project.
- Keep PostHog compatibility explicit: supported payloads, SDK versions, and control
  events need contract tests. Receiving an event is not proof of identity semantics.
- Test authorization boundaries, atomic batches, stable-ID retries, and recovery with
  real temporary SQLite databases. Use `make check` (fast, skips slow journeys) while iterating; run `make check-full` before claiming the build passes. New tests call `t.Parallel()`; mark journeys over a few seconds with `skipInShort(t)`.
- Run compilation-only checks and short pure-Go module checks independently. Coordinate long native/integration suites and comparative benchmarks on the shared machine. Reserve the local execution slot only while a command is running; release it between commands and while editing or waiting for cloud CI.
- Assign each new task or implementation unit to a fresh agent with `fork_turns="none"` and a short handoff of files, constraints and test evidence. Do not reuse an existing worker for a different domain. Corrections within the current unit stay with its current owner.
- Avoid speculative abstractions, new services, a UI, or a broker without a measured need.
- Keep release labels generated from `VERSION`. Follow docs/design/releasing.md before tagging; run the release-version check and the documentation build. Feature claims still need review.
- Keep the [public SQL contract](docs/reference/analytics.md#sql-stability) additive unless a release deprecates an entry.
- SQL, MCP, identity projection and exports must enforce the same project boundaries.
- Test project lifecycle changes with two projects. Check scoped grants,
  retries, restart recovery, and cleanup of the selected analytical store and export objects.
