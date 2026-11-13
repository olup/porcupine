# Porcupine

Porcupine is a headless product analytics service. Use README.md for the quickstart and
docs/README.md for the guides to the released service. Do not describe planned APIs as shipped.
Agents connecting to a running instance should start with the
[agent guide](https://olup.github.io/porcupine/llms.txt).

- Use Go and Git. Keep the server and standalone CLI builds CGO-free and their dependencies small.
- The `experiment/ducklake` branch has an opt-in `ducklake` native build tag using the official DuckDB driver. This experimental server build requires CGO. Keep normal server and standalone CLI builds CGO-free, and do not describe the experiment as released or deploy it automatically.
- Register management operations with Huma. Set the operation ID and authorization
  metadata so HTTP and MCP share their contract. After an API change, run
  `make cli-generate` and commit the generated standalone CLI catalog.
- Keep instance administration separate from project-scoped event permissions.
- Resolve project scope from credentials, never trust an ingestion payload's project ID.
- Preserve durable acceptance: acknowledge only after the bounded SQLite queue commits.
- Keep one-time credential secrets out of logs and source control. Store server-side
  access secrets as hashes and encrypt recoverable pipeline credentials.
- Keep PostHog compatibility explicit: supported payloads, SDK versions, and control
  events need contract tests. Receiving an event is not proof of identity semantics.
- Test authorization boundaries, atomic batches, stable-ID retries, and recovery with
  real temporary SQLite databases. Run `make check` before claiming the build passes.
- Avoid speculative abstractions, new services, a UI, or a broker without a measured need.
- SQL, MCP, identity projection and exports must enforce the same project boundaries.
- Test project lifecycle changes with two projects. Check scoped grants,
  retries, restart recovery, and cleanup of managed ClickHouse data and export objects.
