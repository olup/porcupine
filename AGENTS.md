# Porcupine

Porcupine is a headless product analytics service. Read README.md for what runs
and docs/ for the target architecture. Do not describe planned APIs as shipped.

- Use Go and Git. Keep the server build CGO-free and its dependencies small.
- Keep instance administration separate from project-scoped event permissions.
- Resolve project scope from credentials, never trust an ingestion payload's project ID.
- Preserve durable acceptance: acknowledge only after the bounded SQLite queue commits.
- Store credential hashes only. Never commit or log bootstrap tokens or access secrets.
- Keep PostHog compatibility explicit: supported payloads, SDK versions, and control
  events need contract tests. Receiving an event is not proof of identity semantics.
- Test authorization boundaries, atomic batches, stable-ID retries, and recovery with
  real temporary SQLite databases. Run `make check` before claiming the build passes.
- Avoid speculative abstractions, new services, a UI, or a broker without a measured need.
- SQL, MCP, identity projection and exports must enforce the same project boundaries.
