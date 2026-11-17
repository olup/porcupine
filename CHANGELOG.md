# Changelog

## Unreleased

- Tenants with team administrators, project-scoped memberships, monthly ingestion allowances, and configurable interactive query concurrency, timeout, row, and byte limits.
- Separate SQLite control and ingestion databases. Optional shared PostgreSQL stores control and DuckLake catalog metadata; event payloads remain in owner-local SQLite before S3 Parquet.
- Email OTP and optional trusted Google/OIDC sign-in for invited humans, with operator identity recovery and revocation of previous interactive grants.
- Read-only MCP tools `porcupine_schema` and `porcupine_query`.
- Removed inert `use_cache` input and obsolete result-cache/scan-limit fields. `use_cache` is now rejected. This is a breaking API change.
- Versioned operational database schemas. Startup refuses newer and unknown databases before changing them.
- Faster local checks and parallel test execution, with complete race/integration suites retained for validation.
- DuckLake engine extracted into `internal/lake`. Canonical event types, live input normalization, historical parsing, webhook/row delivery, and S3 client construction have separate modules while durable transaction ownership stays in the service.
- Documentation reorganized into task guides, detailed reference, and design/validation records. The agent entry point describes the current contract rather than cumulative release history.

Tenant-enabled deployments require fresh storage. The published v0.14.0 binaries do not include these changes. Additional stabilization work is tracked in the [review follow-up](docs/design/review-v0.14.md).

## 0.14.0

- Historical native, PostHog, and Segment imports in one resumable CLI command, preserving occurrence dates and stable IDs.
- Published native producer schemas and event-input discovery through API, CLI, and MCP.
- Adaptive analytical batch reduction after typed native allocation errors.
- Complete backups to an independent S3 destination and isolated offline recovery through the standalone CLI.
- DuckDB/DuckLake as the only analytical runtime, with explicit durable acceptance/import/webhook transaction boundaries and offline CLI generation.

## 0.13.0

- Analytical S3 storage for fresh deployments.
- Event profiles with activity, recent examples, and sampled property statistics.
- Scheduled DuckLake snapshot expiration, deleted-row rewrites, and obsolete-file cleanup.
- Coordinated local backup bundles, inventory validation, retention, and isolated restore.

Earlier releases and their assets are recorded in [GitHub releases](https://github.com/olup/porcupine/releases).
