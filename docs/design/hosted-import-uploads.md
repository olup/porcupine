# Hosted historical uploads

The hosted profile uses the existing HTTP import operations and CGO-free CLI
with PostgreSQL and S3. Standalone and owner-routed deployments keep their
journal backend. See [hosted architecture](hosted-architecture.md).

The same project-scoped credentials, format adapters, chunk checksums and
limits apply to native, PostHog and Segment input. An administrator with current authority can resume a job under
its creator's original source. Imports
remain HTTP-only. There is no binary upload tool in MCP.

## Durable progress

PostgreSQL stores the manifest, project, creator, source, per-file offset and
generation, counts, safe samples and active parser object reference. The
immutable S3 parser object stores private tail bytes and serialized rolling SHA
state. The raw request is client-owned until acknowledged. Workers retain only
the parser checkpoint needed to continue hashing and parsing, and the native
proof binds the hash of the incoming bytes. A request interrupted before the
native commit retries at the same offset. This avoids keeping a second complete
copy of the source archive.

The project session serializes chunk writes, finalization and cancellation
across workers. Workers refresh authority and file order under that session
before accepting another chunk. A native commit stores the canonical rows
and exact outcome, including the original row IDs and count deltas. Publication
then verifies the immutable S3 checkpoint, compares the previous PostgreSQL
file generation and state, and advances the file and shared proof cursor
alongside usage in one PostgreSQL transaction. Native ACK follows confirmation
of that transaction. Every hosted recovery caller for the stream must use the
publisher with this upload checkpoint participant installed.

A status request to a cold worker can reconcile a native outcome after a lost
response or failed PostgreSQL publication. Recovery consumes the original
outcome without the creator's credentials and without classifying its rows
again. Resuming to add rows still requires fresh authority. Retrying an
acknowledged byte range requires the same offset, size and hash. EOF finalization
is a distinct transition at the same byte offset and can accept the last line
without a newline.

Once a file or job reaches its terminal state, PostgreSQL clears active parser
references. Native proofs keep the original version-pinned references for
recovery. Individual terminal jobs do not delete these objects while native
proofs and restored control can still reference them. Keep the project import
prefix in the restore inventory and include it in protected project deletion.

Hosted imported rows are queryable by the time their checkpoint is acknowledged.
A completed job therefore reports readiness with zero pending deliveries and
zero quarantined deliveries. Later live activity does not affect this status.
Historical imports emit no live pipeline output.

See [native historical import outcomes](hosted-native-imports.md) for the
native transaction and proof contract.
