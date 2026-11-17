# Shared interactive execution lifecycle

> Design record for renewable permits and quarantine. See the [hosted runtime reference](../reference/shared-deployment.md#native-timeouts-and-worker-supervision) for the released behavior.

## Decision

An HTTP timeout is a cancellation request, not proof that native execution has
returned. A shared interactive scope owns its worker slot and PostgreSQL permit
until its caller returns from the lake operation. Opening the project, preparing
SQL, reading native rows, and decoding the lake result are inside that boundary.
Profile summarization, delivery-status lookups, and response JSON are outside it.
Their context retains the original request parent and execution deadline; finishing
native execution cannot cancel it. A canceled execution cannot return a successful
result even when the driver returns a nil error. Permit loss maps to retryable
`503 query_permit_uncertain`; deadline and client-cancellation mappings stay
unchanged. One keeper per service renews
all its active permits independently of the HTTP context.

The operator chose automatic permit expiry. PostgreSQL connectivity can fail
while an embedded native call remains alive. An expired permit can then be
reassigned elsewhere, exceeding physical tenant concurrency until the old call
returns or its process stops. This implementation does not claim strict physical
fencing during a network partition.

## Behavior changes

| Situation | Previous behavior | Current behavior |
| --- | --- | --- |
| Healthy query completes | Token-scoped release | Token-scoped release |
| Lake method returns before response processing | Places and timer retained through handler completion | Native tracking and places released before summary, delivery status, or JSON processing, in both modes |
| HTTP context expires but native call remains alive | Fixed permit horizon and process watchdog | Local slot retained; independent renewal continues |
| Native work exceeds deadline and both I/O allowances plus 15 seconds | Exit status 70, affecting the whole worker | Project interactive engine quarantined; process stays alive |
| Renewal fails or original permit disappears | No heartbeat | Cancel and quarantine affected project; show uncertain permit |
| Worker crashes or loses heartbeats | Fixed safety expiry | Automatic expiry 30 seconds after the latest grant or renewal |
| Worker receives normal shutdown signal | HTTP shutdown started without awaiting its completion | Drain new shared queries, cancel scopes, await the existing ten-second HTTP grace |
| `Service.Close` while a shared interactive call remains alive | Could close the control pool despite failed native close | Refuse close and preserve the execution keeper/control pool |
| Unrelated healthy project | Process exit interrupted it | It can continue while local and tenant slots remain |

The keeper uses five-second renewal ticks and two-second control-operation
bounds. Initial admission is bounded to ten seconds, leaving time for a first
renewal within the 30-second TTL. Renewal only updates unexpired, matching
worker/token rows. It cannot revive an expired row or affect its replacement.
Final release has the same token fence and is idempotent. Failed release leaves
capacity occupied until expiry.

Quarantine covers new interactive SQL, catalog, and profile operations on this
worker. It does not claim that every ingestion or background lake operation has
stopped. It persists until worker restart. Readiness becomes unavailable for the
whole worker; direct requests for healthy projects can still succeed. Status
reports worker-local execution state without starting a project-native probe.
Only instance administrators receive project identities and active/uncertain
scope counts.

Project deletion waits for release or expiry using the same policy. With a
partitioned native reader, expiry can allow cleanup before that reader returns;
the old read may fail. Physical cleanup does not gain a stronger reader-fencing
guarantee than admission.

## Verification

The execution tests use real disposable PostgreSQL processes and temporary
SQLite storage. The long blocked-call case keeps the production 30-second TTL,
cancels its request context, and waits beyond that TTL while the native-call
stand-in ignores cancellation. The permit must remain renewed and occupied.
During that wait, an unrelated project executes real DuckDB SQL successfully.
The test checks quarantine, status privacy, liveness/readiness, early-close
refusal, and release only after the blocked call returns.

A second regression covers all three production HTTP paths. Real native work
returns while a PostgreSQL table lock blocks only the subsequent delivery-status
lookup. Native active counts, local slots, and shared permits must already be
zero. Late timer callbacks and failed renewal cannot quarantine the engine, and
readiness stays healthy. Releasing the lock lets the response succeed under its
own original deadline. A nil native error after permit-loss cancellation must
still produce an error response.

Additional tests cover a killed worker recovering through real TTL expiry,
failed control renewal, expired-token replacement, batched renewal while
draining, late quarantine callbacks, repeated release, canceled admission, local
mode isolation, and a blocked logger in a subprocess. Existing real native query
cancellation, PostgreSQL metadata timeout, and S3 read cancellation tests remain
unchanged. The stand-in is an execution-lifecycle test, not a reproduced DuckDB
native hang.

## Documentation fact ledger

| Source | Destination | Treatment |
| --- | --- | --- |
| `docs/reference/shared-deployment.md`, fixed horizon and watchdog paragraphs | Same guide, native execution lifecycle section | Replaced with renewable scopes, quarantine, automatic-expiry limitation, shutdown and deletion behavior |
| `docs/reference/tenants.md`, shared query paragraph | Same section plus shared deployment link | Retained tenant/local capacity distinction; replaced obsolete exit70 policy |
| `docs/reference/instance-status.md`, response privacy and readiness | Same guide | Preserved credential privacy; documented admin-only project identities and generic readiness failure |
| `docs/reference/configuration.md`, metadata timeout paragraph | Same paragraph plus canonical lifecycle link | Preserved I/O configuration ownership; removed obsolete process-exit promise |
| `docs/reference/hosting-options.md`, reference ownership paragraph | Same paragraph | Updated canonical guide description from watchdog to renewal/quarantine |
| `docs/reference/security-and-interfaces.md`, native timeout boundary | Same section plus shared lifecycle link | Preserved cancellation limitation and local/shared distinction; stated accepted partition overlap |
| `docs/design/shared-deployment-design.md`, query admission description | Same section plus runtime-reference link | Updated current unreleased design, preserving token fencing and worker capacity |
| `docs/design/saas-and-compute-plans.md`, unreleased query bullets | Same bullets plus runtime-reference link | Replaced fixed-expiry/exit70 claims; retained no prompt-native-return guarantee in canonical reference |
| API metadata timeout/status field descriptions | Generated OpenAPI snapshot | Same runtime facts exposed through HTTP, offline CLI catalog discovery, and MCP operation documentation |

No released schema step or historical measurement is rewritten. No migration,
new executor process, or additional deployment role is introduced.
