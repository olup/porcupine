# Test PostHog ingestion compatibility

Porcupine supports an explicit ingestion profile, not the entire PostHog product.
The executable compatibility matrix is [compat/matrix.json](../compat/matrix.json).
Supported SDK versions are pinned in [compat/package.json](../compat/package.json)
and its lockfile. The SDK settings live in [compat/profiles.mjs](../compat/profiles.mjs).

## Run the suite

Requires the project's Go version, Node.js 24, npm, and Chromium for Playwright.
Node, npm, and Chromium are development dependencies, not server dependencies.

```sh
make compat-install
make check
make compat
```

With Docker available, run `npm --prefix compat run test:identify-clickhouse` to exercise the browser and backend SDKs through to a project-scoped SQL query against a disposable ClickHouse. Its report is `compat/artifacts/identify-clickhouse.json`.

The runner builds the real Go binary, starts it with a temporary SQLite database,
creates two projects, and loads official SDK packages. Chromium runs a local page
on a different origin from the ingestion server. No SDK events go to PostHog Cloud.
The runner rejects unexpected routes and outbound SDK traffic outside its local
servers. Data, processes, and browser contexts are cleaned up afterward.

The report is `compat/artifacts/report.json`. It records installed package versions,
profile, required case outcomes, and network evidence. It contains no raw access
keys or HTTP payloads. Reports and node_modules are ignored by Git. CI uploads the
report even when a test fails.

## What success means

Each required matrix case must execute and pass. Missing required cases fail the
suite; unsupported behavior is listed separately and does not count as coverage.
A 200 response alone is insufficient: tests read back events and verify values,
identifiers, timestamps, metadata, and isolation. Browser transport errors must
fail the browser contract even if a simple POST reached SQLite before CORS failed.

The Node retry contract forwards a real SDK request to Porcupine, replaces its
successful response with a 503, then observes the SDK resend the same UUID. The
read API must show one logical event. This proves retry deduplication for that
scenario, not exactly-once delivery under every failure.

Session assertions verify SDK identifiers and reset behavior. The separate [identified capture contract](data-and-identity.md#link-an-anonymous-browser-at-login) covers login, person-property updates, and links within a project. ClickHouse integration verifies SQL resolution for historical events.

The current browser profile disables unrelated PostHog features and chooses an
explicit transport. Headless Chromium may need the documented SDK bot-filter
override in profiles.mjs; the override is visible rather than hidden in the test.
Do not advertise compatibility with the default SDK configuration.

## Fixtures and provenance

`internal/porcupine/testdata/` contains hand-authored transport fixtures. They test
wire parsing and failure cases independently of browser startup.

`compat/fixtures/` contains normalized payloads observed from the official SDKs.
Each fixture records SDK version, profile, path, content type, encoding, and the
normalization used. These fixtures are transport evidence, not snapshots of
PostHog's server-side analytics or identity semantics. Credentials and random
identifiers must be normalized before a fixture is committed.

Fixture updates must be explicit and reviewed. Normal test runs, including latest
SDK canaries, must not silently rewrite the supported baseline. Regenerate them against the pinned dependencies with:

```sh
PORCUPINE_UPDATE_FIXTURES=1 make compat
make check
```

Review the fixture diff and run the Go replay checks before committing it.

## CI and version changes

The pull-request workflow runs Go checks and the pinned real-SDK suite. The
separate weekly or manually triggered canary installs the latest PostHog SDKs in
a disposable checkout, runs the same profile, and reports the actual versions:

```sh
make compat-canary
```

The canary intentionally modifies the local node_modules installation. Run
`npm ci --prefix compat` afterward to restore the supported baseline locally.
A passing canary does not automatically widen support. Update the pins, matrix,
fixtures, and documented profile together, then pass the required checks.

GitHub workflows are configuration until this repository is published and their
runs succeed. Local results do not imply that remote CI has run.

## Cookieless product default

The supported cookieless profile is exported as `cookielessProfile` in
`compat/profiles.mjs`. The default keeps anonymous capture free of browser storage. To link activity at login, use the opt-in identified profile with `person_profiles: "identified_only"`; it requires persistent IDs.

`browser.cookieless-no-storage` runs the real SDK in isolated Chromium contexts.
It verifies successful CORS responses and stored daily pseudonyms for repeated
events, reload, fresh same-UA context, different UA and a separate project. It
also checks empty cookies/localStorage/sessionStorage and removed IP/correlation
fields. Deterministic Go tests cover next-day rotation, restart stability,
midnight transaction ordering, clock rollback, trusted proxies and atomic failure.
See the [identity design](data-and-identity.md#cookieless-by-default).

## Remaining contracts

The matrix lists current gaps explicitly. Expand it before claiming support for
additional SDK versions, browsers, unload/beacon delivery, browser retry behavior,
new-tab session behavior, and compressed browser configurations. Merging two identified users,
general aliasing, group updates, feature flags, replay, surveys, and full default SDK
configuration are outside the supported initial profile.

Future server-side identity and analytics features need their own semantic
fixtures and, where useful, comparison against a PostHog reference installation.
This suite does not currently run a PostHog server or claim differential parity.
