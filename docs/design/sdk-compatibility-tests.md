# SDK compatibility tests

This guide is for contributors who run the SDK contract suite or change a supported SDK version. To configure an application, use the [PostHog capture guide](../reference/compatibility.md).

## Run the suite

Requires the project's Go version, a C/C++ toolchain for the native server, Node.js 24, npm, and Chromium for Playwright.
Node, npm, and Chromium are development dependencies, not server dependencies.

```sh
make compat-install
make check
make compat
```

Run `npm --prefix compat run test:identify` to exercise browser and backend SDK identity through project-scoped native SQL. Its report is `compat/artifacts/identify-native.json`.

The runner builds the real Go binary, starts it with a temporary SQLite database,
creates test projects, and loads the official SDK packages and the Porcupine browser build. Chromium runs a local page
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

The Porcupine browser contract does the same with a first pageview and a custom
event. It also checks for omitted browser credentials, a fresh send timestamp
on retry, and empty browser storage. To run the SDK's unit tests alone, install
its dependencies with `npm ci --prefix sdk/browser` and run
`npm test --prefix sdk/browser`.

Session assertions verify SDK identifiers and reset behavior. The separate [identified capture contract](../data-and-identity.md#link-an-anonymous-browser-at-login) covers login, person-property updates, and links within a project. Native DuckLake journeys verify SQL identity resolution for historical events.

The browser tests use fetch and a bot-filter override so headless Chromium can send events. The [application setup guide](../reference/compatibility.md) uses normal SDK bot filtering.

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

See the [pinned SDK checks](https://github.com/olup/porcupine/actions/workflows/check.yml) and [SDK canary runs](https://github.com/olup/porcupine/actions/workflows/sdk-canary.yml) for the revision tested.

## Cookieless contract

The cookieless profile is checked with real Chromium contexts and deterministic Go tests. See the [capture behavior](../reference/compatibility.md#cookieless-product-default) and [identity contract](../data-and-identity.md#cookieless-by-default).
