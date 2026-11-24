# Capture browser exceptions and resolve stacks

This guide applies to server, CLI, and browser SDK builds that include error tracking. Check the server's `/openapi.json` for `get-error-stack` and run `porcupine upload-source-maps --help` to check your CLI.

## Capture exceptions

Use the public browser key for a project with mixed ingestion enabled. During your application build, set `build_id` to an ID for the generated assets, such as the Git SHA when each commit produces one build. Use that same ID when uploading source maps.

```js
import { init } from '@porcupine/browser'

const analytics = init('pc_ci_YOUR_KEY', {
  api_host: 'https://analytics.example.com',
  build_id: 'YOUR_BUILD_ID',
  capture_exceptions: true,
})
```

`capture_exceptions: true` captures uncaught browser errors and unhandled promise rejections. To report only errors your application handles, omit that option and call `analytics.captureException(error)` in your error handler. Pass an optional `release` label to group builds under a human-readable version. Exception reports include the error message and available stack frames. Avoid putting secrets in error messages.

## Upload source maps in CI

Create an [API service key](reference/service-keys.md#create-a-team-service-account) for the same project. Set both the service account and the key to `"permissions":["sourcemaps:write"]` and `"projects":["PROJECT_ID"]`. Use `"audience":"api"` for the key. The browser key is scoped to event capture.

Store the service key as a CI secret named `PORCUPINE_API_KEY`. Set `PORCUPINE_URL` to the instance origin or supply `--url` when using that environment key. Generate source maps with your app's build, then upload them before you deploy.

```sh
export PORCUPINE_URL=https://analytics.example.com
porcupine upload-source-maps --id "$PROJECT_ID" --dir dist \
  --build "$GIT_SHA" --asset-base https://app.example.com/assets/
```

Set `--asset-base` to the public URL that corresponds to the root of `--dir`. In this example, `dist/chunks/app.js.map` matches a stack frame for `https://app.example.com/assets/chunks/app.js`. If the build reports a release label, you can pass it with `--release`. Map lookup uses the build ID and asset URL or a PostHog chunk ID.

Porcupine accepts Source Map v3 JSON up to 2 MiB per map, including indexed maps with inline sections. It drops `sourcesContent` before storing compressed maps in the private control database, which uses SQLite or PostgreSQL depending on the deployment. Porcupine resolves stacks from uploaded maps without fetching assets or maps over the network. Project deletion removes its maps.

You can retry an upload with the same artifacts. Porcupine compares the digest of the sanitized map and returns HTTP `409` if the same selector already holds a different map. Use a new build ID for a rebuilt bundle with changed mappings.

### Upload PostHog artifacts

Run PostHog-compatible chunk ID injection as part of your build and deploy the injected JavaScript. Then omit `--build` and `--asset-base` when uploading:

```sh
porcupine upload-source-maps --id "$PROJECT_ID" --dir dist
```

The CLI reads a chunk ID from the map's `chunk_id` or `chunkId` field, or from a `//# chunkId=` comment in the generated bundle. Injection is a separate build step. The SDK's captured stack frames must carry those chunk IDs for lookup.

## Find an exception and read its stack

Use a credential with `events:read` on the project. Run this SQL against delivered events to find recent exceptions. The fingerprint groups similar exceptions.

```sql
SELECT id, event_time,
       json_extract_string(properties, '$."$exception_fingerprint"') AS fingerprint
FROM events
WHERE event = '$exception'
ORDER BY event_time DESC
LIMIT 20
```

Take the decimal `id` from a result row. See [Run an interactive query](query-events.md#run-an-interactive-query) for CLI and MCP query instructions.

```sh
porcupine get-error-stack --id "$PROJECT_ID" --event-id "$EVENT_ID"
```

The HTTP operation is `GET /api/v1/projects/{id}/errors/{event_id}/stack`. Each frame includes its `generated` position and, when a map resolves it, an `original` position. A missing map leaves the generated frame available. If an exception has just been captured, wait for it to appear in SQL before looking up its stack.
