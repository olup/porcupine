# Capture browser exceptions and resolve stacks

This guide applies to server, CLI, and browser SDK builds that include error tracking. Check the server's `/openapi.json` for `get-error-stack` and `list-error-groups` and run `porcupine upload-source-maps --help` to check your CLI.

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

Set `--asset-base` to the public URL that corresponds to the root of `--dir`. In this example, `dist/chunks/app.js.map` matches a stack frame for `https://app.example.com/assets/chunks/app.js`. Map lookup uses the build ID and asset URL or a PostHog chunk ID.

The CLI reads Source Map v3 files up to 16 MiB and strips embedded source text before upload, including in inline sections of indexed maps. Each upload must fit within 2 MiB after stripping. Porcupine stores only mapping fields in the private control database, compressed, using SQLite or PostgreSQL depending on the deployment. Porcupine resolves stacks from uploaded maps without fetching assets or maps over the network. Project deletion removes its maps.

You can retry an upload with the same artifacts. Porcupine compares the digest of the sanitized map and returns HTTP `409` if the same selector already holds a different map. Use a new build ID for a rebuilt bundle with changed mappings.

### Upload PostHog artifacts

Use the PostHog CLI to inject chunk IDs into the build. Then upload the maps with the Porcupine CLI. [PostHog CLI injection](https://posthog.com/changelog?id=2105)

```sh
posthog-cli sourcemap inject --directory dist
porcupine upload-source-maps --id "$PROJECT_ID" --dir dist
```

Deploy the injected JavaScript. Keep the `.map` files out of your public assets. The Porcupine CLI reads a chunk ID from the map's `chunk_id` or `chunkId` field, or from a `//# chunkId=` comment in the generated bundle. Injection is a separate build step. The SDK's captured stack frames must carry those chunk IDs for lookup.

## Explore error groups

Use a credential with `events:read` on the project. List groups and pick a fingerprint. Read the group to see its distribution and occurrence IDs.

```sh
porcupine list-error-groups --id "$PROJECT_ID" --sort count
porcupine get-error-group --id "$PROJECT_ID" --fingerprint "$FINGERPRINT"
porcupine get-error-stack --id "$PROJECT_ID" --event-id "$EVENT_ID"
```

The HTTP operations are `GET /api/v1/projects/{id}/error-groups`, `GET /api/v1/projects/{id}/error-groups/{fingerprint}` and `GET /api/v1/projects/{id}/errors/{event_id}/stack`. Use the same operation IDs with `porcupine_docs` and `porcupine_call` in MCP.

Groups include the count and first and last occurrence within the requested range. Use `q` to search by error type or message. The default range is the last seven days. To pin it across pages, pass both `from` and `to` as RFC3339 timestamps, using an inclusive start and exclusive end. The range can cover up to 31 days. When `next_offset` is present, use it with the same range and filters to read another page.

The distribution lists UTC buckets, including zero-count buckets. Ranges of up to 48 hours use hourly buckets. Longer ranges use daily buckets. The first and last bucket can cover only part of their calendar interval. Counts and event IDs are decimal strings. Counts reflect delivered events with a stored error fingerprint in the requested range.

The stack response includes its `generated` positions and, where a source map resolves them, `original` positions. Builds and release labels appear when the producer supplied them. If an exception has just been captured, wait for it to appear in the group before looking up its stack.
