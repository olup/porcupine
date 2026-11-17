# Publish a release

`VERSION` is the source for the landing release link and documentation badge. The website build generates both mentions. Do not hardcode versions in `site/index.html` or `docs-theme/layouts/_default/baseof.html`.

## Prepare the release

1. Set `VERSION` to the next stable version, without the `v` prefix.
2. Update the current release links and installation examples in `README.md`, `site/llms.txt`, `docs/hosting.md`, and `compose.release.yaml`.
3. Review the feature descriptions, supported inputs, storage limitations, permissions, and migration instructions. Update the relevant guides. Version checks cannot verify these claims.
4. Run `CGO_ENABLED=0 go run ./internal/cmd/docsite check-release --tag v$(cat VERSION)`.
5. Run `CGO_ENABLED=0 go test ./internal/docsite ./internal/cmd/docsite` and `make docs`. The build uses a pinned Hugo binary, downloaded once and verified against its SHA256 checksum. It checks local links and anchors as well as release labels.
6. Add an entry headed `## X.Y.Z` to `CHANGELOG.md`. The release workflow includes that entry in the published notes.
7. Merge the reviewed changes to `main`. Wait for the complete Check workflow to pass on the exact candidate revision. Verify that the approved hosted startup and recovery evidence covers the code that will ship.
8. Create and verify the annotated tag on that revision, then push that tag alone.

```sh
release_sha=APPROVED_MAIN_COMMIT
tag="v$(git show "$release_sha:VERSION")"
git tag -a -m "Porcupine $tag" "$tag" "$release_sha"
test "$(git rev-parse "$tag^{commit}")" = "$release_sha" || exit 1
git push origin "refs/tags/$tag"
```

The release workflow rejects a tag that differs from `VERSION` or current installation pointers. It builds the documentation before building release artifacts. The build also checks the generated landing and each HTML guide for the expected version.

## Verify publication

The release workflow publishes the tested server images and CLI archives, then calls the Pages workflow with that exact tag. A Pages failure fails the release workflow even if the GitHub release has already been created. Fix and republish the site without replacing released binaries.

Pages checks that `VERSION` has a published stable GitHub release. A preparation commit for an unpublished version leaves the current site online. The release workflow publishes that version's site after its assets are available, so the landing does not advertise unfinished downloads.

If the `github-pages` environment restricts deployments, allow the `main` branch and version tags matching `v*.*.*`. Otherwise, GitHub blocks site publication from a release tag.

Confirm the release assets, the landing release link, a documentation badge, and the agent guide after publication. For a documentation-only correction, merge it to `main`; the normal Pages workflow publishes that change. To retry publication manually, run the Pages workflow from the intended branch or tag.
