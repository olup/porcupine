# Publish a release

`VERSION` is the source for the landing release link and documentation badge. The website build generates both mentions. Do not hardcode versions in `site/index.html` or `docs-theme/layouts/_default/baseof.html`.

## Prepare the release

1. Set `VERSION` to the next stable version, without the `v` prefix.
2. Update the current release links and installation examples in `README.md`, `site/llms.txt`, `docs/hosting.md`, and `compose.release.yaml`.
3. Review the feature descriptions, supported inputs, storage limitations, permissions, and migration instructions. Update the relevant guides. Version checks cannot verify these claims.
4. Run `CGO_ENABLED=0 go run ./internal/cmd/docsite check-release --tag v$(cat VERSION)`.
5. Run `CGO_ENABLED=0 go test ./internal/docsite ./internal/cmd/docsite` and `make docs`. The build uses a pinned Hugo binary, downloaded once and verified against its SHA256 checksum. It checks local links and anchors as well as release labels.
6. [Check and record the SQL contract](#check-and-record-the-sql-contract) against the previous stable baseline.
7. Add an entry headed `## X.Y.Z` to `CHANGELOG.md`. Include any SQL deprecations in that entry. The release workflow includes the entry in the published notes.
8. Merge the reviewed changes to `main`. Wait for the complete Check workflow to pass on the exact candidate revision. Verify that the approved hosted startup and recovery evidence covers the code that will ship.
9. Create and verify the annotated tag on that revision, then push that tag alone.

```sh
release_sha=APPROVED_MAIN_COMMIT
tag="v$(git show "$release_sha:VERSION")"
git tag -a -m "Porcupine $tag" "$tag" "$release_sha"
test "$(git rev-parse "$tag^{commit}")" = "$release_sha" || exit 1
git push origin "refs/tags/$tag"
```

The release workflow rejects a tag that differs from `VERSION` or current installation pointers. It builds the documentation before building release artifacts. The build also checks the generated landing and each HTML guide for the expected version.

### Check and record the SQL contract

1. Run `make sql-contract-generate` to refresh `internal/porcupine/testdata/sql_contract.json` from HTTP schema discovery.
2. Add each new deprecation to the current fixture's `deprecated` object. Use the key `events.<column>`, `function.<name>`, or `feature.<name>` and the stable release tag that announces the deprecation. Keep deprecation entries in the current fixture and future baselines after the affected entries are retired.
3. Run `make test PKG=./internal/porcupine RUN=TestSQLContract` and `make test PKG=./internal/lake RUN=TestSQLContract` while `releasedSQLContract` in `internal/porcupine/sql_contract_test.go` still points to the previous baseline. Removal or an incompatible change requires a release-tagged deprecation. Until a release records the first baseline, `releasedSQLContract` is empty and the additive check is skipped.
4. Once both checks pass, copy `sql_contract.json` to `sql_contract_vX.Y.Z.json` in `internal/porcupine/testdata`. Update `releasedSQLContract` to point to the new baseline. Keep previous versioned fixtures intact.

## Verify publication

The release workflow publishes the tested server images, CLI archives, and browser SDK, then calls the Pages workflow with that exact tag. A Pages failure fails the release workflow even if the GitHub release has already been created. Fix and republish the site without replacing released artifacts.

Each stable GitHub release includes `porcupine-image-digest.txt` with a single lowercase `sha256:<64 hex digits>` line. It identifies the registry image index containing the tested Linux `amd64` and `arm64` images. Use `ghcr.io/olup/porcupine@<digest>` to pull that exact release.

Check the browser SDK tarball, script, and script integrity value in the release. Their names share the `porcupine-browser-<VERSION>` prefix. `SHA256SUMS` covers all three files.

Pages checks that `VERSION` has a published stable GitHub release. A preparation commit for an unpublished version leaves the current site online. The release workflow publishes that version's site after its assets are available, so the landing does not advertise unfinished downloads.

If the `github-pages` environment restricts deployments, allow the `main` branch and version tags matching `v*.*.*`. Otherwise, GitHub blocks site publication from a release tag.

Confirm the release assets, the landing release link, a documentation badge, and the agent guide after publication. For a documentation-only correction, merge it to `main`; the normal Pages workflow publishes that change. To retry publication manually, run the Pages workflow from the intended branch or tag.
