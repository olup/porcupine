# Publish a release

`VERSION` is the source for the landing release link and documentation badge. The website build generates both mentions. Do not hardcode versions in `site/index.html` or `docs-theme/main.html`.

## Prepare the release

1. Set `VERSION` to the next stable version, without the `v` prefix.
2. Update the current release links and installation examples in `README.md`, `site/llms.txt`, `docs/hosting.md`, and `compose.release.yaml`.
3. Review the feature descriptions, supported inputs, storage limitations, permissions, and migration instructions. Update the relevant guides. Version checks cannot verify these claims.
4. Run `python3 scripts/release_version.py --tag v$(cat VERSION)`.
5. Install `docs-requirements.txt` in your documentation environment. Run `python3 -m unittest discover -s scripts -p 'test_release_version.py'` and `make docs` there.
6. Merge the reviewed changes and push the matching `vX.Y.Z` tag.

The release workflow rejects a tag that differs from `VERSION` or current installation pointers. It builds the documentation before building release artifacts. The build also checks the generated landing and each HTML guide for the expected version.

## Verify publication

The release workflow publishes the tested server images and CLI archives, then calls the Pages workflow with that exact tag. A Pages failure fails the release workflow even if the GitHub release has already been created. Fix and republish the site without replacing released binaries.

If the `github-pages` environment restricts deployments, allow the `main` branch and version tags matching `v*.*.*`. Otherwise, GitHub blocks site publication from a release tag.

Confirm the release assets, the landing release link, a documentation badge, and the agent guide after publication. For a documentation-only correction, merge it to `main`; the normal Pages workflow publishes that change. To retry publication manually, run the Pages workflow from the intended branch or tag.
