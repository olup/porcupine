# Documentation build

Run `make docs` to build the landing page, HTML guides and browser-readable Markdown in `bin/docs-web/porcupine`. Run `make docs-preview` to serve them at <http://127.0.0.1:8787/porcupine/docs/>.

The Go command in `internal/cmd/docsite` prepares the sources and runs the standard [Hugo](https://gohugo.io/) binary. The first build downloads Hugo 0.170.0 from its official release, verifies the archive against a pinned SHA256 checksum, and caches the extracted executable in your user cache directory. Later builds work offline. On macOS, extraction uses the system `pkgutil`; it does not install a package. Linux and Windows archives are extracted directly by Go. Set `HUGO_BINARY` to an existing executable of the pinned version for an offline first build. Updating Hugo requires updating its version and archive checksums in `internal/docsite/hugo.go`.

Edit the guides in `docs/`, the sidebar in `navigation.json`, and the HTML templates in `layouts/`. Quickstart comes from the Quickstart section of the root README; the CLI guide comes from `cli/README.md`. `relocated-guides.json` preserves the public URLs of guides moved to `reference/` or `design/`. Both HTML and Markdown links use the location of the original source document, including for aliases.

The build verifies every rendered guide, local HTML link, anchor, and current release label. It publishes only the site assets, official Compose files, compatibility manifests, event schemas and examples. Local operator configuration is excluded. The preview serves `.md` as `text/plain` without a download header.

`CGO_ENABLED=0 go test ./internal/docsite ./internal/cmd/docsite` checks the preparation, release gate and preview without downloading or running Hugo. The documentation CI job also runs `make docs`, which exercises the real renderer. No Python environment or server native dependencies are required.
