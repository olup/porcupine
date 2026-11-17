"""Check nested guide links and the raw Markdown served to agents."""
import importlib.util
from functools import partial
from http.server import ThreadingHTTPServer
import threading
import urllib.request
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("docs_hooks", ROOT / "docs-theme/hooks.py")
hooks = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(hooks)
PREVIEW_SPEC = importlib.util.spec_from_file_location("docs_preview", ROOT / "scripts/serve_docs.py")
preview = importlib.util.module_from_spec(PREVIEW_SPEC)
PREVIEW_SPEC.loader.exec_module(preview)


class DocsHooksTests(unittest.TestCase):
    def test_nested_links_stay_relative_to_the_page(self):
        self.assertEqual(
            hooks.rewrite_links("[Start](../README.md) [SQL](analytics.md)", "reference/mcp.md"),
            "[Start](../README.md) [SQL](analytics.md)",
        )

    def test_old_public_alias_uses_the_canonical_source_directory(self):
        self.assertEqual(
            hooks.rewrite_links("[Start](../README.md) [SQL](analytics.md)", "mcp.md"),
            "[Start](README.md) [SQL](reference/analytics.md)",
        )

    def test_quickstart_links_use_the_generated_document(self):
        self.assertEqual(
            hooks.rewrite_links("[Host](docs/hosting.md) [CLI](cli/README.md)", "quickstart.md"),
            "[Host](hosting.md) [CLI](cli.md)",
        )

    def test_nested_markdown_opens_as_text_in_the_preview(self):
        with tempfile.TemporaryDirectory() as directory:
            markdown = Path(directory) / "reference/contract.md"
            markdown.parent.mkdir()
            markdown.write_text("# Native contract\n")
            handler = partial(preview.PreviewHandler, directory=directory)
            with ThreadingHTTPServer(("127.0.0.1", 0), handler) as server:
                worker = threading.Thread(target=server.serve_forever, daemon=True)
                worker.start()
                try:
                    url = "http://127.0.0.1:%s/reference/contract.md" % server.server_port
                    with urllib.request.urlopen(url, timeout=5) as response:
                        self.assertEqual(response.headers.get_content_type(), "text/plain")
                        self.assertIsNone(response.headers.get("Content-Disposition"))
                        self.assertEqual(response.read().decode(), "# Native contract\n")
                finally:
                    server.shutdown()
                    worker.join(timeout=5)

    def test_build_copies_nested_markdown_and_public_aliases(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "docs"
            output.mkdir()
            hooks.on_post_build(SimpleNamespace(site_dir=str(output)))
            self.assertTrue((output / "reference/mcp.md").is_file())
            self.assertTrue((output / "design/ducklake-validation.md").is_file())
            self.assertIn("#", (output / "native-event-contract.md").read_text())
            self.assertIn("reference/analytics.md", (output / "mcp.md").read_text())


if __name__ == "__main__":
    unittest.main()
