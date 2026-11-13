"""Regression coverage for stale site versions and release documentation pointers."""
from pathlib import Path
from contextlib import redirect_stdout
import io
import json
import os
import re
import shutil
import tempfile
import textwrap
import unittest
from unittest import mock
import urllib.error

from release_version import ROOT, check_built_site, check_release, release_version


class ReleaseVersionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        for name in ["VERSION", "compose.release.yaml", "README.md", "site/llms.txt",
                     "docs/hosting.md", "site/index.html", "docs-theme/main.html"]:
            dest = self.root / name
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, dest)

    def test_current_release_and_tag(self):
        version = release_version(self.root)
        self.assertEqual(check_release(self.root, "v" + version), version)
        with self.assertRaisesRegex(ValueError, "does not match"):
            check_release(self.root, "v999.0.0")

    def test_rejects_stale_release_instructions(self):
        version = release_version(self.root)
        for name in ["compose.release.yaml", "README.md", "site/llms.txt", "docs/hosting.md"]:
            with self.subTest(name=name):
                path = self.root / name
                original = path.read_text()
                path.write_text(original.replace(version, "999.0.0"))
                with self.assertRaisesRegex(ValueError, "current release pointers"):
                    check_release(self.root)
                path.write_text(original)

    def test_rejects_hardcoded_badges(self):
        for name, old, new in [
            ("site/index.html", "__PORCUPINE_RELEASE_TAG__", "v0.9.0"),
            ("docs-theme/main.html", "{{ config.extra.release_version }}", "0.10.0"),
        ]:
            with self.subTest(name=name):
                path = self.root / name
                original = path.read_text()
                path.write_text(original.replace(old, new))
                with self.assertRaisesRegex(ValueError, "generated version"):
                    check_release(self.root)
                path.write_text(original)

    def test_built_html_must_match_current_version(self):
        output = self.root / "output"
        docs = output / "docs/hosting"
        docs.mkdir(parents=True)
        (output / "index.html").write_text('<a href="releases/tag/v1.2.3">Release v1.2.3</a>')
        badge = docs / "index.html"
        badge.write_text('<span>Porcupine v1.2.3</span>')
        check_built_site(output, "1.2.3")
        badge.write_text('<span>Porcupine v0.10.0</span>')
        with self.assertRaisesRegex(ValueError, "badge"):
            check_built_site(output, "1.2.3")

    def test_pages_waits_for_a_published_stable_release(self):
        workflow = (ROOT / '.github/workflows/pages.yml').read_text()
        script = textwrap.dedent(re.search(r"python3 - <<'PY'\n(.*?)\n\s+PY\n", workflow, re.S).group(1))
        version = release_version(self.root)
        for status, draft, prerelease, expected in [
            (200, False, False, 'true'), (200, True, False, 'false'),
            (200, False, True, 'false'), (404, False, False, 'false'),
            (503, False, False, None),
        ]:
            with self.subTest(status=status, draft=draft, prerelease=prerelease):
                output = self.root / 'github-output'
                if output.exists():
                    output.unlink()
                response = io.StringIO(json.dumps({'draft': draft, 'prerelease': prerelease}))
                failure = urllib.error.HTTPError('https://example.invalid', status, '', None, None)
                previous = Path.cwd()
                try:
                    os.chdir(self.root)
                    with mock.patch.dict(os.environ, {'GH_TOKEN': 'test-token', 'GITHUB_API_URL': 'https://api.github.com', 'GITHUB_REPOSITORY': 'olup/porcupine', 'GITHUB_OUTPUT': str(output)}), \
                         mock.patch('urllib.request.urlopen', return_value=response, side_effect=failure if status != 200 else None) as request, redirect_stdout(io.StringIO()):
                        if expected is None:
                            with self.assertRaisesRegex(SystemExit, 'Cannot check release publication'):
                                exec(compile(script, 'pages-release-check', 'exec'), {})
                            self.assertFalse(output.exists())
                        else:
                            exec(compile(script, 'pages-release-check', 'exec'), {})
                            self.assertEqual(output.read_text(), 'published=' + expected + '\n')
                        self.assertEqual(request.call_args.args[0].full_url, 'https://api.github.com/repos/olup/porcupine/releases/tags/v' + version)
                finally:
                    os.chdir(previous)


if __name__ == "__main__":
    unittest.main()
