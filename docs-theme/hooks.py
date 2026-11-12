"""Publish each guide as HTML and its original Markdown from one source."""
from pathlib import Path
import re
from urllib.parse import urlsplit

from mkdocs.structure.files import File
from mkdocs.utils import get_relative_url

ROOT = Path(__file__).resolve().parents[1]
GENERATED = {"quickstart.md": "README.md", "cli.md": "cli/README.md"}


def on_files(files, config):
    for target, source in GENERATED.items():
        files.append(File.generated(config, target, content=(ROOT / source).read_text()))
    return files


def on_page_markdown(markdown, page, config, files):
    return rewrite_links(markdown, page.file.src_uri)


def rewrite_links(markdown, source_uri):
    source_dir = ROOT / GENERATED.get(source_uri, "docs/" + source_uri)
    def destination(match):
        href = match.group(1)
        parsed = urlsplit(href)
        if parsed.scheme or href.startswith("#"):
            return match.group(0)
        target = (source_dir.parent / parsed.path).resolve()
        relative = target.relative_to(ROOT).as_posix()
        mapped = {"README.md": "quickstart.md", "cli/README.md": "cli.md"}
        if relative in mapped:
            return "](" + mapped[relative] + ("#" + parsed.fragment if parsed.fragment else "") + ")"
        if relative.startswith("docs/") and relative.endswith(".md"):
            return "](" + relative.removeprefix("docs/") + ("#" + parsed.fragment if parsed.fragment else "") + ")"
        if target.is_file():
            return "](https://github.com/olup/porcupine/blob/main/" + relative + ("#" + parsed.fragment if parsed.fragment else "") + ")"
        return match.group(0)
    markdown = re.sub(r"\]\(([^)\s]+)\)", destination, markdown)
    if source_uri == "quickstart.md":
        markdown = markdown.replace('src="site/assets/', 'src="/porcupine/assets/')
    return markdown


def on_page_context(context, page, config, nav):
    context["raw_url"] = get_relative_url(page.file.src_uri, page.url)
    return context


def on_post_build(config):
    output = Path(config.site_dir)
    for source in (ROOT / "docs").glob("*.md"):
        (output / source.name).write_bytes(source.read_bytes())
    for name, source in GENERATED.items():
        (output / name).write_text(rewrite_links((ROOT / source).read_text(), name))
    # Preserve relative links in raw Markdown as well.
    (output.parent / "README.md").write_bytes((ROOT / "README.md").read_bytes())
    (output.parent / "cli").mkdir(exist_ok=True)
    (output.parent / "cli" / "README.md").write_bytes((ROOT / "cli/README.md").read_bytes())
