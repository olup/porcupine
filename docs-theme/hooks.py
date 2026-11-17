"""Publish HTML and readable Markdown from the same guide sources."""
import json
import os
from pathlib import Path
import re
from urllib.parse import urlsplit

from mkdocs.structure.files import File
from mkdocs.utils import get_relative_url

ROOT = Path(__file__).resolve().parents[1]
RELOCATED = json.loads((ROOT / "docs-theme/relocated-guides.json").read_text())
GENERATED = {"quickstart.md": "README.md", "cli.md": "cli/README.md"}
GENERATED.update({old: "docs/" + new for old, new in RELOCATED.items()})


def on_config(config):
    config.extra["release_version"] = (ROOT / "VERSION").read_text().strip()
    return config


def generated_content(target, source):
    markdown = (ROOT / source).read_text()
    if target == "quickstart.md":
        _, heading, section = markdown.partition("\n## Quickstart\n")
        if not heading:
            raise ValueError("README.md is missing its Quickstart section")
        section = section.split("\n## ", 1)[0]
        markdown = "# Quickstart\n" + section.replace("\n### ", "\n## ")
    return markdown


def on_files(files, config):
    # Old public guide URLs remain useful to installed clients and external links.
    for target, source in GENERATED.items():
        files.append(File.generated(config, target, content=generated_content(target, source)))
    return files


def on_page_markdown(markdown, page, config, files):
    return rewrite_links(markdown, page.file.src_uri)


def rewrite_links(markdown, source_uri):
    source = ROOT / GENERATED.get(source_uri, "docs/" + source_uri)

    def destination(match):
        href = match.group(1)
        parsed = urlsplit(href)
        if parsed.scheme or href.startswith(("#", "/")):
            return match.group(0)
        target = (source.parent / parsed.path).resolve()
        try:
            relative = target.relative_to(ROOT).as_posix()
        except ValueError:
            return match.group(0)
        mapped = {"README.md": "quickstart.md", "cli/README.md": "cli.md"}
        doc_target = mapped.get(relative)
        if relative.startswith("docs/") and relative.endswith(".md"):
            doc_target = relative.removeprefix("docs/")
            doc_target = RELOCATED.get(doc_target, doc_target)
        fragment = "#" + parsed.fragment if parsed.fragment else ""
        if doc_target:
            destination_uri = os.path.relpath(doc_target, str(Path(source_uri).parent))
            return "](" + destination_uri + fragment + ")"
        if target.is_file():
            return "](https://github.com/olup/porcupine/blob/main/" + relative + fragment + ")"
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
    for source in (ROOT / "docs").rglob("*.md"):
        relative = source.relative_to(ROOT / "docs")
        destination = output / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(rewrite_links(source.read_text(), relative.as_posix()))
    for name, source in GENERATED.items():
        destination = output / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(rewrite_links(generated_content(name, source), name))
    (output.parent / "README.md").write_bytes((ROOT / "README.md").read_bytes())
    (output.parent / "cli").mkdir(exist_ok=True)
    (output.parent / "cli" / "README.md").write_bytes((ROOT / "cli/README.md").read_bytes())
