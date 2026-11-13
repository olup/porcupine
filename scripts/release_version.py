"""Check current release pointers; historical version notes are intentionally excluded."""
from pathlib import Path
import argparse
import re

ROOT = Path(__file__).resolve().parents[1]


def release_version(root=ROOT):
    version = (root / "VERSION").read_text().strip()
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version):
        raise ValueError("VERSION must contain a stable version such as 0.11.0")
    return version


def check_release(root=ROOT, tag=None):
    version = release_version(root)
    if tag is not None and tag != "v" + version:
        raise ValueError(f"release tag {tag} does not match VERSION v{version}")
    checks = {
        "compose.release.yaml": r"ghcr\.io/olup/porcupine:([0-9]+\.[0-9]+\.[0-9]+)",
        "README.md": r"releases/tag/v([0-9]+\.[0-9]+\.[0-9]+)",
        "site/llms.txt": r"(?:This guide covers v|releases/tag/v|porcupine/v)([0-9]+\.[0-9]+\.[0-9]+)",
        "docs/hosting.md": r"(?:^# Host Porcupine |releases/(?:tag|download)/v|ghcr\.io/olup/porcupine:|porcupine-)([0-9]+\.[0-9]+\.[0-9]+)",
    }
    for path, pattern in checks.items():
        found = re.findall(pattern, (root / path).read_text(), re.MULTILINE)
        if not found or any(value != version for value in found):
            raise ValueError(f"{path}: current release pointers must match VERSION {version}; found {found}")
    landing = (root / "site/index.html").read_text()
    if 'releases/tag/__PORCUPINE_RELEASE_TAG__' not in landing or 'Release __PORCUPINE_RELEASE_TAG__' not in landing:
        raise ValueError("landing release link and label must use the generated version")
    if 'Porcupine v{{ config.extra.release_version }}' not in (root / "docs-theme/main.html").read_text():
        raise ValueError("documentation badge must use the generated version")
    return version


def check_built_site(output, version):
    tag = "v" + version
    landing = (output / "index.html").read_text()
    if f'releases/tag/{tag}' not in landing or f'Release {tag}</a>' not in landing:
        raise ValueError("built landing has a stale release link or label")
    for path in (output / "docs").rglob("*.html"):
        html = path.read_text()
        if f'<span>Porcupine {tag}</span>' not in html:
            raise ValueError(f"{path}: missing current documentation badge")
    for path in output.rglob("*.html"):
        if "__PORCUPINE_RELEASE_TAG__" in path.read_text():
            raise ValueError(f"{path}: unresolved release tag")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", help="Require this release tag to match VERSION")
    args = parser.parse_args()
    print(check_release(tag=args.tag))
