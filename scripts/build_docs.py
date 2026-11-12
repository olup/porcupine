"""Build the landing page, HTML guides and Markdown into bin/docs-web."""
from pathlib import Path
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "bin/docs-web/porcupine"

def main():
    subprocess.run([sys.executable, "-m", "mkdocs", "build", "--strict", "--clean"], cwd=ROOT, check=True)
    shutil.copytree(ROOT / "site", OUTPUT, dirs_exist_ok=True)
    # Source links in raw Markdown retain their repository-relative locations.
    for relative in [
        "compat/matrix.json", "compat/profiles.mjs",
        "deployment/clickhouse/storage.xml", "deployment/.env.s3.example",
        "compose.yaml", "compose.oauth.yaml", "compose.s3.yaml", "compose.exports.yaml",
    ]:
        source = ROOT / relative
        destination = OUTPUT / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)

if __name__ == "__main__":
    main()
