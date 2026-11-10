"""Build the release archives for one OS, then run the native binary if available."""

import argparse
import os
from pathlib import Path
import re
import subprocess
import tarfile
import tempfile
import zipfile

CLI = Path(__file__).resolve().parents[1] / "cli"


def package(target, version, output):
    if not re.fullmatch(r"(?:v[0-9]+\.[0-9]+\.[0-9]+|dev)", version):
        raise ValueError("expected a vMAJOR.MINOR.PATCH tag or dev")
    host_os, host_arch = subprocess.check_output(
        ["go", "env", "GOHOSTOS", "GOHOSTARCH"], cwd=CLI, text=True
    ).split()
    output.mkdir(parents=True, exist_ok=True)
    for arch in ("amd64", "arm64"):
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / ("porcupine.exe" if target == "windows" else "porcupine")
            env = dict(os.environ, GOOS=target, GOARCH=arch, CGO_ENABLED="0")
            subprocess.run(["go", "build", "-trimpath", "-buildvcs=false", "-ldflags",
                            f"-s -w -X main.version={version}", "-o", binary, "./cmd/porcupine"],
                           cwd=CLI, env=env, check=True)
            name = f"porcupine-cli-{version}-{target}-{arch}"
            if target == "windows":
                archive = output / (name + ".zip")
                with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as z:
                    z.write(binary, binary.name)
                binary.unlink()
                with zipfile.ZipFile(archive) as z:
                    z.extract(binary.name, directory)
            else:
                archive = output / (name + ".tar.gz")
                with tarfile.open(archive, "w:gz") as t:
                    t.add(binary, arcname=binary.name)
                binary.unlink()
                with tarfile.open(archive) as t:
                    member = t.getmember(binary.name)
                    binary.write_bytes(t.extractfile(member).read())
                    binary.chmod(member.mode & 0o777)
            if (target, arch) == (host_os, host_arch):
                reported = subprocess.check_output([binary, "--version"], text=True).strip()
                if reported != f"porcupine version {version}":
                    raise ValueError(f"unexpected version in {archive.name}: {reported}")
                subprocess.run([binary, "list-projects", "--help"], check=True,
                               stdout=subprocess.DEVNULL)
            print(archive)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("target", choices=["darwin", "linux", "windows"])
    parser.add_argument("version")
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    package(args.target, args.version, args.output)
