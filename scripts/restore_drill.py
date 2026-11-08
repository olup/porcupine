#!/usr/bin/env python3
"""Prove a coordinated stopped-copy restore using disposable Docker storage only."""
from contextlib import ExitStack, closing
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import secrets
import sqlite3
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

from integration_storage import MINIO_IMAGE, start_minio, clickhouse_s3_docker_args, minio_objects
from online_backup import pause_restored_pipelines

ROOT = Path(__file__).resolve().parents[1]
CLICKHOUSE_IMAGE = "clickhouse/clickhouse-server:26.8.36.3"


def docker(*arguments, capture=False, environment=None):
    return subprocess.run(["docker", *arguments], check=True, env=environment,
                          stdout=subprocess.PIPE if capture else subprocess.DEVNULL,
                          text=capture).stdout


def remove_container(name):
    subprocess.run(["docker", "rm", "-fv", name], check=False,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def remove_network(name):
    subprocess.run(["docker", "network", "rm", name], check=False,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def host_endpoint(container, port):
    binding = docker("port", container, str(port) + "/tcp", capture=True).strip()
    return "http://127.0.0.1:" + binding.rsplit(":", 1)[1]


def wait_minio(container):
    endpoint = host_endpoint(container, 9000)
    for _ in range(150):
        try:
            with urllib.request.urlopen(endpoint + "/minio/health/live", timeout=1) as response:
                if response.status == 200:
                    return
        except (OSError, urllib.error.HTTPError):
            pass
        time.sleep(0.2)
    raise RuntimeError("restored isolated MinIO did not become ready")


def wait_clickhouse(container, environment):
    endpoint = host_endpoint(container, 8123)
    for attempt in range(300):
        request = urllib.request.Request(endpoint, data=b"SELECT 1", headers={
            "X-ClickHouse-User": environment["CLICKHOUSE_USER"],
            "X-ClickHouse-Key": environment["CLICKHOUSE_PASSWORD"]})
        try:
            with urllib.request.urlopen(request, timeout=2) as response:
                if response.read().strip() == b"1":
                    return endpoint
        except (OSError, urllib.error.HTTPError):
            pass
        if attempt % 10 == 0 and docker("inspect", "--format", "{{.State.Running}}", container, capture=True).strip() == "false":
            break
        time.sleep(0.2)
    logs = subprocess.run(["docker", "logs", "--tail", "60", container], check=False,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True).stdout
    for name in ("CLICKHOUSE_PASSWORD", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
        if environment.get(name):
            logs = logs.replace(environment[name], "[redacted]")
    raise RuntimeError("isolated ClickHouse did not become ready:\n" + logs[-6000:])


def create_clickhouse(stack, container, storage, environment):
    docker("create", "--name", container, "--label", "porcupine.restore-drill=true",
           "--memory", "2g", "--cpus", "2", "-p", "127.0.0.1::8123",
           "-e", "CLICKHOUSE_USER", "-e", "CLICKHOUSE_PASSWORD",
           "-e", "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT",
           *clickhouse_s3_docker_args(storage), CLICKHOUSE_IMAGE, environment=environment)
    stack.callback(remove_container, container)


def fixture(phase, directory, db, manifest, endpoint, environment):
    settings = {**environment, "PORCUPINE_RESTORE_DRILL": "1", "PORCUPINE_RESTORE_PHASE": phase,
                "PORCUPINE_RESTORE_ROOT": str(directory), "PORCUPINE_RESTORE_DB": str(db),
                "PORCUPINE_RESTORE_MANIFEST": str(manifest),
                "PORCUPINE_TEST_CLICKHOUSE_URL": endpoint,
                "PORCUPINE_TEST_CLICKHOUSE_USER": environment["CLICKHOUSE_USER"],
                "PORCUPINE_TEST_CLICKHOUSE_PASSWORD": environment["CLICKHOUSE_PASSWORD"]}
    subprocess.run(["go", "test", "./internal/porcupine", "-run", "^TestRestoreDrillFixture$",
                    "-count=1", "-timeout=180s", "-v"], env=settings, cwd=ROOT, check=True)


def sqlite_copy(source, destination):
    """Copy through SQLite's backup API after the fixture process has exited."""
    with closing(sqlite3.connect(source)) as original, closing(sqlite3.connect(destination)) as restored:
        original.backup(restored)
        if restored.execute("PRAGMA journal_mode=DELETE").fetchone()[0].lower() != "delete":
            raise RuntimeError("cannot make restored SQLite self-contained")
        if restored.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
            raise RuntimeError("restored SQLite integrity check failed")
        if restored.execute("PRAGMA foreign_key_check").fetchall():
            raise RuntimeError("restored SQLite foreign-key check failed")
        restored.commit()
    destination.chmod(0o600)


def run():
    # The helper owns its randomized container/network. Every remaining resource
    # is also created here and recorded before any fixture or copy can fail.
    with ExitStack() as stack:
        directory = Path(stack.enter_context(tempfile.TemporaryDirectory(prefix="porcupine-restore-")))
        storage = stack.enter_context(start_minio())
        suffix = secrets.token_hex(6)
        source_ch, restored_ch = "porcupine-restore-source-" + suffix, "porcupine-restore-target-" + suffix
        restored_minio, restored_network = "porcupine-restore-minio-" + suffix, "porcupine-restore-network-" + suffix
        environment = {**os.environ, **storage["environment"], "CLICKHOUSE_USER": "porcupine_restore_admin",
                       "CLICKHOUSE_PASSWORD": secrets.token_hex(32), "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT": "1"}
        create_clickhouse(stack, source_ch, storage, environment)
        docker("start", source_ch)
        source_endpoint = wait_clickhouse(source_ch, environment)
        original_db, checkpoint_db, restored_db = directory / "original.db", directory / "checkpoint.db", directory / "restored.db"
        manifest = directory / "fixture-private.json"
        fixture("seed", directory, original_db, manifest, source_endpoint, environment)
        if b"<Contents>" not in minio_objects(storage):
            raise RuntimeError("fixture created no native cold-storage objects")

        # No application process survives fixture(). Stop ClickHouse next so it
        # cannot change local disk metadata or remote objects during either copy.
        docker("stop", "-t", "30", source_ch)
        docker("stop", "-t", "30", storage["container"])
        sqlite_copy(original_db, checkpoint_db)
        docker("cp", source_ch + ":/var/lib/clickhouse", str(directory / "clickhouse"))
        docker("cp", storage["container"] + ":/data", str(directory / "minio"))

        docker("network", "create", "--label", "porcupine.restore-drill=true", restored_network)
        stack.callback(remove_network, restored_network)
        minio_environment = {**environment, "MINIO_ROOT_USER": storage["access_key"],
                             "MINIO_ROOT_PASSWORD": storage["secret_key"]}
        # The old S3 endpoint name remains valid inside this fresh network.
        # The stopped source storage is never started again.
        docker("create", "--name", restored_minio, "--network", restored_network,
               "--network-alias", storage["container"], "--label", "porcupine.restore-drill=true",
               "--memory", "512m", "--cpus", "1", "-p", "127.0.0.1::9000",
               "-e", "MINIO_ROOT_USER", "-e", "MINIO_ROOT_PASSWORD",
               MINIO_IMAGE, "server", "/data", "--address", ":9000", environment=minio_environment)
        stack.callback(remove_container, restored_minio)
        docker("cp", str(directory / "minio") + "/.", restored_minio + ":/data")
        docker("start", restored_minio)
        wait_minio(restored_minio)
        restored_storage = {**storage, "network": restored_network}
        create_clickhouse(stack, restored_ch, restored_storage, environment)
        docker("cp", str(directory / "clickhouse") + "/.", restored_ch + ":/var/lib/clickhouse")
        # docker cp crosses the host's UID mapping. Restore the image's data
        # ownership inside the new volume before starting the database process.
        docker("run", "--rm", "--network", "none", "--user", "0:0",
               "--volumes-from", restored_ch, "--entrypoint", "chown",
               CLICKHOUSE_IMAGE, "-R", "clickhouse:clickhouse", "/var/lib/clickhouse")
        docker("start", restored_ch)
        restored_endpoint = wait_clickhouse(restored_ch, environment)
        sqlite_copy(checkpoint_db, restored_db)
        with closing(sqlite3.connect(restored_db)) as restored:
            pause_restored_pipelines(restored)
            restored.commit()
        fixture("verify", directory, restored_db, manifest, restored_endpoint, environment)
        print(json.dumps({"status": "passed", "completed_at": datetime.now(timezone.utc).isoformat(),
                          "clickhouse_image": CLICKHOUSE_IMAGE, "minio_image": MINIO_IMAGE,
                          "method": "coordinated stopped copies into fresh containers and SQLite path",
                          "checks": ["SQLite integrity and foreign keys", "exact checkpoint logical projections",
                                     "native S3 metadata and cold-object reads", "pending original and mutation replay",
                                     "corrections and deletion tombstones", "receipt deduplication",
                                     "OAuth tokens and revoked client", "project isolation", "HTTP API and MCP"]}))


if __name__ == "__main__":
    run()
