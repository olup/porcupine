#!/usr/bin/env python3
"""Test real ClickHouse, optionally with native S3 storage; never prints credentials."""
import argparse
from contextlib import ExitStack
import os
from pathlib import Path
import secrets
import subprocess
import time
import urllib.error
import urllib.request
from integration_storage import start_minio, clickhouse_s3_docker_args, minio_objects

IMAGE = "clickhouse/clickhouse-server:26.8.36.3"
ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument("--s3", action="store_true", help="Exercise native cold storage and exports on isolated MinIO")
arguments = parser.parse_args()
with ExitStack() as stack:
    storage = stack.enter_context(start_minio()) if arguments.s3 else None
    container = "porcupine-integration-" + secrets.token_hex(6)
    password = secrets.token_hex(32)
    environment = {**os.environ, "CLICKHOUSE_USER": "porcupine_admin",
                   "CLICKHOUSE_PASSWORD": password, "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT": "1"}
    extra = []
    if storage:
        environment.update(storage["environment"])
        environment["PORCUPINE_TEST_STORAGE_POLICY"] = "hot_cold"
        environment.update({"PORCUPINE_TEST_S3_ENDPOINT": storage["host_endpoint"],
                            "PORCUPINE_TEST_S3_BUCKET": storage["bucket"],
                            "PORCUPINE_TEST_S3_REGION": "us-east-1",
                            "PORCUPINE_TEST_S3_ACCESS_KEY_ID": storage["access_key"],
                            "PORCUPINE_TEST_S3_SECRET_ACCESS_KEY": storage["secret_key"],
                            "PORCUPINE_TEST_S3_SESSION_TOKEN": ""})
        extra = clickhouse_s3_docker_args(storage)
    else:
        environment.pop("PORCUPINE_TEST_STORAGE_POLICY", None)
        for name in ("ENDPOINT", "BUCKET", "REGION", "ACCESS_KEY_ID", "SECRET_ACCESS_KEY", "SESSION_TOKEN"):
            environment.pop("PORCUPINE_TEST_S3_" + name, None)
    created = False
    try:
        subprocess.run(["docker", "run", "-d", "--name", container,
                        "--label", "porcupine.integration=true", "--memory", "2g", "--cpus", "2",
                        "-p", "127.0.0.1::8123", "-e", "CLICKHOUSE_USER", "-e", "CLICKHOUSE_PASSWORD",
                        "-e", "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", *extra, IMAGE],
                       env=environment, check=True, stdout=subprocess.DEVNULL)
        created = True
        port = subprocess.check_output(["docker", "port", container, "8123/tcp"], text=True).strip().rsplit(":", 1)[1]
        endpoint = "http://127.0.0.1:" + port
        for attempt in range(300):
            try:
                req = urllib.request.Request(endpoint, data=b"SELECT 1", headers={
                    "X-ClickHouse-User": environment["CLICKHOUSE_USER"], "X-ClickHouse-Key": password})
                with urllib.request.urlopen(req, timeout=2) as response:
                    if response.read().strip() == b"1":
                        break
            except (OSError, urllib.error.HTTPError):
                pass
            time.sleep(0.2)
        else:
            raise RuntimeError("ClickHouse did not become ready")
        environment.update({"PORCUPINE_TEST_CLICKHOUSE_URL": endpoint,
                            "PORCUPINE_TEST_CLICKHOUSE_USER": environment["CLICKHOUSE_USER"],
                            "PORCUPINE_TEST_CLICKHOUSE_PASSWORD": password})
        if storage:
            subprocess.run(["go", "test", "-race", "./internal/exportstore", "-run", "TestMinIOIntegration",
                            "-count=1", "-timeout=60s", "-v"], env=environment, cwd=ROOT, check=True)
        # The S3 export fixture can spend 90 seconds within its own deadline.
        # Give it a separate invocation so it cannot exhaust the other fixtures' budget.
        package_tests = ["go", "test", "-race", "./internal/porcupine", "-count=1", "-timeout=120s", "-v"]
        if storage:
            subprocess.run(package_tests + ["-run", "^TestClickHouseIntegrationExports$"],
                           env=environment, cwd=ROOT, check=True)
        subprocess.run(package_tests + ["-run", "^TestClickHouseIntegration", "-skip", "^TestClickHouseIntegrationExports$"],
                       env=environment, cwd=ROOT, check=True)
        if storage and b"<Contents>" not in minio_objects(storage):
            raise RuntimeError("Cold storage tests created no S3 objects")
    finally:
        if created:
            subprocess.run(["docker", "rm", "-fv", container], check=True, stdout=subprocess.DEVNULL)
