#!/usr/bin/env python3
"""Prove restored permanent-ledger ingestion still starts with ClickHouse offline."""
from contextlib import ExitStack
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import tempfile

from integration_storage import start_minio
from online_restore_drill import (ROOT, DATABASE, DrillFailure, api, app_start, ch, check,
                                  create_ch, docker, sqlite_rows, token, tool, wait_until)


def run():
    with ExitStack() as stack:
        directory = Path(stack.enter_context(tempfile.TemporaryDirectory(prefix="pc-default-restore-"))).resolve()
        storage = stack.enter_context(start_minio())
        environment = {**os.environ, **storage["environment"], "CLICKHOUSE_USER": "permanent_restore_admin",
                       "CLICKHOUSE_PASSWORD": secrets.token_hex(32), "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT": "1"}
        environment.update(PORCUPINE_CLICKHOUSE_USER=environment["CLICKHOUSE_USER"], PORCUPINE_CLICKHOUSE_PASSWORD=environment["CLICKHOUSE_PASSWORD"])
        archives = directory / "archives"; archives.mkdir(mode=0o777); archives.chmod(0o777)
        configuration = directory / "backup.xml"
        configuration.write_text('<clickhouse><storage_configuration><disks><backups><type>local</type><path>/var/lib/clickhouse/backups/</path></backups></disks></storage_configuration><backups><allowed_disk>backups</allowed_disk></backups></clickhouse>')
        name = "porcupine-permanent-restore-" + secrets.token_hex(6)
        endpoint = create_ch(stack, name, storage, environment, archives, configuration)
        binary = directory / "porcupine"
        subprocess.run(["go", "build", "-o", str(binary), "./cmd/porcupine"], cwd=ROOT, check=True)
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0)); port = reservation.getsockname()[1]
        origin = "http://127.0.0.1:" + str(port)
        bootstrap = secrets.token_hex(32)
        source, restored, bundle = directory / "source.db", directory / "restored.db", directory / "bundle"
        _, stop = app_start(stack, binary, source, origin, endpoint, bootstrap, environment, directory, hot_receipt_limit=0)
        admin = token(origin, ("bootstrap", bootstrap))
        project = api(origin, "/api/v1/projects", {"name": "permanent restore fixture"}, admin, expected=201)
        ingest = project["ingest_token"]
        event = lambda uuid: {"api_key": ingest, "uuid": uuid, "event": "permanent recovery", "distinct_id": "synthetic"}
        api(origin, "/capture/", event("before-checkpoint"))
        wait_until(lambda: sqlite_rows(source, "SELECT count(*) FROM event_receipts")[0][0] == 1, "source event delivered")
        check(not sqlite_rows(source, "SELECT value FROM metadata WHERE key='receipt_source_config'"), "default source remains unbound to finite receipt history")
        tool(["backup", "--db", str(source), "--clickhouse-url", endpoint, "--database", DATABASE,
              "--output", str(bundle)], environment, directory / "backup-private.log")
        stop()
        ch(endpoint, environment, "DROP DATABASE " + DATABASE + " SYNC")
        tool(["restore", "--bundle", str(bundle), "--db", str(restored), "--clickhouse-url", endpoint,
              "--acknowledge-credential-rollback"], environment, directory / "restore-private.log")
        check(not sqlite_rows(restored, "SELECT value FROM metadata WHERE key='receipt_source_config'"), "restore preserves permanent-ledger outage behavior")
        docker("stop", "-t", "30", name)
        process, _ = app_start(stack, binary, restored, origin, endpoint, bootstrap, environment, directory, hot_receipt_limit=0)
        fresh_admin = token(origin, ("bootstrap", bootstrap))
        duplicate = api(origin, "/capture/", event("before-checkpoint"))
        check(duplicate["accepted"] == 0 and duplicate["duplicates"] == 1, "offline restored receipt deduplicates")
        accepted = api(origin, "/capture/", event("accepted-with-clickhouse-offline"))
        check(accepted["accepted"] == 1, "offline restored ingestion stays available")
        queued = api(origin, "/api/v1/projects/" + project["project"]["id"] + "/events", token=fresh_admin)
        check(len(queued["events"]) == 1 and queued["events"][0]["uuid"] == "accepted-with-clickhouse-offline", "offline acceptance is durably queued")
        check(process.poll() is None, "restored default-mode process remains running")
    return {"passed": True, "method": "native online backup/restore followed by actual default-mode restart with ClickHouse stopped",
            "checks": ["default receipt source stays unbound", "OAuth recovery exchange", "offline duplicate receipt", "offline new event durable acceptance"],
            "production_connected": False, "disposable_resources_cleaned": True}


if __name__ == "__main__":
    try:
        report = run()
    except DrillFailure as failure:
        report = {"passed": False, "failed_check": str(failure), "production_connected": False}
    print(json.dumps(report, indent=2))
    raise SystemExit(0 if report["passed"] else 1)
