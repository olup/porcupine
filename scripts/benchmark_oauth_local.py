#!/usr/bin/env python3
"""Reproduce a bounded OAuth/SQL load run in disposable local Docker resources."""
import argparse
import base64
from contextlib import ExitStack
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import secrets
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

from load import NoRedirect, Stage, make_batch, run
from credentials import load_credentials
from soak_metrics import Sampler, build_probe, evaluate
from integration_storage import start_minio, clickhouse_s3_docker_args, minio_objects

ROOT = Path(__file__).resolve().parents[1]
CLICKHOUSE = "clickhouse/clickhouse-server:26.8.36.3"


def write_json(path, value):
    temporary = path.with_name(path.name + ".tmp-" + secrets.token_hex(4))
    try:
        temporary.write_text(json.dumps(value, indent=2) + "\n")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def docker(*args, environment=None):
    return subprocess.check_output(["docker", *args], cwd=ROOT, env=environment,
                                   stderr=subprocess.PIPE, text=True).strip()


def request(opener, origin, path, body=None, token=None, form=None, basic=None):
    headers = {"Accept": "application/json"}
    data = None
    if body is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(body).encode()
    if form is not None:
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        data = urllib.parse.urlencode(form).encode()
    if token:
        headers["Authorization"] = "Bearer " + token
    if basic:
        headers["Authorization"] = "Basic " + base64.b64encode(
            (urllib.parse.quote_plus(basic[0], safe="") + ":" +
             urllib.parse.quote_plus(basic[1], safe="")).encode()).decode()
    with opener.open(urllib.request.Request(origin + path, data=data, headers=headers), timeout=10) as response:
        return json.load(response)


def check_cold_storage(storage, app, ch, opener, origin, project_id, credentials, report, sampler, hot_limit):
    """Require physical cold placement, an evicted-key lookup and stable logical count."""
    phase = "partition_discovery"
    result = {"manual_partition_move": True, "retry_payload_timestamp_regenerated": True}
    def ch_query(sql):
        return docker("exec", ch, "sh", "-c", 'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --query "$1"', "sh", sql)
    try:
        retry_body = json.loads(make_batch(credentials, report["run_id"], 1, 1, 20, 512, "oauth"))
        keys = [event["uuid"] for event in retry_body["batch"]]
        probe = json.loads(docker("exec", app, "/measure", "/data/porcupine.db", *keys))
        result["pre_retry_local_key_counts"] = {key: value for key, value in probe.items() if key.startswith("probe")}
        evicted = not probe["errors"] and all(probe["probe_" + table + "_keys"] == 0 for table in ("events", "event_receipts", "event_heads"))
        ch_query("SYSTEM STOP MERGES porcupine.events_v2")
        partitions = ch_query("SELECT DISTINCT partition FROM system.parts WHERE active AND database='porcupine' AND table='events_v2'").splitlines()
        if not partitions or any(not partition.isdigit() for partition in partitions):
            raise ValueError("invalid local partition set")
        phase = "native_cold_partition_move"
        for partition in partitions:
            ch_query("ALTER TABLE porcupine.events_v2 MOVE PARTITION '" + partition + "' TO VOLUME 'cold'")
        placement = json.loads(ch_query("SELECT count() AS active_parts,countIf(disk_name='s3') AS s3_parts FROM system.parts WHERE active AND database='porcupine' AND table='events_v2' FORMAT JSONEachRow"))
        placement = {key: int(value) for key, value in placement.items()}
        result.update(partitions_moved=len(partitions), placement=placement)
        phase = "cold_history_retry"
        ch_query("SYSTEM FLUSH LOGS")
        before = sampler.history_cost()
        retry = request(opener, origin, "/batch/", retry_body, token=credentials["write_token"])
        ch_query("SYSTEM FLUSH LOGS")
        after = sampler.history_cost()
        lookup_delta = after.get("fact_queries", 0) - before.get("fact_queries", 0)
        read_delta = after.get("fact_read_bytes", 0) - before.get("fact_read_bytes", 0)
        phase = "post_retry_oauth_sql_reconciliation"
        response = request(opener, origin, "/api/v1/projects/" + project_id + "/query", {
            "sql": "SELECT count() AS total FROM events WHERE event={event:String} AND JSONExtractString(properties,'run_id')={run_id:String}", "use_cache": False,
            "parameters": {"event": "porcupine_load", "run_id": report["run_id"]}}, token=credentials["read_token"])
        count = int(response["result"]["data"][0]["total"])
        previous_samples = len(sampler.samples)
        sampler.sample()
        cold_metrics = sampler.samples[-1] if len(sampler.samples) > previous_samples else None
        checks = {
            "nonempty_partitions_moved": len(partitions) > 0,
            "all_active_parts_on_s3": placement["active_parts"] > 0 and placement["s3_parts"] == placement["active_parts"],
            "s3_objects_present": b"<Contents>" in minio_objects(storage),
            "stable_uuid_retry_deduplicated": retry == {"status": 1, "accepted": 0, "duplicates": 20},
            "sql_count_unchanged_after_retry": count == report["ingest_events_accepted"],
            "delivery_still_drained": response["delivery"]["pending"] == 0 and response["delivery"]["pending_mutations"] == 0,
            "post_cold_private_sample_succeeded": cold_metrics is not None,
        }
        if hot_limit:
            checks.update(retried_keys_evicted_from_local_metadata=evicted,
                fact_lookup_observed=before.get("available") and after.get("available") and lookup_delta > 0 and read_delta > 0,
                receipts_within_limit_after_cold_retry=cold_metrics is not None and cold_metrics["sqlite"]["event_receipts_rows"] <= hot_limit)
        result.update(checks=checks, oldest_unretried_batch_uuid_retry=retry,
            sql_count_after_retry=count, post_retry_private_metrics=cold_metrics,
            history_before_retry=before, history_after_retry=after,
            fact_lookup_count_delta=lookup_delta, fact_lookup_read_bytes_delta=read_delta,
            matched=all(checks.values()), status="passed" if all(checks.values()) else "failed")
    except Exception:
        # Transport exceptions may include URLs or credentials; retain only our fixed phase label.
        result.update(matched=False, status="failed", failed_check=phase)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=ROOT / "benchmark-results")
    parser.add_argument("--seconds", type=int, default=45)
    parser.add_argument("--rps", type=float, default=20, help="steady-stage batch requests per second")
    parser.add_argument("--batch", type=int, default=50)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--sample-interval", type=float, default=5)
    parser.add_argument("--query-interval", type=float, default=1)
    parser.add_argument("--drain-timeout", type=float, default=120)
    parser.add_argument("--hot-receipt-limit", type=int, default=0)
    parser.add_argument("--history-query-substring", help="known history lookup SQL signature for private query_log cost sampling")
    parser.add_argument("--history-fact-substring", default=" PREWHERE ", help="finalized query fragment distinguishing UUID facts from metadata-only source checks")
    parser.add_argument("--s3", action="store_true", help="use isolated MinIO and verify a native cold-volume move after draining")
    parser.add_argument("--max-pending", type=int, default=20000)
    parser.add_argument("--max-oldest-seconds", type=float, default=30)
    parser.add_argument("--max-sqlite-mib", type=int, default=1024)
    args = parser.parse_args()
    if args.s3 and args.hot_receipt_limit and not args.history_query_substring:
        parser.error("finite S3 proof requires the finalized server history query signature")
    if not args.history_fact_substring or len(args.history_fact_substring) > 256 or args.history_query_substring and len(args.history_query_substring) > 256:
        parser.error("history query signature exceeds 256 characters")
    if (not 40 <= args.seconds <= 3600 or not 0.1 <= args.rps <= 200
            or not 1 <= args.batch <= 1000 or not 1 <= args.concurrency <= 64
            or not 2 <= args.sample_interval <= 60 or not 0.1 <= args.query_interval <= 60
            or not 1 <= args.drain_timeout <= 600 or not 0 <= args.hot_receipt_limit <= 10000000
            or not 1 <= args.max_pending <= 100000 or not 1 <= args.max_oldest_seconds <= 600
            or not 1 <= args.max_sqlite_mib <= 10240):
        parser.error("duration, load, sampling or acceptance limits exceed bounded local-test ranges")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "oauth-load.json").unlink(missing_ok=True)
    write_json(args.output_dir / "run-state.json", {"phase": "building", "started_at_unix": time.time()})
    suffix = secrets.token_hex(6)
    prefix = "porcupine-oauth-bench-" + suffix
    network, app, ch, image, volume = prefix, prefix + "-api", prefix + "-ch", prefix + ":local", prefix + "-data"
    bootstrap, password = secrets.token_hex(32), secrets.token_hex(32)
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    origin = "http://127.0.0.1:" + str(port)
    environment = {**os.environ, "CLICKHOUSE_USER": "porcupine_admin", "CLICKHOUSE_PASSWORD": password,
        "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT": "1", "PORCUPINE_BOOTSTRAP_TOKEN": bootstrap,
        "PORCUPINE_CLICKHOUSE_PASSWORD": password, "PORCUPINE_CLICKHOUSE_USER": "porcupine_admin",
        "PORCUPINE_CLICKHOUSE_DATABASE": "porcupine", "PORCUPINE_CLICKHOUSE_URL": "http://" + ch + ":8123",
        "PORCUPINE_OAUTH_ISSUER": origin, "PORCUPINE_OAUTH_TOKEN_LIFETIME_SECONDS": "60",
        "PORCUPINE_HOT_RECEIPT_LIMIT": str(args.hot_receipt_limit),
        "PORCUPINE_CLICKHOUSE_STORAGE_POLICY": "", "PORCUPINE_HOT_STORAGE_DAYS": "30",
        "PORCUPINE_CLICKHOUSE_COLD_VOLUME": "cold",
        "PORCUPINE_QUEUE_MAX_EVENTS": "100000", "PORCUPINE_QUEUE_MAX_BYTES": str(256 << 20)}
    opener = urllib.request.build_opener(NoRedirect(), urllib.request.ProxyHandler({}))
    source_files = sorted({path for path in subprocess.check_output(
        ["git", "ls-files", "-co", "--exclude-standard"], cwd=ROOT, text=True).splitlines()
        if path.endswith((".go", ".py")) or path in ("Dockerfile", "go.mod", "go.sum", "deployment/clickhouse/storage.xml")})
    fingerprint = hashlib.sha256()
    source_contents = {name: (ROOT / name).read_bytes() for name in source_files}
    for name in source_files:
        fingerprint.update(name.encode() + b"\0" + source_contents[name] + b"\0")
    source = {"commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
              "dirty_status": subprocess.check_output(["git", "status", "--short"], cwd=ROOT, text=True).splitlines(),
              "source_sha256": fingerprint.hexdigest(), "fingerprint_files": source_files}
    created = []
    probe_directory = tempfile.TemporaryDirectory(prefix=prefix + "-probe-")
    sampler = None
    resources = ExitStack()
    storage = None
    try:
        probe = build_probe(ROOT, probe_directory.name, docker("version", "--format", "{{.Server.Arch}}"))
        with tempfile.TemporaryDirectory(prefix=prefix + "-source-") as snapshot:
            for name, content in source_contents.items():
                target = Path(snapshot) / name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(content)
            docker("build", "-t", image, snapshot)
        s3_flags = []
        if args.s3:
            storage = resources.enter_context(start_minio())
            network = storage["network"]
            environment.update(storage["environment"])
            environment.update(PORCUPINE_CLICKHOUSE_STORAGE_POLICY="hot_cold", PORCUPINE_HOT_STORAGE_DAYS="1", PORCUPINE_CLICKHOUSE_COLD_VOLUME="cold")
            storage_config = Path(probe_directory.name) / "storage.xml"
            storage_config.write_bytes(source_contents["deployment/clickhouse/storage.xml"])
            storage_config.chmod(0o644)
            s3_flags = [value.replace(str(ROOT / "deployment/clickhouse/storage.xml"), str(storage_config))
                        for value in clickhouse_s3_docker_args(storage)[2:]]
        else:
            docker("network", "create", network)
            created.append(("network", network))
        docker("volume", "create", volume)
        created.append(("volume", volume))
        docker("run", "-d", "--name", ch, "--network", network, "--memory", "2g", "--cpus", "2",
               "-e", "CLICKHOUSE_USER", "-e", "CLICKHOUSE_PASSWORD", "-e", "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT",
               *s3_flags,
               CLICKHOUSE, environment=environment)
        created.append(("container", ch))
        for attempt in range(150):
            try:
                if docker("exec", ch, "sh", "-c", 'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --query "SELECT 1"') == "1":
                    break
            except subprocess.CalledProcessError:
                pass
            time.sleep(0.2)
        else:
            raise RuntimeError("local ClickHouse readiness failed")
        docker("run", "-d", "--name", app, "--network", network, "--memory", "512m", "--cpus", "1",
               "-p", "127.0.0.1:" + str(port) + ":8080", "-v", volume + ":/data",
               "--mount", "type=bind,src=" + str(probe) + ",dst=/measure,readonly",
               *[item for key in environment if key.startswith("PORCUPINE_") and key in {
                   "PORCUPINE_BOOTSTRAP_TOKEN", "PORCUPINE_CLICKHOUSE_PASSWORD", "PORCUPINE_CLICKHOUSE_USER",
                   "PORCUPINE_CLICKHOUSE_DATABASE", "PORCUPINE_CLICKHOUSE_URL", "PORCUPINE_OAUTH_ISSUER",
                   "PORCUPINE_OAUTH_TOKEN_LIFETIME_SECONDS", "PORCUPINE_HOT_RECEIPT_LIMIT",
                   "PORCUPINE_QUEUE_MAX_EVENTS", "PORCUPINE_QUEUE_MAX_BYTES",
                   "PORCUPINE_CLICKHOUSE_STORAGE_POLICY", "PORCUPINE_HOT_STORAGE_DAYS", "PORCUPINE_CLICKHOUSE_COLD_VOLUME"} for item in ("-e", key)],
               image, environment=environment)
        created.append(("container", app))
        for attempt in range(150):
            try:
                ready = request(opener, origin, "/readyz")
                if ready.get("status") == "ready":
                    break
            except (OSError, ValueError, urllib.error.URLError):
                pass
            time.sleep(0.2)
        else:
            raise RuntimeError("local service readiness failed")
        token = request(opener, origin, "/oauth/token", form={"grant_type": "client_credentials",
            "resource": origin + "/api/v1"}, basic=("bootstrap", bootstrap))
        assert token["expires_in"] == 60
        project = request(opener, origin, "/api/v1/projects", {"name": "bounded-local-oauth-load",
            "ingestion_mode": "server_only"}, token=token["access_token"])
        assert project["project"]["ingestion_mode"] == "server_only" and "ingest_token" not in project
        clients = {}
        for role, permission in (("write", "events:write"), ("read", "events:read")):
            created_client = request(opener, origin, "/api/v1/oauth-clients", {"name": "load-" + role,
                "permissions": [permission], "projects": [project["project"]["id"]]}, token=token["access_token"])
            clients[role] = {key: created_client[key] for key in ("client_id", "client_secret")}
        with tempfile.TemporaryDirectory(prefix=prefix) as directory:
            credentials = Path(directory) / "credentials.json"
            descriptor = os.open(credentials, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "w") as output:
                json.dump({"project_id": project["project"]["id"], "oauth": {"issuer": origin, **clients}}, output)
            cold_credentials = load_credentials(credentials)
            cold_credentials.validate_target(origin)
            sampler = Sampler(docker, app, ch, args.output_dir / "resource-trends.jsonl", args.sample_interval,
                args.history_query_substring, (storage["container"],) if storage else (), args.history_fact_substring)
            sampler.start()
            try:
                report = run(argparse.Namespace(credentials=credentials, ca_file=None, url=origin, timeout=10,
                    ingestion_auth="oauth", run_id=None, stage=[Stage(10, 10, 20, 4), Stage(args.seconds - 10, args.rps, args.batch, args.concurrency)],
                    property_bytes=512, query_interval=args.query_interval, drain_timeout=args.drain_timeout, poll_interval=0.5))
            finally:
                if args.history_query_substring:
                    try:
                        docker("exec", ch, "sh", "-c", 'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --query "SYSTEM FLUSH LOGS"')
                    except subprocess.CalledProcessError:
                        pass
                sampler.finish()
        exchanges = report["oauth_token_exchanges"]
        report["renewal_verified"] = exchanges.get("read/api/v1", 0) >= 2 and exchanges.get("write/api/v1", 0) >= 2
        report["passed"] = report["passed"] and report["renewal_verified"]
        report["sampled_trends"] = sampler.summary()
        if args.hot_receipt_limit:
            receipt_counts = [row["sqlite"]["event_receipts_rows"] for row in sampler.samples]
            report["hot_receipt_observations"] = {"configured_limit": args.hot_receipt_limit,
                "sampled_maximum": max(receipt_counts, default=0),
                "samples_above_limit": sum(count > args.hot_receipt_limit for count in receipt_counts),
                "maximum_sampled_excess": max([0] + [count - args.hot_receipt_limit for count in receipt_counts]),
                "final_count": report["sampled_trends"].get("final_receipt_rows")}
        report["acceptance"] = evaluate(report, report["sampled_trends"], {
            "max_pending_count": args.max_pending, "max_oldest_pending_seconds": args.max_oldest_seconds,
            "max_api_memory_bytes": 384 << 20, "max_clickhouse_memory_bytes": 1536 << 20,
            "max_sqlite_bytes": args.max_sqlite_mib << 20,
            "minimum_successful_samples": math.ceil(args.seconds / args.sample_interval * 0.9),
            "minimum_ingest_window_coverage": 0.9,
            "maximum_sample_gap_seconds": args.sample_interval * 2.5}, args.hot_receipt_limit)
        report["passed"] = report["passed"] and report["acceptance"]["passed"]
        report["run_configuration"] = {**vars(args), "output_dir": str(args.output_dir),
            "property_bytes": 512, "request_timeout_seconds": 10, "poll_interval_seconds": 0.5,
            "queue_max_events": 100000, "queue_max_bytes": 256 << 20,
            "query_sql": "SELECT count() AS total FROM events WHERE event={event:String} AND JSONExtractString(properties,'run_id')={run_id:String}"}
        report["production_connected"] = False
        report["source"] = source
        report["server_only_no_public_key"] = True
        report["credentials_permissions"] = {"write": ["events:write"], "read": ["events:read"]}
        report["docker"] = {"server_version": docker("version", "--format", "{{.Server.Version}}"),
            "api_image_id": docker("image", "inspect", image, "--format", "{{.Id}}"),
            "clickhouse_image_id": docker("image", "inspect", CLICKHOUSE, "--format", "{{.Id}}"),
            "api_cpus": 1, "api_memory_mib": 512, "clickhouse_cpus": 2, "clickhouse_memory_mib": 2048,
            "host_architecture": platform.machine(), "token_lifetime_seconds": 60}
        if storage:
            report["docker"]["minio"] = {"cpus": 1, "memory_mib": 512,
                "container_name": storage["container"],
                "image_id": docker("inspect", storage["container"], "--format", "{{.Image}}")}
        report["resource_snapshot"] = json.loads("[" + ",".join(docker("stats", "--no-stream", "--format", "{{json .}}", app, ch).splitlines()) + "]")
        report["load_passed"] = report["passed"]
        if storage:
            report["cold_storage_check"] = {"status": "pending"}
            report["passed"] = False
        write_json(args.output_dir / "oauth-load.json", report)
        if storage:
            report["cold_storage_check"] = check_cold_storage(storage, app, ch, opener, origin,
                project["project"]["id"], cold_credentials, report, sampler, args.hot_receipt_limit)
            report["passed"] = report["load_passed"] and report["cold_storage_check"]["matched"]
        # Docker names are random local identifiers, not credentials.
        write_json(args.output_dir / "oauth-load.json", report)
        print(json.dumps({key: report[key] for key in ("passed", "ingest_events_accepted", "accepted_event_rps", "request_errors", "renewal_verified", "oauth_token_exchanges")}))
        write_json(args.output_dir / "run-state.json", {"phase": "complete", "passed": report["passed"]})
        return 0 if report["passed"] else 1
    except BaseException:
        write_json(args.output_dir / "run-state.json", {"phase": "failed", "error": "benchmark_execution_failed"})
        raise
    finally:
        if sampler and sampler.thread.is_alive():
            sampler.finish()
        for kind, name in reversed(created):
            try:
                docker("rm", "-fv", name) if kind == "container" else docker(kind, "rm", name)
            except subprocess.CalledProcessError:
                pass
        try:
            docker("image", "rm", image)
        except subprocess.CalledProcessError:
            pass
        probe_directory.cleanup()
        resources.close()


if __name__ == "__main__":
    raise SystemExit(main())
