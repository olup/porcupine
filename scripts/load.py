#!/usr/bin/env python3
"""Bounded synthetic ingestion and query load; credentials never enter reports."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import re
import ssl
import stat
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

from credentials import CredentialError, load_credentials


EVENT_NAME = "porcupine_load"
BODY_LIMIT = 1 << 20
RESPONSE_LIMIT = 2 << 20
COUNT_SQL = ("SELECT count() AS total FROM events WHERE event={event:String} "
             "AND JSONExtractString(properties,'run_id')={run_id:String}")


@dataclass(frozen=True)
class Stage:
    seconds: float
    rps: float
    batch: int
    concurrency: int

    @classmethod
    def parse(cls, value):
        try:
            seconds, rps, batch, concurrency = value.split(":")
            stage = cls(float(seconds), float(rps), int(batch), int(concurrency))
            if (not math.isfinite(stage.seconds) or not math.isfinite(stage.rps)
                    or not 0 < stage.seconds <= 86400 or not 0 < stage.rps <= 10000
                    or not 1 <= stage.batch <= 1000 or not 1 <= stage.concurrency <= 128):
                raise ValueError
            return stage
        except ValueError:
            raise argparse.ArgumentTypeError(
                "stage must be seconds:rps:batch:concurrency, with seconds<=86400, "
                "rps<=10000, batch 1..1000, concurrency 1..128") from None


class Histogram:
    """Fixed memory latency histogram, with two-percent upper-bound quantiles."""

    def __init__(self):
        self.buckets = [0] * 1200
        self.count = 0
        self.total = 0.0
        self.maximum = 0.0

    def add(self, seconds):
        micros = max(seconds * 1_000_000, 1)
        index = min(len(self.buckets) - 1, math.ceil(math.log(micros) / math.log(1.02)))
        self.buckets[index] += 1
        self.count += 1
        self.total += seconds
        self.maximum = max(self.maximum, seconds)

    def quantile(self, fraction):
        if not self.count:
            return None
        target = math.ceil(self.count * fraction)
        cumulative = 0
        for index, count in enumerate(self.buckets):
            cumulative += count
            if cumulative >= target:
                return round(1.02 ** index / 1000, 3)

    def report(self):
        return {"p50_ms": self.quantile(0.50), "p95_ms": self.quantile(0.95),
                "p99_ms": self.quantile(0.99),
                "mean_ms": round(self.total / self.count * 1000, 3) if self.count else None,
                "max_ms": round(self.maximum * 1000, 3) if self.count else None}


class Measurements:
    def __init__(self):
        self.lock = threading.Lock()
        self.groups = {}
        self.first_batch = None
        self.latest_count = 0
        self.maximum_count = 0

    def record(self, group, elapsed, status, error=None, sent=0, accepted=0, duplicates=0):
        with self.lock:
            row = self.groups.setdefault(group, {
                "requests": 0, "errors": 0, "events_sent": 0, "events_accepted": 0,
                "events_duplicate": 0, "http_statuses": {}, "error_types": {},
                "latency": Histogram()})
            row["requests"] += 1
            row["events_sent"] += sent
            row["events_accepted"] += accepted
            row["events_duplicate"] += duplicates
            row["latency"].add(elapsed)
            key = str(status) if status else "transport"
            row["http_statuses"][key] = row["http_statuses"].get(key, 0) + 1
            if error:
                row["errors"] += 1
                row["error_types"][error] = row["error_types"].get(error, 0) + 1

    def report(self):
        with self.lock:
            return {name: {**{key: value for key, value in row.items() if key != "latency"},
                           "latency": row["latency"].report()}
                    for name, row in self.groups.items()}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        return None


class Client:
    def __init__(self, base_url, credentials, timeout, ca_file=None, ingestion_auth="auto"):
        credentials.validate_target(base_url)
        parsed = urllib.parse.urlsplit(base_url)
        if (parsed.scheme not in ("http", "https") or not parsed.hostname
                or parsed.username is not None or parsed.password is not None
                or parsed.query or parsed.fragment):
            raise ValueError("URL must be an HTTP(S) endpoint without credentials, query or fragment")
        self.base = base_url.rstrip("/")
        self.credentials = credentials
        self.ingestion_auth = ingestion_mode(credentials, ingestion_auth)
        self.timeout = timeout
        self.context = ssl.create_default_context(cafile=ca_file)
        self.local = threading.local()
        self.query_path = "/api/v1/projects/" + urllib.parse.quote(
            credentials["project_id"], safe="") + "/query"

    def request(self, path, body, token=None, timeout=None):
        if not hasattr(self.local, "opener"):
            self.local.opener = urllib.request.build_opener(
                NoRedirect(), urllib.request.HTTPSHandler(context=self.context))
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = "Bearer " + token
        request = urllib.request.Request(self.base + path, data=body, headers=headers,
                                         method="POST")
        started = time.monotonic()
        status, result, error = 0, None, None
        try:
            with self.local.opener.open(request, timeout=timeout or self.timeout) as response:
                status = response.status
                raw = response.read(RESPONSE_LIMIT + 1)
                if len(raw) > RESPONSE_LIMIT:
                    error = "response_too_large"
                else:
                    try:
                        result = json.loads(raw)
                    except (ValueError, UnicodeDecodeError):
                        error = "invalid_json"
        except urllib.error.HTTPError as failure:
            status = failure.code
            error = "http_" + str(status)
            failure.close()
        except (OSError, ValueError, urllib.error.URLError):
            error = "transport_error"
        return status, result, error, time.monotonic() - started

    def count(self, run_id, measurements, group="query", timeout=None):
        body = json.dumps({"sql": COUNT_SQL, "use_cache": False, "parameters": {
            "event": EVENT_NAME, "run_id": run_id}}).encode()
        started = time.monotonic()
        try:
            token = self.credentials.get("read_token") or self.credentials["admin_token"]
        except CredentialError:
            measurements.record(group, time.monotonic() - started, 0, "oauth_token_exchange")
            return None, None
        status, result, error, elapsed = self.request(self.query_path, body, token, timeout)
        count, delivery = None, None
        if not error:
            try:
                rows = result["result"]["data"]
                if len(rows) != 1 or type(rows[0]["total"]) not in (str, int):
                    raise ValueError
                count = int(rows[0]["total"])
                delivery = result["delivery"]
                if (count < 0 or delivery["enabled"] is not True
                        or type(delivery["pending"]) is not int
                        or delivery["pending"] < 0
                        or type(delivery.get("pending_mutations", 0)) is not int
                        or delivery.get("pending_mutations", 0) < 0):
                    raise ValueError
            except (KeyError, TypeError, ValueError, OverflowError):
                count, delivery, error = None, None, "invalid_query_response"
        measurements.record(group, elapsed, status, error)
        if count is not None:
            with measurements.lock:
                measurements.latest_count = count
                measurements.maximum_count = max(measurements.maximum_count, count)
        return count, delivery


def ingestion_mode(credentials, requested="auto"):
    mode = ("oauth" if "write_token" in credentials else "public") if requested == "auto" else requested
    if mode == "oauth" and "write_token" not in credentials:
        raise ValueError("OAuth ingestion requires writer client credentials")
    if mode == "public" and "ingest_token" not in credentials:
        raise ValueError("public ingestion requires an ingestion key")
    return mode


def credentials_from(path, ca_file=None, ingestion_auth="auto"):
    try:
        source = Path(path)
        if os.name == "posix" and stat.S_IMODE(source.stat().st_mode) & 0o077:
            raise ValueError("credentials file must have owner-only permissions, such as mode 600")
        credentials = load_credentials(source, ca_file=ca_file)
        if "read_token" not in credentials and "admin_token" not in credentials:
            raise ValueError("load queries require reader credentials")
        ingestion_mode(credentials, ingestion_auth)
        return credentials
    except OSError:
        raise ValueError("cannot read a valid private credentials JSON file") from None


def make_batch(credentials, run_id, stage, request_index, size, property_bytes, ingestion_auth="auto"):
    namespace = uuid.uuid5(uuid.NAMESPACE_URL, "porcupine-load:" + run_id)
    timestamp = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    events = [{"uuid": str(uuid.uuid5(namespace, f"{stage}:{request_index}:{index}")),
               "event": EVENT_NAME, "distinct_id": "synthetic-load-visitor",
               "timestamp": timestamp, "properties": {
                   "run_id": run_id, "synthetic": True, "stage": stage,
                   "payload": "x" * property_bytes}}
              for index in range(size)]
    payload = {"batch": events}
    if ingestion_mode(credentials, ingestion_auth) == "public":
        payload["api_key"] = credentials["ingest_token"]
    body = json.dumps(payload,
                      separators=(",", ":")).encode()
    if len(body) > BODY_LIMIT:
        raise ValueError("batch plus synthetic properties exceeds the 1 MiB ingestion limit")
    return body


def ingest(client, measurements, body, size, group, duplicate=False):
    started = time.monotonic()
    try:
        token = client.credentials["write_token"] if client.ingestion_auth == "oauth" else None
    except CredentialError:
        measurements.record(group, time.monotonic() - started, 0, "oauth_token_exchange", size)
        return False
    status, result, error, elapsed = client.request("/batch/", body, token)
    accepted, duplicates = 0, 0
    if not error:
        try:
            accepted, duplicates = result["accepted"], result["duplicates"]
            if (type(accepted) is not int or type(duplicates) is not int
                    or accepted < 0 or duplicates < 0 or accepted + duplicates != size
                    or result["status"] != 1):
                raise ValueError
            if duplicate and (accepted != 0 or duplicates != size):
                raise ValueError
            if not duplicate and duplicates != 0:
                raise ValueError
        except (KeyError, TypeError, ValueError):
            accepted, duplicates, error = 0, 0, "invalid_duplicate_response" if duplicate else "invalid_ingest_response"
    measurements.record(group, elapsed, status, error, size, accepted, duplicates)
    if not duplicate and not error:
        with measurements.lock:
            if measurements.first_batch is None:
                measurements.first_batch = body, size
    return error is None


def run_stage(client, measurements, run_id, stage, index, property_bytes):
    started = time.monotonic()
    deadline = started + stage.seconds
    scheduled = math.ceil(stage.seconds * stage.rps)
    dispatched = skipped_capacity = skipped_schedule = 0
    pending = set()
    with ThreadPoolExecutor(max_workers=stage.concurrency,
                            thread_name_prefix="porcupine-ingest") as pool:
        for request_index in range(scheduled):
            due = started + request_index / stage.rps
            remaining = due - time.monotonic()
            if remaining > 0:
                time.sleep(remaining)
            now = time.monotonic()
            if now >= deadline:
                skipped_schedule += scheduled - request_index
                break
            completed = {future for future in pending if future.done()}
            for future in completed:
                future.result()
            pending -= completed
            if len(pending) >= stage.concurrency:
                skipped_capacity += 1
                continue
            # Do not catch up missed schedule slots in a burst.
            if now - due > max(1 / stage.rps, 0.01):
                skipped_schedule += 1
                continue
            body = make_batch(client.credentials, run_id, index, request_index,
                              stage.batch, property_bytes, client.ingestion_auth)
            pending.add(pool.submit(ingest, client, measurements, body, stage.batch,
                                    "ingest_stage_" + str(index)))
            dispatched += 1
        remaining = deadline - time.monotonic()
        if remaining > 0:
            time.sleep(remaining)
        for future in pending:
            future.result()
    elapsed = time.monotonic() - started
    return {"stage": index, "seconds": stage.seconds, "target_rps": stage.rps,
            "batch": stage.batch, "concurrency": stage.concurrency,
            "scheduled_requests": scheduled, "dispatched_requests": dispatched,
            "skipped_capacity": skipped_capacity, "skipped_schedule": skipped_schedule,
            "elapsed_seconds": round(elapsed, 3),
            "achieved_request_rps": round(dispatched / elapsed, 3)}


def drain(client, measurements, run_id, expected, timeout, interval):
    started = time.monotonic()
    deadline = started + timeout
    stable = 0
    count = delivery = None
    while time.monotonic() < deadline:
        count, delivery = client.count(run_id, measurements, "drain",
                                       min(client.timeout, max(0.1, deadline - time.monotonic())))
        if (count == expected and delivery is not None and delivery["pending"] == 0
                and delivery.get("pending_mutations", 0) == 0):
            stable += 1
            if stable == 2:
                return {"matched": True, "expected_events": expected, "actual_events": count,
                        "pending": 0, "pending_mutations": 0,
                        "elapsed_seconds": round(time.monotonic() - started, 3)}
        else:
            stable = 0
        if count is not None and count > expected:
            break
        time.sleep(min(interval, max(0, deadline - time.monotonic())))
    return {"matched": False, "expected_events": expected, "actual_events": count,
            "pending": delivery.get("pending") if delivery else None,
            "pending_mutations": delivery.get("pending_mutations", 0) if delivery else None,
            "elapsed_seconds": round(time.monotonic() - started, 3)}


def run(args):
    credentials = credentials_from(args.credentials, args.ca_file, args.ingestion_auth)
    client = Client(args.url, credentials, args.timeout, args.ca_file, args.ingestion_auth)
    run_id = args.run_id or "load-" + uuid.uuid4().hex
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", run_id):
        raise ValueError("run_id must contain 1..64 ASCII letters, digits, underscores or hyphens")
    stages = args.stage or [Stage(10, 2, 20, 4), Stage(20, 10, 50, 8)]
    if len(stages) > 100:
        raise ValueError("at most 100 stages are supported")
    payload_sizes = []
    for index, stage in enumerate(stages, 1):
        payload_sizes.append(len(make_batch(credentials, run_id, index, 0, stage.batch,
                                           args.property_bytes, client.ingestion_auth)))
    measurements = Measurements()
    initial, _ = client.count(run_id, measurements, "preflight")
    if initial is None:
        raise ValueError("preflight query failed; check endpoint, read permission and ClickHouse readiness")
    if initial != 0:
        raise ValueError("run_id already has events; use a new run_id")
    stop = threading.Event()

    def query_loop():
        while not stop.is_set():
            client.count(run_id, measurements)
            stop.wait(args.query_interval)

    query_thread = threading.Thread(target=query_loop, name="porcupine-query", daemon=True)
    started_at = datetime.now(timezone.utc).isoformat()
    started = time.monotonic()
    query_thread.start()
    stage_reports = []
    try:
        for index, stage in enumerate(stages, 1):
            stage_reports.append(run_stage(client, measurements, run_id, stage, index,
                                           args.property_bytes))
        ingest_elapsed = time.monotonic() - started
        duplicate_attempted = measurements.first_batch is not None
        duplicate_verified = False
        if duplicate_attempted:
            body, size = measurements.first_batch
            duplicate_verified = ingest(client, measurements, body, size, "duplicate", True)
    finally:
        stop.set()
        query_thread.join()
    load_elapsed = time.monotonic() - started
    measured = measurements.report()
    ingest_groups = [value for name, value in measured.items() if name.startswith("ingest_stage_")]
    expected = sum(value["events_accepted"] for value in ingest_groups)
    drained = drain(client, measurements, run_id, expected, args.drain_timeout, args.poll_interval)
    measured = measurements.report()
    errors = sum(group["errors"] for group in measured.values())
    dispatched = sum(stage["dispatched_requests"] for stage in stage_reports)
    skipped = sum(stage["skipped_capacity"] + stage["skipped_schedule"] for stage in stage_reports)
    passed = drained["matched"] and duplicate_verified and errors == 0 and skipped == 0
    return {"passed": passed, "run_id": run_id, "target": client.base,
            "ingestion_auth": client.ingestion_auth,
            "oauth_token_exchanges": credentials.token_exchanges(),
            "property_bytes_per_event": args.property_bytes,
            "sample_batch_bytes": payload_sizes,
            "project_id": credentials["project_id"], "synthetic_event": EVENT_NAME,
            "started_at": started_at, "finished_at": datetime.now(timezone.utc).isoformat(),
            "latency_method": "2_percent_histogram_upper_bound",
            "load_elapsed_seconds": round(load_elapsed, 3),
            "ingest_elapsed_seconds": round(ingest_elapsed, 3),
            "ingest_requests": dispatched, "ingest_events_sent": sum(g["events_sent"] for g in ingest_groups),
            "ingest_events_accepted": expected, "request_errors": errors,
            "generator_skipped_requests": skipped,
            "accepted_event_rps": round(expected / ingest_elapsed, 3),
            "duplicate": {"attempted": duplicate_attempted, "verified": duplicate_verified},
            "query": {"last_observed_count": measurements.latest_count,
                      "maximum_observed_count": measurements.maximum_count},
            "stages": stage_reports, "measurements": measured, "drain": drained}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True, help="HTTP(S) service URL; redirects are rejected")
    parser.add_argument("--credentials", required=True, help="private JSON file, mode 600")
    parser.add_argument("--ingestion-auth", choices=("auto", "oauth", "public"), default="auto",
                        help="auto uses an OAuth writer when supplied, otherwise the public key")
    parser.add_argument("--stage", action="append", type=Stage.parse,
                        help="repeat seconds:rps:batch:concurrency; RPS counts batch requests")
    parser.add_argument("--query-interval", type=float, default=1)
    parser.add_argument("--poll-interval", type=float, default=1)
    parser.add_argument("--drain-timeout", type=float, default=180)
    parser.add_argument("--timeout", type=float, default=15)
    parser.add_argument("--property-bytes", type=int, default=128)
    parser.add_argument("--run-id", help="optional unique synthetic run marker")
    parser.add_argument("--ca-file", help="optional PEM CA file; HTTPS verification stays enabled")
    parser.add_argument("--output", help="also write the safe JSON report to an owner-only file")
    args = parser.parse_args()
    if (not all(math.isfinite(value) for value in (args.query_interval, args.poll_interval,
                                                   args.drain_timeout, args.timeout))
            or not 0.1 <= args.query_interval <= 3600
            or not 0.1 <= args.poll_interval <= 60
            or not 1 <= args.drain_timeout <= 3600
            or not 0.1 <= args.timeout <= 120
            or not 0 <= args.property_bytes <= 8192):
        parser.error("intervals, timeouts or property size exceed supported bounds")
    try:
        report = run(args)
    except KeyboardInterrupt:
        report = {"passed": False, "error": "interrupted"}
    except ValueError as error:
        report = {"passed": False, "error": str(error)}
    except Exception:
        report = {"passed": False, "error": "load generator failed"}
    encoded = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output:
        try:
            descriptor = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(descriptor, "w") as target:
                os.fchmod(target.fileno(), 0o600)
                target.write(encoded)
        except OSError:
            print(json.dumps({"passed": False, "error": "cannot write output report"}))
            return 1
    sys.stdout.write(encoded)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
