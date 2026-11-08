"""Private Docker/SQLite sampling and explicit local-soak acceptance checks."""
import json
import math
import os
from pathlib import Path
import re
import subprocess
import threading
import time
from datetime import datetime

PROBE = r'''package main
import("database/sql";"encoding/json";"os";"time";_ "modernc.org/sqlite")
func main(){
 out:=map[string]any{}; failures:=[]string{}
 db,err:=sql.Open("sqlite","file:"+os.Args[1]+"?mode=ro&_pragma=busy_timeout(1000)");if err!=nil{panic("probe open failed")};defer db.Close();db.SetMaxOpenConns(1)
 number:=func(label,query string){var value int64;if db.QueryRow(query).Scan(&value)!=nil{failures=append(failures,label)}else{out[label]=value}}
 number("pending_count","SELECT pending_count FROM queue_usage WHERE id=1")
 number("pending_bytes","SELECT pending_bytes FROM queue_usage WHERE id=1")
 for _,table:=range []string{"event_receipts","event_heads","mutation_outbox","mutation_audit","receipt_provenance","receipt_lookup_leases","oauth_tokens"}{var exists int;db.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",table).Scan(&exists);if exists==1{number(table+"_rows","SELECT count(*) FROM "+table)}}
 number("page_count","PRAGMA page_count");number("page_size","PRAGMA page_size");number("freelist_pages","PRAGMA freelist_count")
 if len(os.Args)>2{for _,table:=range []string{"event_receipts","events","event_heads"}{count:=0;for _,uuid:=range os.Args[2:]{var found bool;if db.QueryRow("SELECT EXISTS(SELECT 1 FROM "+table+" WHERE uuid=?)",uuid).Scan(&found)!=nil{failures=append(failures,"key_probe")}else if found{count++}};out["probe_"+table+"_keys"]=count};out["probed_keys"]=len(os.Args)-2}
 var oldest sql.NullString;if db.QueryRow("SELECT min(received_at) FROM events WHERE delivered=0").Scan(&oldest)!=nil{failures=append(failures,"oldest_pending")}else{age:=float64(0);if oldest.Valid{received,e:=time.Parse(time.RFC3339Nano,oldest.String);if e!=nil{failures=append(failures,"oldest_pending_time")}else{age=time.Since(received).Seconds()}};out["oldest_pending_seconds"]=age}
 total:=int64(0);for _,suffix:=range []string{"","-wal","-shm"}{if info,e:=os.Stat(os.Args[1]+suffix);e==nil{out["sqlite"+suffix+"_bytes"]=info.Size();total+=info.Size()}};out["sqlite_total_bytes"]=total
 out["errors"]=failures;json.NewEncoder(os.Stdout).Encode(out)
}'''


def container_group(name):
    prefixes = ("porcupine-native-", "porcupine-online-", "porcupine-s3-", "porcupine-receipt-",
                "porcupine-integration-", "porcupine-oauth-bench-", "pc-backup-")
    return "test_drill" if name.startswith(prefixes) else "unrelated"


def container_counts(names):
    counts = {"test_drill": 0, "unrelated": 0}
    for name in names:
        counts[container_group(name)] += 1
    return counts


def build_probe(root, directory, architecture):
    source = Path(directory) / "probe.go"
    source.write_text(PROBE)
    binary = Path(directory) / "probe"
    subprocess.run(["go", "build", "-o", str(binary), str(source)], cwd=root,
        env={**os.environ, "CGO_ENABLED": "0", "GOOS": "linux", "GOARCH": architecture},
        check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    binary.chmod(0o755)
    return binary


def memory_bytes(value):
    match = re.fullmatch(r"([0-9.]+)([A-Za-z]+)", value.strip())
    if not match:
        raise ValueError("invalid Docker memory metric")
    units = {"B": 1, "kB": 1000, "KB": 1000, "MB": 1000**2, "GB": 1000**3,
             "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3}
    return round(float(match[1]) * units[match[2]])


class Sampler:
    def __init__(self, docker, app, clickhouse, output, interval=5, history_query_substring=None, owned_extra=(), history_fact_substring=" PREWHERE "):
        self.docker, self.app, self.ch = docker, app, clickhouse
        self.output, self.interval = Path(output), interval
        self.history_query_substring = history_query_substring
        self.history_fact_substring = history_fact_substring
        self.owned = {app, clickhouse, *owned_extra}
        self.other_seen = set()
        self.samples, self.failures = [], 0
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._loop, name="private-soak-sampler", daemon=True)

    def history_cost(self):
        if not self.history_query_substring:
            return {"available": False, "reason": "history_query_signature_not_configured"}
        signature = self.history_query_substring.encode().hex()
        fact = "position(query,unhex('" + self.history_fact_substring.encode().hex() + "'))>0"
        sql = "/* porcupine:soak-metrics */ SELECT count() AS queries, sum(read_rows) AS total_read_rows, sum(read_bytes) AS total_read_bytes, sum(query_duration_ms) AS duration_ms, max(query_duration_ms) AS max_duration_ms, countIf(" + fact + ") AS fact_queries, sumIf(read_bytes," + fact + ") AS fact_read_bytes, sumIf(read_rows," + fact + ") AS fact_read_rows FROM system.query_log WHERE type='QueryFinish' AND is_initial_query=1 AND position(query,'porcupine:soak-metrics')=0 AND position(query,unhex('" + signature + "'))>0 FORMAT JSONEachRow"
        try:
            raw = self.docker("exec", self.ch, "sh", "-c", 'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --query "$1"', "sh", sql)
            counters = {key.removeprefix("total_"): int(value) for key, value in json.loads(raw).items()}
            return {"available": True, **counters}
        except (ValueError, KeyError, subprocess.CalledProcessError):
            return {"available": False, "reason": "query_log_unavailable"}

    def sample(self):
        started = time.monotonic()
        phase = "sqlite"
        try:
            sqlite = json.loads(self.docker("exec", self.app, "/measure", "/data/porcupine.db"))
            if sqlite["errors"]:
                raise ValueError("SQLite probe failed")
            phase = "container_resources"
            resources = [json.loads(row) for row in self.docker("stats", "--no-stream", "--format", "{{json .}}", self.app, self.ch).splitlines()]
            resource_values = {}
            for row in resources:
                role = {self.app: "api", self.ch: "clickhouse"}[row["Name"]]
                resource_values[role] = {"cpu_percent": float(row["CPUPerc"].removesuffix("%")),
                    "memory_bytes": memory_bytes(row["MemUsage"].split(" / ")[0]),
                    "pids": int(row["PIDs"])}
            query = "SELECT count() AS active_parts, sum(rows) AS physical_rows, sum(bytes_on_disk) AS bytes_on_disk FROM system.parts WHERE active AND database='porcupine' AND table='events_v2' FORMAT JSONEachRow"
            phase = "clickhouse_parts"
            parts = json.loads(self.docker("exec", self.ch, "sh", "-c", 'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --query "$1"', "sh", query))
            phase = "clickhouse_directory_size"
            for attempt in range(3):
                try:
                    disk = int(self.docker("exec", self.ch, "du", "-sk", "/var/lib/clickhouse").split()[0]) * 1024
                    break
                except subprocess.CalledProcessError:
                    if attempt == 2:
                        raise
                    time.sleep(0.05)
            phase = "container_inventory"
            others = [name for name in self.docker("ps", "--format", "{{.Names}}").splitlines() if name not in self.owned]
            self.other_seen.update(others)
            row = {"sampled_at_unix": round(time.time(), 3), "sqlite": sqlite, "resources": resource_values,
                "clickhouse": {**{key: int(value) for key, value in parts.items()}, "data_directory_bytes": disk},
                "history_lookups": self.history_cost(),
                "other_containers": container_counts(others),
                "sampling_elapsed_seconds": round(time.monotonic() - started, 3)}
            self.samples.append(row)
        except (ValueError, KeyError, subprocess.CalledProcessError):
            self.failures += 1
            row = {"sampled_at_unix": round(time.time(), 3), "error": "private_metrics_sample_failed", "phase": phase}
        with self.output.open("a") as target:
            target.write(json.dumps(row) + "\n")

    def _loop(self):
        while not self.stop.is_set():
            started = time.monotonic()
            self.sample()
            self.stop.wait(max(0, self.interval - (time.monotonic() - started)))

    def start(self):
        self.output.write_text("")
        self.thread.start()

    def finish(self):
        self.stop.set()
        self.thread.join()
        self.sample()

    def summary(self):
        if not self.samples:
            return {"samples": 0, "failed_samples": self.failures}
        first, last = self.samples[0], self.samples[-1]
        middle = self.samples[len(self.samples) // 2]
        elapsed = max(1, last["sampled_at_unix"] - first["sampled_at_unix"])
        late_elapsed = max(1, last["sampled_at_unix"] - middle["sampled_at_unix"])
        receipt_delta = last["sqlite"]["event_receipts_rows"] - first["sqlite"]["event_receipts_rows"]
        return {"samples": len(self.samples), "failed_samples": self.failures,
            "first_sample_unix": first["sampled_at_unix"], "last_sample_unix": last["sampled_at_unix"],
            "other_containers_distinct_seen": container_counts(self.other_seen),
            "other_containers_max_concurrent": {group: max(row["other_containers"][group] for row in self.samples) for group in ("test_drill", "unrelated")},
            "max_pending_count": max(row["sqlite"]["pending_count"] for row in self.samples),
            "max_pending_bytes": max(row["sqlite"]["pending_bytes"] for row in self.samples),
            "max_oldest_pending_seconds": max(row["sqlite"]["oldest_pending_seconds"] for row in self.samples),
            "max_api_memory_bytes": max(row["resources"]["api"]["memory_bytes"] for row in self.samples),
            "max_clickhouse_memory_bytes": max(row["resources"]["clickhouse"]["memory_bytes"] for row in self.samples),
            "max_api_cpu_percent": max(row["resources"]["api"]["cpu_percent"] for row in self.samples),
            "max_clickhouse_cpu_percent": max(row["resources"]["clickhouse"]["cpu_percent"] for row in self.samples),
            "first_sqlite_bytes": first["sqlite"]["sqlite_total_bytes"],
            "final_sqlite_bytes": last["sqlite"]["sqlite_total_bytes"],
            "max_sqlite_bytes": max(row["sqlite"]["sqlite_total_bytes"] for row in self.samples),
            "sqlite_growth_bytes_per_second": round((last["sqlite"]["sqlite_total_bytes"] - first["sqlite"]["sqlite_total_bytes"]) / elapsed, 3),
            "late_half_sqlite_growth_bytes_per_second": round((last["sqlite"]["sqlite_total_bytes"] - middle["sqlite"]["sqlite_total_bytes"]) / late_elapsed, 3),
            "first_receipt_rows": first["sqlite"]["event_receipts_rows"],
            "final_receipt_rows": last["sqlite"]["event_receipts_rows"],
            "max_receipt_rows": max(row["sqlite"]["event_receipts_rows"] for row in self.samples),
            "final_history_lookups": last.get("history_lookups", {"available": False, "reason": "not_measured_in_this_snapshot"}),
            "receipt_growth_rows_per_second": round(receipt_delta / elapsed, 3),
            "late_half_receipt_growth_rows_per_second": round((last["sqlite"]["event_receipts_rows"] - middle["sqlite"]["event_receipts_rows"]) / late_elapsed, 3),
            "final_clickhouse_data_bytes": last["clickhouse"]["data_directory_bytes"],
            "first_clickhouse_data_bytes": first["clickhouse"]["data_directory_bytes"],
            "maximum_sample_gap_seconds": round(max([0] + [right["sampled_at_unix"] - left["sampled_at_unix"] for left, right in zip(self.samples, self.samples[1:])]), 3),
            "sampled_duration_seconds": round(elapsed, 3)}


def evaluate(report, metrics, thresholds, hot_receipt_limit=0):
    checks = {
        "all_target_requests_dispatched": report["generator_skipped_requests"] == 0 and all(row["dispatched_requests"] == row["scheduled_requests"] for row in report["stages"]),
        "no_request_errors": report["request_errors"] == 0,
        "all_dispatched_events_accepted": report["ingest_events_sent"] == report["ingest_events_accepted"],
        "sql_count_matches_acknowledged_events": report["drain"]["matched"] and report["drain"]["actual_events"] == report["ingest_events_accepted"],
        "drained_pending_events_and_mutations": report["drain"]["pending"] == 0 and report["drain"]["pending_mutations"] == 0,
        "stable_uuid_retry_deduplicated": report["duplicate"]["verified"],
        "both_oauth_clients_renewed": report["renewal_verified"],
        "private_samples_complete": metrics["samples"] >= 2 and metrics["failed_samples"] == 0,
    }
    for metric, maximum in thresholds.items():
        if metric.startswith("max_"):
            checks[metric + "_within_limit"] = metrics.get(metric, math.inf) <= maximum
    if hot_receipt_limit:
        checks["hot_receipts_within_limit_after_drain"] = metrics.get("final_receipt_rows", math.inf) <= hot_receipt_limit
        checks["sampled_hot_receipts_within_limit"] = metrics.get("max_receipt_rows", math.inf) <= hot_receipt_limit
    if "minimum_successful_samples" in thresholds:
        checks["minimum_sampling_coverage"] = metrics["samples"] >= thresholds["minimum_successful_samples"]
        checks["sampling_gaps_within_limit"] = metrics.get("maximum_sample_gap_seconds", math.inf) <= thresholds["maximum_sample_gap_seconds"]
        start = datetime.fromisoformat(report["started_at"]).timestamp()
        duration = report["ingest_elapsed_seconds"]
        covered = max(0, min(metrics.get("last_sample_unix", 0), start + duration) - max(metrics.get("first_sample_unix", math.inf), start))
        checks["ingest_window_sampling_coverage"] = covered / duration >= thresholds["minimum_ingest_window_coverage"]
        checks["initial_sample_within_limit"] = metrics.get("first_sample_unix", math.inf) - start <= thresholds["maximum_sample_gap_seconds"]
    return {"passed": all(checks.values()), "checks": checks, "limits": thresholds,
            "hot_receipt_limit": hot_receipt_limit, "sampled_metrics_are_not_continuous_peaks": True}
