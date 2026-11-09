#!/usr/bin/env python3
"""Prove online checkpoint and native restore with disposable local services only.

Starts a real Porcupine binary, ClickHouse and MinIO. Private data stays in a
TemporaryDirectory; stdout and the optional report contain only verification metadata.
"""
import argparse
import base64
from contextlib import ExitStack
from datetime import datetime, timezone
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import re
from pathlib import Path
import secrets
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request

from integration_storage import start_minio, clickhouse_s3_docker_args, minio_objects
from restore_drill import docker, remove_container, wait_clickhouse

ROOT = Path(__file__).resolve().parents[1]
IMAGE = "clickhouse/clickhouse-server:26.8.36.3"
DATABASE = "porcupine_online_drill"
TABLE = DATABASE + ".events_v2"


class DrillFailure(Exception):
    pass


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, newurl):
        return None


OPENER = urllib.request.build_opener(NoRedirect(), urllib.request.ProxyHandler({}))


def check(condition, label):
    if not condition:
        raise DrillFailure(label)


def http(origin, path, body=None, token=None, form=None, basic=None, method=None):
    headers = {"Accept": "application/json"}
    data = None
    if body is not None:
        data, headers["Content-Type"] = json.dumps(body).encode(), "application/json"
    if form is not None:
        data, headers["Content-Type"] = urllib.parse.urlencode(form).encode(), "application/x-www-form-urlencoded"
    if token:
        headers["Authorization"] = "Bearer " + token
    if basic:
        value = urllib.parse.quote_plus(basic[0], safe="") + ":" + urllib.parse.quote_plus(basic[1], safe="")
        headers["Authorization"] = "Basic " + base64.b64encode(value.encode()).decode()
    request = urllib.request.Request(origin + path, data=data, headers=headers, method=method or ("POST" if data is not None else "GET"))
    try:
        response = OPENER.open(request, timeout=15)
    except urllib.error.HTTPError as error:
        response = error
    except Exception:
        raise DrillFailure("local HTTP request failed") from None
    with response:
        raw = response.read((4 << 20) + 1)
        check(len(raw) <= 4 << 20, "bounded HTTP response")
        try:
            return response.status, json.loads(raw) if raw else None
        except ValueError:
            raise DrillFailure("local HTTP JSON response failed") from None


def api(origin, path, body=None, token=None, expected=200, method=None):
    status, value = http(origin, path, body=body, token=token, method=method)
    check(status == expected, "API status for " + path.split("?")[0].rsplit("/", 1)[-1])
    return value


def token(origin, role, resource=None, expected=200):
    status, body = http(origin, "/oauth/token", form={"grant_type": "client_credentials", "resource": resource or origin + "/api/v1"}, basic=role)
    check(status == expected, "OAuth exchange status")
    return body["access_token"] if expected == 200 else None


def ch(endpoint, environment, sql):
    request = urllib.request.Request(endpoint, data=sql.encode(), headers={"X-ClickHouse-User": environment["CLICKHOUSE_USER"], "X-ClickHouse-Key": environment["CLICKHOUSE_PASSWORD"]})
    try:
        with OPENER.open(request, timeout=30) as response:
            return response.read(8 << 20)
    except Exception:
        raise DrillFailure("isolated ClickHouse operation failed") from None


def sqlite_rows(path, sql, parameters=()):
    try:
        with sqlite3.connect("file:" + str(path) + "?mode=ro", uri=True, timeout=5) as database:
            return database.execute(sql, parameters).fetchall()
    except sqlite3.OperationalError as error:
        metadata_error = re.fullmatch(r"(no such table|no such column): ([A-Za-z_][A-Za-z0-9_]*)", str(error))
        raise DrillFailure("SQLite fixture " + Path(path).name + " at line " + str(traceback.extract_stack()[-2].lineno) + ": " + (metadata_error.group(0) if metadata_error else str(error)[:160])) from None


def sqlite_state(path):
    state = {}
    for table in ("projects", "access_keys", "oauth_clients", "events", "event_receipts", "event_heads", "mutation_audit", "mutation_outbox", "delivery_quarantine", "delivery_ready", "cookieless_keys", "queue_usage", "identity_profiles", "identity_links"):
        rows = sqlite_rows(path, "SELECT * FROM " + table)
        convert = lambda value: value.hex() if isinstance(value, bytes) else value
        rows.sort(key=lambda row: json.dumps(row, default=convert, separators=(",", ":")))
        encoded = json.dumps(rows, default=convert, separators=(",", ":")).encode()
        state[table] = {"rows": len(rows), "sha256": hashlib.sha256(encoded).hexdigest()}
    return state


def fingerprint(endpoint, environment):
    sql = "SELECT event_id,project_id,uuid,event,distinct_id,hex(SHA256(properties)) AS properties_sha,timestamp,received_at,revision,is_deleted,hex(SHA256(original_snapshot)) AS evidence_sha,hex(SHA256(identity_snapshot)) AS identity_sha,source_id,message_id,event_type,hex(SHA256(context)) AS context_sha FROM (SELECT * FROM " + TABLE + " ORDER BY revision DESC LIMIT 1 BY project_id,uuid) ORDER BY project_id,uuid FORMAT JSONEachRow"
    return hashlib.sha256(ch(endpoint, environment, sql)).hexdigest()


class ProjectionProxy:
    def __init__(self, target):
        self.target = target
        self.block = threading.Event()
        self.fail_backup = threading.Event()
        self.native_windows = []
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                query = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query).get("query", [body.decode(errors="replace")])[0].lstrip()
                native = query.upper().startswith("BACKUP ")
                if owner.block.is_set() and query.upper().startswith("INSERT INTO "):
                    self.send_response(503); self.end_headers(); self.wfile.write(b"projection gate closed"); return
                if native and owner.fail_backup.is_set():
                    self.send_response(500); self.end_headers(); self.wfile.write(b"injected backup failure"); return
                started = time.monotonic()
                headers = {key: value for key, value in self.headers.items() if key.lower() not in {"host", "content-length", "connection", "transfer-encoding"}}
                request = urllib.request.Request(owner.target + self.path, data=body, headers=headers, method="POST")
                try:
                    try:
                        response = OPENER.open(request, timeout=180)
                    except urllib.error.HTTPError as error:
                        response = error
                    with response:
                        raw = response.read(8 << 20)
                        self.send_response(response.status)
                        for key, value in response.headers.items():
                            if key.lower() not in {"content-length", "connection", "transfer-encoding", "server", "date"}:
                                self.send_header(key, value)
                        self.send_header("Content-Length", str(len(raw))); self.end_headers(); self.wfile.write(raw)
                except Exception:
                    try:
                        self.send_response(502); self.end_headers()
                    except OSError:
                        pass
                finally:
                    if native:
                        owner.native_windows.append((started, time.monotonic()))

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = "http://127.0.0.1:" + str(self.server.server_port)

    def close(self):
        self.server.shutdown(); self.server.server_close(); self.thread.join(timeout=5)


def create_ch(stack, name, storage, environment, backups, configuration):
    docker("run", "-d", "--name", name, "--label", "porcupine.online-restore=true", "--memory", "2g", "--cpus", "2",
           "-p", "127.0.0.1::8123", "-e", "CLICKHOUSE_USER", "-e", "CLICKHOUSE_PASSWORD", "-e", "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT",
           "--mount", "type=bind,src=" + str(configuration) + ",dst=/etc/clickhouse-server/config.d/online-backup.xml,readonly",
           "--mount", "type=bind,src=" + str(backups) + ",dst=/var/lib/clickhouse/backups",
           *clickhouse_s3_docker_args(storage), IMAGE, environment=environment)
    stack.callback(remove_container, name)
    return wait_clickhouse(name, environment)


def wait_until(probe, label, seconds=45):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if probe():
            return
        time.sleep(0.05)
    raise DrillFailure(label)


def app_start(stack, binary, db, origin, endpoint, bootstrap, environment, directory, hot_receipt_limit=4):
    clean_environment = {key: value for key, value in environment.items() if not key.startswith("PORCUPINE_")}
    settings = {**clean_environment, "PORCUPINE_ADDR": urllib.parse.urlsplit(origin).netloc, "PORCUPINE_DB": str(db),
                "PORCUPINE_QUEUE_MAX_EVENTS": "2000", "PORCUPINE_QUEUE_MAX_BYTES": str(64 << 20),
                "PORCUPINE_OAUTH_ALLOW_LEGACY_KEYS": "false", "PORCUPINE_HOT_STORAGE_DAYS": "30",
                "PORCUPINE_BOOTSTRAP_TOKEN": bootstrap, "PORCUPINE_OAUTH_ISSUER": origin,
                "PORCUPINE_OAUTH_TOKEN_LIFETIME_SECONDS": "3600", "PORCUPINE_CLICKHOUSE_URL": endpoint,
                "PORCUPINE_CLICKHOUSE_DATABASE": DATABASE, "PORCUPINE_CLICKHOUSE_USER": environment["CLICKHOUSE_USER"],
                "PORCUPINE_CLICKHOUSE_PASSWORD": environment["CLICKHOUSE_PASSWORD"], "PORCUPINE_CLICKHOUSE_STORAGE_POLICY": "hot_cold",
                "PORCUPINE_CLICKHOUSE_COLD_VOLUME": "cold", "PORCUPINE_HOT_RECEIPT_LIMIT": str(hot_receipt_limit)}
    log = stack.enter_context((directory / (db.stem + "-private.log")).open("wb"))
    process = subprocess.Popen([str(binary)], cwd=directory, env=settings, stdout=log, stderr=log)
    def stop():
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill(); process.wait(timeout=5)
    stack.callback(stop)
    def ready():
        check(process.poll() is None, "real Porcupine process remained running")
        try:
            return http(origin, "/readyz")[0] == 200
        except DrillFailure:
            return False
    wait_until(ready, "live Porcupine readiness")
    return process, stop


def tool(arguments, environment, log_path, expected=0, progress=None):
    with log_path.open("wb") as errors:
        process = subprocess.Popen([sys.executable, str(ROOT / "scripts/online_backup.py"), *arguments], env=environment,
                                   stdout=subprocess.PIPE, stderr=errors, text=True)
        lines = []
        def collect():
            for line in process.stdout:
                try:
                    value = json.loads(line)
                    lines.append(value)
                    if progress:
                        progress(value)
                except (ValueError, DrillFailure):
                    lines.append({"event": "invalid_progress"})
        thread = threading.Thread(target=collect, daemon=True); thread.start()
        try:
            result = process.wait(timeout=240)
        except subprocess.TimeoutExpired:
            process.kill(); process.wait(timeout=5)
            raise DrillFailure("online backup tool exceeded bounded runtime") from None
        thread.join(timeout=20)
        check(not thread.is_alive() and not any(entry.get("event")=="invalid_progress" for entry in lines), "online backup progress coordination")
        stages = {"checkpoint_acquired", "sqlite_snapshot_complete", "native_backup_started", "native_backup_finished", "backup_complete", "restore_complete"}
        last = next((entry.get("event") for entry in reversed(lines) if entry.get("event") in stages), "startup")
        check((result == 0) == (expected == 0), "online backup tool exit after " + last)
        return lines


def run():
    checks = []
    started_at = datetime.now(timezone.utc).isoformat()
    with ExitStack() as stack:
        directory = Path(stack.enter_context(tempfile.TemporaryDirectory(prefix="porcupine-online-restore-"))).resolve()
        source_storage = stack.enter_context(start_minio())
        destination_storage = stack.enter_context(start_minio())
        suffix = secrets.token_hex(6)
        source_name, destination_name = "porcupine-online-source-" + suffix, "porcupine-online-target-" + suffix
        environment = {**os.environ, **source_storage["environment"], "CLICKHOUSE_USER": "porcupine_online_admin",
                       "CLICKHOUSE_PASSWORD": secrets.token_hex(32), "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT": "1"}
        environment.update(PORCUPINE_CLICKHOUSE_USER=environment["CLICKHOUSE_USER"], PORCUPINE_CLICKHOUSE_PASSWORD=environment["CLICKHOUSE_PASSWORD"])
        backup_dir = directory / "native-backups"; backup_dir.mkdir(mode=0o777); backup_dir.chmod(0o777)
        backup_xml = directory / "backup.xml"
        backup_xml.write_text('<clickhouse><storage_configuration><disks><backups><type>local</type><path>/var/lib/clickhouse/backups/</path></backups></disks></storage_configuration><backups><allowed_disk>backups</allowed_disk><allowed_path>/var/lib/clickhouse/backups/</allowed_path></backups></clickhouse>')
        source_ch = create_ch(stack, source_name, source_storage, environment, backup_dir, backup_xml)
        proxy = ProjectionProxy(source_ch); stack.callback(proxy.close)
        binary = directory / "porcupine"
        subprocess.run(["go", "build", "-o", str(binary), "./cmd/porcupine"], cwd=ROOT, env={**os.environ,"CGO_ENABLED":"0"}, check=True, stdout=subprocess.DEVNULL)
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0)); port = reservation.getsockname()[1]
        origin = "http://127.0.0.1:" + str(port)
        source_db, restored_db = directory / "source.db", directory / "restored.db"
        bootstrap = secrets.token_hex(32)
        process, stop_source = app_start(stack,binary,source_db,origin,proxy.url,bootstrap,environment,directory)
        admin = token(origin,("bootstrap",bootstrap))
        created = api(origin,"/api/v1/projects",{"name":"online recovery synthetic"},admin,201)
        pid, ingest = created["project"]["id"],created["ingest_token"]
        other = api(origin,"/api/v1/projects",{"name":"isolated other project"},admin,201)
        base = "/api/v1/projects/" + pid
        clients = {}
        for role in ("active","revoked"):
            created_client=api(origin,"/api/v1/oauth-clients",{"name":role,"permissions":["events:read","events:edit","events:delete"],"projects":[pid]},admin,201)
            clients[role]=(created_client["client_id"],created_client["client_secret"])
        reader = token(origin,clients["active"])
        native_client = api(origin,"/api/v1/oauth-clients",{"name":"restore native source","source_id":"restore-native","permissions":["events:write"],"projects":[pid]},admin,201)
        clients["native"] = (native_client["client_id"], native_client["client_secret"])
        native_writer = token(origin, clients["native"])
        wait_until(lambda:http(origin,base+"/query",body={"sql":"SELECT count() AS total FROM events","use_cache":False},token=reader)[0]==200,"source analytics initialization",45)
        revoked_token = token(origin,clients["revoked"])
        api(origin,"/api/v1/oauth-clients/"+clients["revoked"][0],token=admin,expected=204,method="DELETE")
        payloads = {}
        def capture(name, properties=None, distinct="synthetic-person"):
            body={"api_key":ingest,"uuid":name,"event":name,"distinct_id":distinct,"properties":properties or {"synthetic":True}}
            response=api(origin,"/capture/",body)
            check(response["accepted"]==1 and response["duplicates"]==0,"new synthetic event acknowledged")
            payloads[name]=body
        baseline = ["cold-evicted","deleted-evicted","corrected","pending-correction","pending-deletion","cookieless"]
        for name in baseline:
            capture(name,distinct="$posthog_cookieless" if name=="cookieless" else "synthetic-person")
        capture("before-login", distinct="browser-before-login")
        native_original = {"type":"pageview","message_id":"restore-native-message","actor_id":"browser-before-login",
                           "context":{"page":{"url":"https://synthetic.example/restore"},"locale":"fr-FR","additional":{"preserved":True}}}
        native_receipt = api(origin,"/ingest/v1/events",native_original,native_writer)
        check(native_receipt["accepted"] == 1, "accepted native original before checkpoint")
        native_event_id = native_receipt["receipts"][0]["event_id"]
        link = {"api_key": ingest, "uuid": "restore-link", "event": "$identify", "distinct_id": "account-42",
                "properties": {"$anon_distinct_id": "browser-before-login", "$set": {"plan": "free"}}}
        check(api(origin, "/capture/", link)["accepted"] == 1, "accepted identity link before checkpoint")
        for index in range(16):
            capture("bulk-%02d"%index,{"synthetic":True,"backup_data":secrets.token_hex(150000)})
        last_diagnostic = [time.monotonic()]
        def drain():
            counts = sqlite_rows(source_db,"SELECT (SELECT count(*) FROM events),(SELECT count(*) FROM mutation_outbox),(SELECT count(*) FROM event_receipts),(SELECT count(*) FROM delivery_quarantine)")[0]
            if sum(counts[:2]) and time.monotonic()-last_diagnostic[0] > 10:
                print(json.dumps({"event":"source_queue_wait","pending_originals":counts[0],"pending_mutations":counts[1],"hot_receipts":counts[2],"quarantined":counts[3]}),file=sys.stderr,flush=True)
                last_diagnostic[0] = time.monotonic()
            return sum(counts[:2]) == 0
        wait_until(drain,"seed delivery did not drain",60)
        native_projection = api(origin,base+"/query",{"sql":"SELECT uuid,event_id,source_id,message_id,event_type,context FROM events WHERE source_id='restore-native' AND message_id='restore-native-message'"},reader)["result"]["data"]
        check(len(native_projection) == 1 and native_projection[0]["event_id"] == native_event_id, "native original projected with acknowledged numeric ID")
        native_uuid = native_projection[0]["uuid"]
        def edit(name,event,key):
            return api(origin,base+"/events/"+name,{"idempotency_key":key,"expected_revision":"0","snapshot":{"event":event,"distinct_id":"synthetic-person","properties":{"corrected":True},"timestamp":"2026-01-01T00:00:00Z"}},reader,202,method="PATCH")["mutation_id"]
        def remove(name,key):
            return api(origin,base+"/events/"+name,{"idempotency_key":key,"expected_revision":"0"},reader,202,method="DELETE")["mutation_id"]
        edit("corrected","corrected-v2","applied-correction"); remove("deleted-evicted","applied-deletion")
        wait_until(drain,"applied mutations did not drain",45)
        for index in range(8):
            capture("receipt-filler-%d"%index)
        wait_until(drain,"receipt fixtures did not drain",45)
        check(not sqlite_rows(source_db,"SELECT 1 FROM event_receipts WHERE project_id=? AND uuid=?",(pid,native_uuid)), "native original receipt evicted before backup")
        check(sqlite_rows(source_db,"SELECT count(*) FROM event_receipts")[0][0]<=4,"bounded hot receipt cache")
        check(sqlite_rows(source_db,"SELECT count(*) FROM event_receipts WHERE uuid IN ('cold-evicted','deleted-evicted')")[0][0]==0,"cold and deleted receipt eviction occurred")
        checks.append("cold and deleted exact-duplicate receipts evicted before checkpoint")
        ch(source_ch,environment,"SYSTEM STOP MERGES "+TABLE)
        for raw in ch(source_ch,environment,"SELECT name FROM system.parts WHERE database='"+DATABASE+"' AND table='events_v2' AND active AND disk_name='default' FORMAT JSONEachRow").splitlines():
            name=json.loads(raw)["name"]; check(all(char.isalnum() or char=='_' for char in name),"safe part name")
            ch(source_ch,environment,"ALTER TABLE "+TABLE+" MOVE PART '"+name+"' TO VOLUME 'cold'")
        check(b"<Contents>" in minio_objects(source_storage),"source native cold objects exist")
        proxy.block.set()
        pending_edit=edit("pending-correction","pending-correction-v2","pending-correction")
        pending_delete=remove("pending-deletion","pending-deletion")
        capture("pending-original")
        update = {"api_key": ingest, "uuid": "restore-traits", "event": "$set", "distinct_id": "account-42",
                  "properties": {"$set": {"plan": "pro"}}}
        check(api(origin, "/capture/", update)["accepted"] == 1, "accepted pending profile update")
        frozen_fingerprint=fingerprint(source_ch,environment)
        checks.append("live source seeded applied and pending corrections/deletions with native cold parts")
        accepted=[]; writer_errors=[]; writer_stop=threading.Event()
        def writer():
            for index in range(1000):
                if writer_stop.is_set(): return
                name="concurrent-%06d"%index
                try:
                    capture(name)
                    accepted.append((name,time.monotonic()))
                except Exception:
                    writer_errors.append(True); return
                writer_stop.wait(.02)
        writer_thread=threading.Thread(target=writer,daemon=True); writer_thread.start()
        def stop_writer():
            writer_stop.set(); writer_thread.join(timeout=20)
        stack.callback(stop_writer)
        wait_until(lambda:len(accepted)>=3,"concurrent capture did not start")
        bundle=directory/"checkpoint"
        backup_args=["backup","--db",str(source_db),"--clickhouse-url",proxy.url,"--database",DATABASE,"--backup-disk","backups","--output",str(bundle),"--timeout","180","--lock-timeout","30"]
        progress=[]
        def observed(value):
            progress.append(value.get("event"))
            if value.get("event")=="checkpoint_acquired":
                # The proxy no longer freezes ClickHouse. The real application's
                # shared projection guard must now block delivery until backup ends.
                proxy.block.clear()
            if value.get("event")=="sqlite_snapshot_complete":
                capture("after-sqlite-snapshot")
        tool(backup_args,environment,directory/"backup-private.log",progress=observed)
        check(not writer_errors and process.poll() is None,"capture remains live throughout native backup")
        check("backup_complete" in progress and proxy.native_windows,"native backup completed")
        native_start,native_end=proxy.native_windows[0]
        during_native=sum(native_start<=when<=native_end for _,when in accepted)
        check(during_native>0,"capture acknowledgements overlap native BACKUP request")
        checkpoint=bundle/"sqlite.db"
        check(sqlite_rows(checkpoint,"PRAGMA integrity_check")==[("ok",)] and not sqlite_rows(checkpoint,"PRAGMA foreign_key_check"),"independent SQLite checkpoint integrity")
        checkpoint_state=sqlite_state(checkpoint)
        pending=sqlite_rows(checkpoint,"SELECT uuid FROM events WHERE project_id=? ORDER BY id",(pid,))
        cut=[row[0] for row in pending if row[0].startswith("concurrent-")]
        check(cut==[name for name,_ in accepted[:len(cut)]],"SQLite checkpoint is exact acknowledged capture prefix")
        check(("after-sqlite-snapshot",) not in pending and checkpoint_state["mutation_outbox"]["rows"]==2,"post-cut capture excluded and pending mutations included")
        checks.append("capture acknowledged during native backup and SQLite snapshot defines an exact prefix cut")
        failed_bundle=directory/"failed-checkpoint"
        proxy.block.set()
        wait_until(lambda:sqlite_rows(source_db,"SELECT count(*) FROM events")[0][0]>0,"failure fixture retains pending capture")
        proxy.fail_backup.set()
        failed_args=backup_args.copy(); failed_args[failed_args.index(str(bundle))]=str(failed_bundle)
        tool(failed_args,environment,directory/"failed-backup-private.log",expected=1)
        proxy.fail_backup.clear(); stop_writer()
        check(not(failed_bundle/"manifest.json").exists(),"failed native backup never publishes complete manifest")
        proxy.block.clear()
        wait_until(drain,"failed backup did not release projection guard",60)
        check(process.poll() is None and not writer_errors,"backup failure did not stop source writer")
        checks.append("injected native backup failure releases guard, keeps service live and publishes no complete checkpoint")
        stop_source()
        docker("stop","-t","30",source_name); docker("stop","-t","30",source_storage["container"])
        destination_environment={**environment,**destination_storage["environment"]}
        target_ch=create_ch(stack,destination_name,destination_storage,destination_environment,backup_dir,backup_xml)
        target_proxy=ProjectionProxy(target_ch); target_proxy.block.set(); stack.callback(target_proxy.close)
        corrupt=directory/"corrupt-checkpoint"; shutil.copytree(bundle,corrupt)
        with (corrupt/"sqlite.db").open("ab") as bad: bad.write(b"invalid checkpoint hash")
        restore_args=["restore","--bundle",str(bundle),"--db",str(restored_db),"--clickhouse-url",target_proxy.url,"--timeout","180","--acknowledge-credential-rollback"]
        bad_args=restore_args.copy(); bad_args[bad_args.index(str(bundle))]=str(corrupt); bad_db=directory/"must-not-publish.db"; bad_args[bad_args.index(str(restored_db))]=str(bad_db)
        tool(bad_args,destination_environment,directory/"bad-restore-private.log",expected=1)
        check(not bad_db.exists(),"corrupt checkpoint cannot publish SQLite restore")
        check(ch(target_ch,destination_environment,"SELECT count() FROM system.databases WHERE name='"+DATABASE+"'").strip()==b"0","corrupt checkpoint does not create target database")
        tool(restore_args,destination_environment,directory/"restore-private.log")
        check(sqlite_rows(restored_db,"PRAGMA integrity_check")==[("ok",)] and not sqlite_rows(restored_db,"PRAGMA foreign_key_check"),"independent restored SQLite integrity")
        check(sqlite_state(restored_db)==checkpoint_state,"restored SQLite operational state matches exact checkpoint")
        check(sqlite_rows(restored_db,"SELECT count(*) FROM oauth_tokens")[0][0]==0,"default restore invalidates old access tokens")
        check(fingerprint(target_ch,destination_environment)==frozen_fingerprint,"restored complete logical projections match independent pre-backup fingerprint")
        # Native RESTORE preserves contents and policy, but may allocate restored
        # parts on the default hot disk. Place them explicitly on the fresh cold
        # volume before proving independent native object reads.
        ch(target_ch,destination_environment,"SYSTEM STOP MERGES "+TABLE)
        for raw in ch(target_ch,destination_environment,"SELECT name FROM system.parts WHERE database='"+DATABASE+"' AND table='events_v2' AND active AND disk_name='default' FORMAT JSONEachRow").splitlines():
            name=json.loads(raw)["name"]; check(all(char.isalnum() or char=='_' for char in name),"safe restored part name")
            ch(target_ch,destination_environment,"ALTER TABLE "+TABLE+" MOVE PART '"+name+"' TO VOLUME 'cold'")
        check(fingerprint(target_ch,destination_environment)==frozen_fingerprint,"native cold move preserves restored projections")
        check(b"<Contents>" in minio_objects(destination_storage),"restored native cold data uses independent fresh MinIO storage")
        cold_parts=int(ch(target_ch,destination_environment,"SELECT count() FROM system.parts WHERE database='"+DATABASE+"' AND table='events_v2' AND active AND disk_name='s3'").strip())
        check(cold_parts>0,"restored active parts read from fresh native S3 disk")
        checks.append("corrupt restore rejected before publication; native backup restores exact state into fresh ClickHouse/MinIO")
        restored_process,_=app_start(stack,binary,restored_db,origin,target_proxy.url,"must-not-reset-bootstrap-secret",destination_environment,directory)
        check(http(origin,base+"/schema",token=reader)[0]==401,"old access token invalid after default restore")
        restored_admin=token(origin,("bootstrap",bootstrap)); restored_reader=token(origin,clients["active"])
        token(origin,clients["revoked"],expected=401)
        check(http(origin,base+"/schema",token=revoked_token)[0]==401,"revoked access remains revoked")
        check(http(origin,"/api/v1/projects/"+other["project"]["id"]+"/schema",token=restored_reader)[0]==403,"restored project isolation")
        token(origin,("bootstrap","must-not-reset-bootstrap-secret"),expected=401)
        check(api(origin,base+"/events/pending-correction",token=restored_reader)["pending"] is True,"pending correction visible before replay")
        check(http(origin,base+"/events/pending-deletion",token=restored_reader)[0]==410,"pending tombstone visible before replay")
        target_proxy.block.clear()
        wait_until(lambda:sqlite_rows(restored_db,"SELECT (SELECT count(*) FROM events)+(SELECT count(*) FROM mutation_outbox)")[0][0]==0,"restored normal worker did not resume",60)
        for mutation in (pending_edit,pending_delete):
            check(api(origin,base+"/event-mutations/"+mutation,token=restored_reader)["state"]=="applied","restored mutation acknowledged")
        rows=[json.loads(raw) for raw in ch(target_ch,destination_environment,"SELECT uuid,event FROM "+TABLE+" FINAL WHERE project_id=unhex('"+pid.encode().hex()+"') AND is_deleted=0 AND identity_snapshot='' ORDER BY uuid FORMAT JSONEachRow").splitlines()]
        expected=set(payloads)-{"deleted-evicted","pending-deletion","after-sqlite-snapshot"}-{name for name,_ in accepted}
        expected.update(cut)
        expected.add(native_uuid)
        check({row["uuid"] for row in rows}==expected and len(rows)==len(expected),"restored history includes exact checkpoint cut and excludes later source acknowledgements")
        event_names={row["uuid"]:row["event"] for row in rows}
        check(event_names["corrected"]=="corrected-v2" and event_names["pending-correction"]=="pending-correction-v2","corrections survive native restore and resumed replay")
        for name in ("cold-evicted","deleted-evicted"):
            result=api(origin,"/capture/",payloads[name]); check(result["accepted"]==0 and result["duplicates"]==1,"evicted cold/deleted receipt remains exact duplicate after restore")
        status=api(origin,base+"/query",{"sql":"SELECT count() AS total FROM events","use_cache":False},restored_reader)
        check(int(status["result"]["data"][0]["total"])==len(expected),"restored reader sees exact checkpoint cardinality")
        resolved = api(origin, base+"/query", {"sql": "SELECT distinct_id,person_id,JSONExtractString(person_properties,'plan') AS plan FROM events WHERE uuid='before-login'"}, restored_reader)
        check(resolved["result"]["data"] == [{"distinct_id": "browser-before-login", "person_id": "account-42", "plan": "pro"}], "restored profile resolves cold pre-login event after pending trait replay")
        checks.append("restored anonymous link and pending trait update resolve through public SQL")
        restored_native = api(origin,base+"/query",{"sql":"SELECT uuid,event_id,source_id,message_id,event_type,context FROM events WHERE source_id='restore-native' AND message_id='restore-native-message'"},restored_reader)["result"]["data"]
        check(restored_native == native_projection, "restored native source/message/type/context and numeric ID are byte-exact")
        restored_native_writer = token(origin, clients["native"])
        native_retry = api(origin,"/ingest/v1/events",native_original,restored_native_writer)
        check(native_retry["duplicates"] == 1 and native_retry["receipts"][0]["event_id"] == native_event_id, "restored evicted native receipt preserves original numeric ID")
        checks.append("native original canonical metadata and evicted numeric receipt survive coordinated restore and independent cold storage")
        mcp_token=token(origin,clients["active"],origin+"/mcp")
        def mcp(method,params):
            request=urllib.request.Request(origin+"/mcp",data=json.dumps({"jsonrpc":"2.0","id":1,"method":method,"params":params}).encode(),headers={"Content-Type":"application/json","Accept":"application/json, text/event-stream","MCP-Protocol-Version":"2025-11-25","Authorization":"Bearer "+mcp_token})
            with OPENER.open(request,timeout=10) as response:return json.load(response)
        check("result" in mcp("initialize",{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"online-restore-drill","version":"1"}}),"restored MCP session")
        result=mcp("tools/call",{"name":"porcupine_call","arguments":{"operation_id":"query-events","path":{"id":pid},"body":{"sql":"SELECT count() AS total FROM events","use_cache":False}}})["result"]
        check(not result.get("isError") and int(result["structuredContent"]["body"]["result"]["data"][0]["total"])==len(expected),"restored MCP query matches HTTP")
        check(restored_process.poll() is None,"restored service remains live")
        checks.append("restored live API/MCP, active/revoked credentials, pending replay, tombstones and evicted cold receipt deduplication verified")
        report={"passed":True,"started_at":started_at,"completed_at":datetime.now(timezone.utc).isoformat(),"method":"online SQLite snapshot plus native ClickHouse backup under live projection guard","clickhouse_image":IMAGE,"porcupine_binary_sha256":hashlib.sha256(binary.read_bytes()).hexdigest(),"restored_native_cold_parts":cold_parts,"acknowledged_during_native_backup":during_native,"native_backup_seconds":round(native_end-native_start,3),"concurrent_acknowledged_total":len(accepted),"concurrent_checkpoint_cut":len(cut),"restored_visible_events":len(expected),"native_original_event_id":native_event_id,"metadata_fingerprint_columns":["source_id","message_id","event_type","context"],"checkpoint_tables":checkpoint_state,"checks":checks,"production_connected":False}
    report["disposable_resources_cleaned"]=True
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output",type=Path)
    args=parser.parse_args()
    try:
        report=run()
    except DrillFailure as error:
        report={"passed":False,"failed_check":str(error),"production_connected":False}
    except Exception as error:
        location = ",".join(str(frame.lineno) for frame in traceback.extract_tb(error.__traceback__) if Path(frame.filename).name == Path(__file__).name)
        report={"passed":False,"failed_check":"isolated checker " + type(error).__name__ + " at line " + str(location),"production_connected":False}
    text=json.dumps(report,indent=2)+"\n"
    if args.output:
        args.output.parent.mkdir(parents=True,exist_ok=True)
        descriptor=os.open(args.output,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
        os.fchmod(descriptor,0o600)
        with os.fdopen(descriptor,"w") as output:output.write(text)
    print(text,end="")
    return 0 if report["passed"] else 1


if __name__=="__main__":
    sys.exit(main())
