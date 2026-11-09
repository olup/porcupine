#!/usr/bin/env python3
"""Operator-only, same-host coordinated SQLite and native ClickHouse recovery."""
import argparse
from contextlib import closing, contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")
ARCHIVE = re.compile(r"^pc_[a-f0-9]{32}\.zip$")
PROJECTION_COLUMNS = "event_id,project_id,uuid,event,distinct_id,properties,timestamp,timestamp_ms,received_at,received_at_ms,revision,is_deleted,original_snapshot"


def progress(event, **values):
    print(json.dumps({"event": event, "at": datetime.now(timezone.utc).isoformat(), **values}), flush=True)


def sync_directory(path):
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def write_json(path, value):
    data = json.dumps(value, sort_keys=True, indent=2).encode() + b"\n"
    temporary = path.with_name(path.name + ".tmp")
    with open(temporary, "xb") as output:
        os.chmod(temporary, 0o600)
        output.write(data)
        output.flush()
        os.fsync(output.fileno())
    os.replace(temporary, path)
    sync_directory(path.parent)


def file_hash(path):
    digest = hashlib.sha256()
    with open(path, "rb") as source:
        for block in iter(lambda: source.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


@contextmanager
def lock(path, exclusive, timeout):
    descriptor = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    deadline = time.monotonic() + timeout
    try:
        while True:
            try:
                fcntl.flock(descriptor, (fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH) | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise RuntimeError("timed out waiting for the local checkpoint lock") from None
                time.sleep(0.02)
        yield
    finally:
        os.close(descriptor)


class ClickHouse:
    def __init__(self, endpoint, timeout):
        parsed = urllib.parse.urlsplit(endpoint)
        if parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise RuntimeError("invalid ClickHouse URL")
        self.endpoint, self.timeout = endpoint, timeout
        self.user = os.environ.get("PORCUPINE_CLICKHOUSE_USER", "default")
        self.password = os.environ.get("PORCUPINE_CLICKHOUSE_PASSWORD", "")
        self.opener = urllib.request.build_opener(NoRedirect())

    def query(self, sql, query_id=None):
        values = {"wait_end_of_query": "1", "default_format": "JSONEachRow"}
        if query_id:
            values["query_id"] = query_id
        request = urllib.request.Request(self.endpoint + "?" + urllib.parse.urlencode(values), data=sql.encode(), headers={
            "X-ClickHouse-User": self.user, "X-ClickHouse-Key": self.password, "Content-Type": "text/plain"})
        try:
            with self.opener.open(request, timeout=self.timeout) as response:
                data = response.read((4 << 20) + 1)
                if len(data) > 4 << 20 or response.headers.get("X-ClickHouse-Exception-Code", "0") != "0":
                    raise RuntimeError("ClickHouse operation failed")
                return [json.loads(line) for line in data.splitlines() if line.strip()]
        except (OSError, ValueError, urllib.error.HTTPError):
            # Server errors may contain credentials, filesystem paths, and SQL.
            raise RuntimeError("ClickHouse operation failed; inspect the server's operator logs") from None

    def cancel(self, query_id):
        previous = self.timeout
        self.timeout = min(self.timeout, 5)
        try:
            self.query("KILL QUERY WHERE query_id='" + query_id + "' SYNC")
        except RuntimeError:
            pass
        finally:
            self.timeout = previous


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *arguments):
        return None


def literal(value):
    return "unhex('" + value.encode().hex() + "')"


def table_identity(ch, database):
    rows = ch.query("SELECT name,toString(uuid) AS uuid,engine,create_table_query FROM system.tables WHERE database=" + literal(database) + " ORDER BY name")
    tables = {row["name"]: row for row in rows}
    if set(tables) != {"events_v2", "schema_state"} or tables["events_v2"]["engine"] != "ReplacingMergeTree":
        raise RuntimeError("backup supports only the migrated Porcupine event database with events_v2 and schema_state")
    return tables


def full_visibility(ch, database):
    policies = ch.query("SELECT name,is_restrictive,select_filter,apply_to_all,apply_to_list,apply_to_except FROM system.row_policies WHERE database=" + literal(database) + " AND table='events_v2'")
    if not policies:
        return
    if any(int(policy["is_restrictive"]) for policy in policies):
        raise RuntimeError("operator must resolve restrictive event row policies before backup or restore validation")
    if not any(str(policy["select_filter"]).strip() in ("1", "true") and ch.user not in policy["apply_to_except"] and
               (int(policy["apply_to_all"]) or ch.user in policy["apply_to_list"]) for policy in policies):
        raise RuntimeError("operator lacks an explicit full-visibility event row policy")


def logical_summary(ch, database):
    # Include tombstones, which FINAL deliberately hides. Native archive file
    # checksums protect stored bytes; this independent summary checks row meaning.
    identity_column = ch.query("SELECT name,type FROM system.columns WHERE database=" + literal(database) +
                               " AND table='events_v2' AND name='identity_snapshot'")
    columns = PROJECTION_COLUMNS
    if identity_column:
        if identity_column != [{"name": "identity_snapshot", "type": "String"}]:
            raise RuntimeError("unexpected identity snapshot type")
        columns += ",identity_snapshot"
    projected = "(SELECT argMax(tuple(" + columns + "),revision) AS winner FROM " + database + ".events_v2 GROUP BY project_id,uuid)"
    rows = ch.query("SELECT count() AS rows,groupBitXor(cityHash64(winner)) AS city,groupBitXor(sipHash64(winner)) AS sip,max(tupleElement(winner,1)) AS max_event_id,max(tupleElement(winner,11)) AS max_revision FROM " + projected + " SETTINGS final=0,optimize_aggregation_in_order=1,max_memory_usage=536870912,max_bytes_before_external_group_by=134217728,max_temporary_data_on_disk_size_for_query=1073741824,max_execution_time=" + str(ch.timeout))
    if len(rows) != 1:
        raise RuntimeError("cannot validate logical event checkpoint")
    schema = ch.query("SELECT name,legacy_uuid,version FROM " + database + ".schema_state FINAL ORDER BY name")
    return {"events": rows[0], "schema_state": schema}


def sqlite_snapshot(source, destination):
    uri = "file:" + urllib.parse.quote(str(source)) + "?mode=ro"
    with closing(sqlite3.connect(uri, uri=True, timeout=5)) as original, closing(sqlite3.connect(destination)) as snapshot:
        original.backup(snapshot)
        if snapshot.execute("PRAGMA journal_mode=DELETE").fetchone()[0].lower() != "delete":
            raise RuntimeError("cannot make the SQLite snapshot self-contained")
        validate_sqlite(snapshot)
        snapshot.commit()
    os.chmod(destination, 0o600)
    with open(destination, "rb") as snapshot:
        os.fsync(snapshot.fileno())
    sync_directory(destination.parent)


def validate_sqlite(connection):
    if connection.execute("PRAGMA integrity_check").fetchall() != [("ok",)] or connection.execute("PRAGMA foreign_key_check").fetchall():
        raise RuntimeError("SQLite integrity or foreign-key verification failed")


def backup(arguments):
    source = Path(arguments.db).resolve(strict=True)
    output = Path(arguments.output).absolute()
    output.mkdir(mode=0o700, parents=False, exist_ok=False)
    write_json(output / "INCOMPLETE.json", {"operation": "backup", "created_at": datetime.now(timezone.utc).isoformat()})
    database = arguments.database
    if not IDENTIFIER.fullmatch(database) or not IDENTIFIER.fullmatch(arguments.backup_disk):
        raise RuntimeError("invalid ClickHouse database or backup disk")
    ch = ClickHouse(arguments.clickhouse_url, arguments.timeout)
    archive, operation = "pc_" + uuid.uuid4().hex + ".zip", str(uuid.uuid4())
    # An old binary does not hold this lifetime lock. A stale metadata marker
    # alone must not make a downgraded server appear safe for online backup.
    probe = os.open(str(source) + ".writer.lock", os.O_RDWR | os.O_CREAT, 0o600)
    try:
        try:
            fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            pass
        else:
            raise RuntimeError("online backup requires an active compatible application lock holder")
    finally:
        os.close(probe)
    with lock(str(source) + ".writer.lock", False, arguments.lock_timeout), lock(str(source) + ".projection.lock", True, arguments.lock_timeout):
        paused_at = time.monotonic()
        checkpoint_at = datetime.now(timezone.utc).isoformat()
        progress("checkpoint_acquired")
        sqlite_snapshot(source, output / "sqlite.db")
        snapshot_at = datetime.now(timezone.utc).isoformat()
        with closing(sqlite3.connect(output / "sqlite.db")) as snapshot:
            metadata = dict(snapshot.execute("SELECT key,value FROM metadata"))
            watermarks = {name: snapshot.execute("SELECT coalesce((SELECT seq FROM sqlite_sequence WHERE name=?),0)", (name,)).fetchone()[0] for name in ("events", "mutation_audit")}
        if metadata.get("online_backup_guard") != "flock-v1" or metadata.get("online_backup_lock_path") != str(source) + ".projection.lock":
            raise RuntimeError("application does not advertise the matching online backup guard; restart the supported binary first")
        full_visibility(ch, database)
        identity = table_identity(ch, database)
        if metadata.get("online_backup_source_database") != database or metadata.get("online_backup_source_uuid") != identity["events_v2"]["uuid"]:
            raise RuntimeError("ClickHouse does not match the active projection worker's attested SQLite source")
        if metadata.get("receipt_source_uuid") and metadata["receipt_source_uuid"] != identity["events_v2"]["uuid"]:
            raise RuntimeError("ClickHouse table differs from SQLite's pinned receipt source")
        progress("sqlite_snapshot_complete")
        validation_at = time.monotonic()
        progress("checkpoint_validation_started")
        summary = logical_summary(ch, database)
        if int(summary["events"]["max_event_id"]) > watermarks["events"] or int(summary["events"]["max_revision"]) > watermarks["mutation_audit"]:
            raise RuntimeError("ClickHouse history is newer than the SQLite checkpoint counters")
        validation_seconds = time.monotonic() - validation_at
        progress("checkpoint_validation_finished", seconds=round(validation_seconds, 6))
        progress("native_backup_started", operation_id=operation)
        native_at = time.monotonic()
        try:
            result = ch.query("BACKUP DATABASE " + database + " TO Disk('" + arguments.backup_disk + "','" + archive + "') SETTINGS id='" + operation + "',backup_threads=2,max_backup_bandwidth=67108864", operation)
            if len(result) != 1 or result[0].get("status") != "BACKUP_CREATED":
                raise RuntimeError("native backup did not complete")
        except BaseException:
            ch.cancel(operation)
            raise
        native_seconds = time.monotonic() - native_at
        progress("native_backup_finished", operation_id=operation, seconds=round(native_seconds, 6))
        if table_identity(ch, database) != identity:
            raise RuntimeError("ClickHouse checkpoint changed while projection was paused")
        manifest = {"format_version": 1, "completed_at": datetime.now(timezone.utc).isoformat(), "database": database,
                    "checkpoint_acquired_at": checkpoint_at, "sqlite_snapshot_completed_at": snapshot_at, "watermarks": watermarks,
                    "guard": "flock-v1", "native_backup": {"disk": arguments.backup_disk, "name": archive, "id": operation},
                    "sqlite": {"file": "sqlite.db", "sha256": file_hash(output / "sqlite.db")},
                    "table_identity": identity, "logical_summary": summary, "clickhouse_version": ch.query("SELECT version() AS version")[0]["version"],
                    "checkpoint_validation_seconds": validation_seconds, "native_backup_seconds": native_seconds,
                    "exports": "metadata only; external export objects are not part of this checkpoint",
                    "pipelines": "destinations are paused on restore; encryption key is managed separately"}
        write_json(output / "manifest.json", manifest)
        (output / "INCOMPLETE.json").unlink()
        sync_directory(output)
    progress("backup_complete", bundle=str(output), manifest_sha256=file_hash(output / "manifest.json"), projection_pause_seconds=round(time.monotonic() - paused_at, 6))


def restore(arguments):
    bundle = Path(arguments.bundle).resolve(strict=True)
    manifest = json.loads((bundle / "manifest.json").read_bytes())
    if (bundle / "INCOMPLETE.json").exists() or manifest.get("format_version") != 1 or manifest.get("guard") != "flock-v1":
        raise RuntimeError("backup bundle is incomplete or unsupported")
    database, native = manifest["database"], manifest["native_backup"]
    if not IDENTIFIER.fullmatch(database) or not IDENTIFIER.fullmatch(native["disk"]) or not ARCHIVE.fullmatch(native["name"]) or manifest["sqlite"]["file"] != "sqlite.db":
        raise RuntimeError("invalid private backup manifest")
    if file_hash(bundle / "sqlite.db") != manifest["sqlite"]["sha256"]:
        raise RuntimeError("SQLite backup checksum mismatch")
    target = Path(arguments.db).absolute()
    if target.exists() or Path(str(target) + "-wal").exists() or Path(str(target) + "-shm").exists():
        raise RuntimeError("restore requires a new SQLite path without existing WAL files")
    if not arguments.acknowledge_credential_rollback:
        raise RuntimeError("restore requires --acknowledge-credential-rollback; review revocations since the checkpoint before admitting clients")
    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    target = target.resolve()
    marker = Path(str(target) + ".restore-incomplete")
    if marker.exists():
        raise RuntimeError("a prior restore is incomplete; discard the isolated target or finish recovery manually")
    ch = ClickHouse(arguments.clickhouse_url, arguments.timeout)
    with lock(str(target) + ".writer.lock", True, arguments.lock_timeout), lock(str(target) + ".projection.lock", True, arguments.lock_timeout):
        if target.exists() or Path(str(target) + "-wal").exists() or Path(str(target) + "-shm").exists() or marker.exists():
            raise RuntimeError("restore target changed while waiting for its locks")
        if ch.query("SELECT name FROM system.databases WHERE name=" + literal(database)):
            raise RuntimeError("restore requires an absent target ClickHouse database")
        write_json(marker, {"operation": "restore", "manifest_sha256": file_hash(bundle / "manifest.json")})
        working = Path(str(target) + ".restore-working")
        sqlite_snapshot(bundle / "sqlite.db", working)
        with closing(sqlite3.connect(working)) as snapshot:
            snapshot.execute("INSERT INTO metadata(key,value) VALUES('online_restore_incomplete','1') ON CONFLICT(key) DO UPDATE SET value='1'")
            snapshot.commit()
        operation = str(uuid.uuid4())
        progress("native_restore_started", operation_id=operation)
        try:
            result = ch.query("RESTORE DATABASE " + database + " FROM Disk('" + native["disk"] + "','" + native["name"] + "') SETTINGS id='" + operation + "'", operation)
            if len(result) != 1 or result[0].get("status") != "RESTORED":
                raise RuntimeError("native restore did not complete")
        except BaseException:
            ch.cancel(operation)
            raise
        full_visibility(ch, database)
        restored_identity = table_identity(ch, database)
        for name, table in restored_identity.items():
            normalize = lambda sql: re.sub(r"\s+UUID\s+'[a-fA-F0-9-]+'", "", sql)
            if normalize(table["create_table_query"]) != normalize(manifest["table_identity"][name]["create_table_query"]):
                raise RuntimeError("restored ClickHouse table definition differs from the manifest")
        if logical_summary(ch, database) != manifest["logical_summary"]:
            raise RuntimeError("restored ClickHouse logical checkpoint differs from the manifest")
        with closing(sqlite3.connect(working)) as snapshot:
            # The guarded recovery uses a verified archive, never a pre-existing
            # table. The receipt source identity is rebound below when present.
            rebind_receipt_source(snapshot, manifest, restored_identity, arguments.service_clickhouse_url or ch.endpoint, arguments.service_clickhouse_user or ch.user)
            if not arguments.preserve_access_tokens:
                snapshot.execute("DELETE FROM oauth_tokens")
            restored_at = datetime.now(timezone.utc).isoformat()
            for principal in arguments.revoke_principal:
                if not snapshot.execute("SELECT 1 FROM access_keys WHERE id=?", (principal,)).fetchone():
                    raise RuntimeError("requested post-checkpoint revocation principal is absent from the snapshot")
                snapshot.execute("UPDATE access_keys SET revoked_at=coalesce(revoked_at,?) WHERE id=?", (restored_at, principal))
            if not snapshot.execute("SELECT count(*) FROM access_keys WHERE revoked_at IS NULL AND EXISTS(SELECT 1 FROM json_each(permissions) WHERE value='instance:admin')").fetchone()[0]:
                raise RuntimeError("restore would leave no active instance administrator")
            snapshot.execute("DELETE FROM export_files")
            snapshot.execute("UPDATE export_jobs SET state='canceled',cancel_requested=1,cleanup_pending=0,sql='',parameters='{}',completed_at=?,error='operator_restore_reissue_required' WHERE state!='expired'", (restored_at,))
            pipelines_paused = pause_restored_pipelines(snapshot)
            snapshot.execute("DELETE FROM metadata WHERE key='online_restore_incomplete'")
            snapshot.execute("INSERT INTO metadata(key,value) VALUES('online_backup_lock_path',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(target) + ".projection.lock",))
            snapshot.execute("INSERT INTO metadata(key,value) VALUES('online_backup_source_uuid',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (restored_identity["events_v2"]["uuid"],))
            snapshot.execute("INSERT INTO metadata(key,value) VALUES('online_backup_source_database',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (database,))
            validate_sqlite(snapshot)
            snapshot.commit()
        with open(working, "rb") as snapshot:
            os.fsync(snapshot.fileno())
        os.replace(working, target)
        sync_directory(target.parent)
        with closing(sqlite3.connect("file:" + urllib.parse.quote(str(target)) + "?mode=ro", uri=True)) as published:
            validate_sqlite(published)
            if published.execute("SELECT value FROM metadata WHERE key='online_restore_incomplete'").fetchone():
                raise RuntimeError("published SQLite still has an incomplete restore marker")
            if published.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='pipelines'").fetchone() and published.execute("SELECT count(*) FROM pipelines WHERE state IS NULL OR state!='paused'").fetchone()[0]:
                raise RuntimeError("published SQLite contains an unpaused pipeline destination")
        marker.unlink()
        sync_directory(target.parent)
    progress("restore_complete", database=database, sqlite=str(target), access_tokens="preserved" if arguments.preserve_access_tokens else "revoked", exports="reissue_required", pipelines_paused=pipelines_paused)


def pause_restored_pipelines(snapshot):
    """Disable restored egress within the caller's offline publication transaction."""
    found = snapshot.execute("SELECT type FROM sqlite_master WHERE name='pipelines'").fetchone()
    if found is None:
        return 0  # Pre-pipeline snapshots require no schema migration here.
    if found[0] != "table" or "state" not in {column[1] for column in snapshot.execute("PRAGMA table_info(pipelines)")}:
        raise RuntimeError("unsupported pipeline schema; cannot guarantee restored egress is paused")
    return snapshot.execute("UPDATE pipelines SET state='paused'").rowcount


def rebind_receipt_source(snapshot, manifest, restored_identity, service_endpoint, service_user):
    metadata = dict(snapshot.execute("SELECT key,value FROM metadata"))
    source_uuid = manifest["table_identity"]["events_v2"]["uuid"]
    target_uuid = restored_identity["events_v2"]["uuid"]
    receipt_bound = bool(metadata.get("receipt_source_uuid") or metadata.get("receipt_source_config") or metadata.get("receipt_fallback_required") == "1")
    if metadata.get("receipt_source_uuid") and metadata["receipt_source_uuid"] != source_uuid:
        raise RuntimeError("SQLite receipt source differs from the verified manifest")
    if snapshot.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='receipt_provenance'").fetchone()[0]:
        if snapshot.execute("SELECT count(*) FROM receipt_provenance WHERE source_uuid!=?", (source_uuid,)).fetchone()[0]:
            raise RuntimeError("receipt provenance contains an unrelated source")
        snapshot.execute("UPDATE receipt_provenance SET source_uuid=? WHERE source_uuid=?", (target_uuid, source_uuid))
    if snapshot.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='receipt_lookup_leases'").fetchone()[0]:
        snapshot.execute("DELETE FROM receipt_lookup_leases")
    ClickHouse(service_endpoint, 1)  # Validate the daemon's operator-supplied endpoint without connecting.
    canonical_endpoint = urllib.parse.urlsplit(service_endpoint).geturl()
    if canonical_endpoint.endswith("/"):
        canonical_endpoint = canonical_endpoint[:-1]
    config_hash = hashlib.sha256((canonical_endpoint + "\0" + manifest["database"] + "\0" + service_user).encode()).hexdigest()
    if receipt_bound:
        generation = str(int(metadata.get("receipt_generation", "0")) + 1)
        for key, value in (("receipt_source_uuid", target_uuid), ("receipt_source_config", config_hash), ("receipt_generation", generation)):
            snapshot.execute("INSERT INTO metadata(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))
    snapshot.execute("INSERT INTO metadata(key,value) VALUES('online_backup_source_config',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (config_hash,))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("backup", "restore"):
        command = commands.add_parser(name)
        command.add_argument("--db", required=True)
        command.add_argument("--clickhouse-url", required=True)
        command.add_argument("--timeout", type=int, default=300)
        command.add_argument("--lock-timeout", type=int, default=30)
    commands.choices["backup"].add_argument("--database", required=True)
    commands.choices["backup"].add_argument("--backup-disk", default="backups")
    commands.choices["backup"].add_argument("--output", required=True)
    commands.choices["restore"].add_argument("--bundle", required=True)
    commands.choices["restore"].add_argument("--acknowledge-credential-rollback", action="store_true")
    commands.choices["restore"].add_argument("--preserve-access-tokens", action="store_true")
    commands.choices["restore"].add_argument("--service-clickhouse-url", help="Restored daemon's exact ClickHouse URL, if different from the operator connection")
    commands.choices["restore"].add_argument("--service-clickhouse-user", help="Restored daemon's configured ClickHouse user, if different from the operator")
    commands.choices["restore"].add_argument("--revoke-principal", action="append", default=[], help="Replay a post-checkpoint access-key/OAuth-principal revocation; repeat as needed")
    arguments = parser.parse_args()
    if arguments.timeout < 1 or arguments.timeout > 86400 or arguments.lock_timeout < 0 or arguments.lock_timeout > 3600:
        parser.error("timeout must be 1..86400 and lock-timeout 0..3600 seconds")
    os.umask(0o077)
    def interrupt(signum, frame):
        raise TimeoutError("operator backup/restore interrupted or timed out")
    signal.signal(signal.SIGTERM, interrupt)
    signal.signal(signal.SIGALRM, interrupt)
    signal.alarm(arguments.timeout + arguments.lock_timeout)
    try:
        (backup if arguments.command == "backup" else restore)(arguments)
    except (OSError, ValueError, KeyError, RuntimeError, sqlite3.Error) as error:
        print(json.dumps({"event": "operation_failed", "error": str(error)}), file=sys.stderr)
        return 1
    finally:
        signal.alarm(0)
    return 0


if __name__ == "__main__":
    sys.exit(main())
