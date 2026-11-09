"""Failure-boundary tests for the operator tool; no Docker or running service."""
import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

import online_backup as backup


class NativeFailure:
    user = "operator"
    endpoint = "http://localhost:8123"
    timeout = 1

    def __init__(self, table_uuid="source-table"):
        self.table_uuid, self.backup_started, self.canceled = table_uuid, False, False

    def query(self, sql, query_id=None):
        if "FROM system.row_policies" in sql:
            return []
        if "FROM system.columns" in sql:
            return []
        if "FROM system.tables" in sql:
            return [{"name": "events_v2", "uuid": self.table_uuid, "engine": "ReplacingMergeTree", "create_table_query": "events definition"},
                    {"name": "schema_state", "uuid": "schema-table", "engine": "ReplacingMergeTree", "create_table_query": "schema definition"}]
        if sql.startswith("SELECT count()"):
            return [{"rows": "0", "city": "0", "sip": "0", "max_event_id": "0", "max_revision": "0"}]
        if "FROM example.schema_state" in sql:
            return []
        if sql.startswith("BACKUP"):
            self.backup_started = True
            raise RuntimeError("injected native failure")
        raise AssertionError("unexpected operator SQL")

    def cancel(self, query_id):
        self.canceled = True


class OnlineBackupTests(unittest.TestCase):
    def test_identity_projection_is_included_in_checkpoint_hash_when_present(self):
        for present in (False, True):
            with self.subTest(present=present):
                native = NativeFailure()
                statements = []

                def query(sql):
                    statements.append(sql)
                    if "FROM system.columns" in sql and present:
                        return [{"name": "identity_snapshot", "type": "String"}]
                    return NativeFailure.query(native, sql)

                native.query = query
                backup.logical_summary(native, "example")
                digest_sql = next(sql for sql in statements if "cityHash64(winner)" in sql)
                self.assertEqual("original_snapshot,identity_snapshot)" in digest_sql, present)

    def fixture(self, directory):
        db = Path(directory).resolve() / "source.db"
        with sqlite3.connect(db) as connection:
            connection.execute("CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT)")
            connection.execute("CREATE TABLE events(id INTEGER PRIMARY KEY AUTOINCREMENT)")
            # No receipt_source_* keys: pairing must also protect permanent mode.
            connection.executemany("INSERT INTO metadata VALUES(?,?)", [
                ("online_backup_guard", "flock-v1"), ("online_backup_lock_path", str(db) + ".projection.lock"),
                ("online_backup_source_uuid", "source-table"), ("online_backup_source_database", "example")])
        holder = open(str(db) + ".writer.lock", "w")
        fcntl.flock(holder, fcntl.LOCK_SH)
        self.addCleanup(holder.close)
        return argparse.Namespace(db=str(db), output=str(db.parent / "bundle"), database="example", holder=holder,
                                  backup_disk="backups", clickhouse_url="http://localhost:8123", timeout=1, lock_timeout=0)

    def test_wrong_table_rejected_without_bounded_receipts(self):
        with tempfile.TemporaryDirectory() as directory:
            arguments = self.fixture(directory)
            native = NativeFailure("unrelated-healthy-table")
            with patch.object(backup, "ClickHouse", return_value=native), self.assertRaisesRegex(RuntimeError, "attested"):
                backup.backup(arguments)
            self.assertFalse(native.backup_started)
            self.assertFalse((Path(arguments.output) / "manifest.json").exists())

    def test_native_failure_keeps_incomplete_bundle_and_releases_guard(self):
        with tempfile.TemporaryDirectory() as directory:
            arguments = self.fixture(directory)
            native = NativeFailure()
            with patch.object(backup, "ClickHouse", return_value=native), self.assertRaisesRegex(RuntimeError, "injected"):
                backup.backup(arguments)
            self.assertTrue(native.backup_started and native.canceled)
            self.assertTrue((Path(arguments.output) / "INCOMPLETE.json").exists())
            self.assertFalse((Path(arguments.output) / "manifest.json").exists())
            with open(arguments.db + ".projection.lock", "r+") as probe:
                fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def test_stale_metadata_without_live_guard_holder_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            arguments = self.fixture(directory)
            arguments.holder.close()
            with self.assertRaisesRegex(RuntimeError, "active compatible"):
                backup.backup(arguments)

    def test_rebind_preserves_fallback_and_normalizes_daemon_endpoint(self):
        with sqlite3.connect(":memory:") as connection:
            connection.executescript("CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT); CREATE TABLE receipt_provenance(source_uuid TEXT); CREATE TABLE receipt_lookup_leases(lease_id TEXT);")
            connection.executemany("INSERT INTO metadata VALUES(?,?)", [("receipt_source_uuid", "old"), ("receipt_generation", "5"), ("receipt_fallback_required", "1")])
            connection.execute("INSERT INTO receipt_provenance VALUES('old')")
            connection.execute("INSERT INTO receipt_lookup_leases VALUES('interrupted')")
            backup.rebind_receipt_source(connection, {"database": "example", "table_identity": {"events_v2": {"uuid": "old"}}},
                                         {"events_v2": {"uuid": "restored"}}, "http://daemon:8123/", "daemon_user")
            metadata = dict(connection.execute("SELECT key,value FROM metadata"))
            expected = hashlib.sha256(b"http://daemon:8123\0example\0daemon_user").hexdigest()
            self.assertEqual(metadata["receipt_source_config"], expected)
            self.assertEqual(metadata["receipt_fallback_required"], "1")
            self.assertEqual(metadata["receipt_generation"], "6")
            self.assertEqual(connection.execute("SELECT source_uuid FROM receipt_provenance").fetchall(), [("restored",)])
            self.assertEqual(connection.execute("SELECT count(*) FROM receipt_lookup_leases").fetchone()[0], 0)

    def test_default_permanent_mode_does_not_gain_a_receipt_archive_dependency(self):
        with sqlite3.connect(":memory:") as connection:
            connection.execute("CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT)")
            connection.execute("INSERT INTO metadata VALUES('receipt_generation','0')")
            backup.rebind_receipt_source(connection, {"database": "example", "table_identity": {"events_v2": {"uuid": "old"}}},
                                         {"events_v2": {"uuid": "restored"}}, "http://daemon:8123", "daemon_user")
            metadata = dict(connection.execute("SELECT key,value FROM metadata"))
            self.assertNotIn("receipt_source_config", metadata)
            self.assertNotIn("receipt_source_uuid", metadata)
            self.assertEqual(metadata["receipt_generation"], "0")
            self.assertIn("online_backup_source_config", metadata)

    def test_wal_restore_publishes_all_security_changes_for_another_process(self):
        self.assert_restore_security_state(with_pipelines=False)

    def test_restore_pauses_all_pipeline_destinations_before_publication(self):
        self.assert_restore_security_state(with_pipelines=True)

    def assert_restore_security_state(self, with_pipelines):
        class RestoreStorage(NativeFailure):
            def query(self, sql, query_id=None):
                if "FROM system.databases" in sql:
                    return []
                if sql.startswith("RESTORE"):
                    return [{"status": "RESTORED"}]
                return super().query(sql, query_id)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            bundle = root / "bundle"; bundle.mkdir()
            original = sqlite3.connect(root / "live.db")
            self.addCleanup(original.close)
            original.execute("PRAGMA journal_mode=WAL")
            original.execute("PRAGMA wal_autocheckpoint=0")
            original.executescript("""CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT);
CREATE TABLE access_keys(id TEXT,permissions TEXT,revoked_at TEXT);
INSERT INTO access_keys VALUES('admin','["instance:admin"]',NULL),('victim','["events:read"]',NULL);
CREATE TABLE oauth_tokens(token_hash TEXT); INSERT INTO oauth_tokens VALUES('synthetic-hash');
CREATE TABLE receipt_provenance(source_uuid TEXT); INSERT INTO receipt_provenance VALUES('old');
CREATE TABLE receipt_lookup_leases(lease_id TEXT); INSERT INTO receipt_lookup_leases VALUES('old-request');
CREATE TABLE export_files(id TEXT); INSERT INTO export_files VALUES('external-object-reference');
CREATE TABLE export_jobs(state TEXT,cancel_requested INTEGER,cleanup_pending INTEGER,sql TEXT,parameters TEXT,completed_at TEXT,error TEXT);
INSERT INTO export_jobs VALUES('succeeded',0,0,'private query','{}','','');""")
            if with_pipelines:
                original.executescript("""CREATE TABLE pipelines(id TEXT PRIMARY KEY,state TEXT NOT NULL,secret BLOB NOT NULL);
INSERT INTO pipelines VALUES('webhook','active',x'010203'),('s3','active',x'040506'),('already-paused','paused',x'070809');
CREATE TABLE pipeline_runs(id TEXT,pipeline_id TEXT,state TEXT);
INSERT INTO pipeline_runs VALUES('running-run','webhook','running'),('pending-run','s3','pending');
INSERT INTO metadata VALUES('pipeline_key_fingerprint','externally-managed-key');""")
            original.executemany("INSERT INTO metadata VALUES(?,?)", [("initialized", "1"), ("receipt_source_uuid", "old"), ("receipt_source_config", "old-config"), ("receipt_fallback_required", "1"), ("receipt_generation", "5")])
            original.commit()
            backup.sqlite_snapshot(root / "live.db", bundle / "sqlite.db")
            native = RestoreStorage("restored")
            identity = backup.table_identity(native, "example")
            identity["events_v2"]["uuid"] = "old"
            manifest = {"format_version": 1, "guard": "flock-v1", "database": "example", "native_backup": {"disk": "backups", "name": "pc_" + "a" * 32 + ".zip"},
                        "sqlite": {"file": "sqlite.db", "sha256": backup.file_hash(bundle / "sqlite.db")}, "table_identity": identity,
                        "logical_summary": {"events": {"rows": "0", "city": "0", "sip": "0", "max_event_id": "0", "max_revision": "0"}, "schema_state": []}}
            (bundle / "manifest.json").write_text(json.dumps(manifest))
            target = root / "restored.db"
            arguments = argparse.Namespace(db=str(target), bundle=str(bundle), clickhouse_url=native.endpoint, timeout=1, lock_timeout=0,
                                           acknowledge_credential_rollback=True, preserve_access_tokens=False, revoke_principal=["victim"],
                                           service_clickhouse_url=None, service_clickhouse_user=None)
            with patch.object(backup, "ClickHouse", return_value=native):
                backup.restore(arguments)
            code = """import pathlib,sqlite3,sys
p=pathlib.Path(sys.argv[1])
c=sqlite3.connect('file:'+str(p)+'?mode=ro',uri=True)
m=dict(c.execute('SELECT key,value FROM metadata'))
assert m['receipt_source_uuid']=='restored' and m['receipt_generation']=='6' and m['receipt_fallback_required']=='1'
assert c.execute('SELECT count(*) FROM oauth_tokens').fetchone()[0]==0
assert c.execute(\"SELECT revoked_at FROM access_keys WHERE id='victim'\").fetchone()[0]
assert c.execute('SELECT count(*) FROM export_files').fetchone()[0]==0
assert c.execute('SELECT state,sql,parameters FROM export_jobs').fetchone()==('canceled','','{}')
if sys.argv[2]=='pipelines':
 assert c.execute('SELECT id,state,hex(secret) FROM pipelines ORDER BY id').fetchall()==[('already-paused','paused','070809'),('s3','paused','040506'),('webhook','paused','010203')]
 assert c.execute('SELECT state FROM pipeline_runs ORDER BY id').fetchall()==[('pending',),('running',)]
 assert m['pipeline_key_fingerprint']=='externally-managed-key'
else:
 assert not c.execute(\"SELECT 1 FROM sqlite_master WHERE name='pipelines'\").fetchone()
c.close()
assert not pathlib.Path(str(p)+'.restore-incomplete').exists()
assert not pathlib.Path(str(p)+'-wal').exists()
"""
            subprocess.run([sys.executable, "-c", code, str(target), "pipelines" if with_pipelines else "legacy"], check=True)

    def test_unknown_pipeline_schema_fails_closed(self):
        with sqlite3.connect(":memory:") as connection:
            connection.execute("CREATE TABLE pipelines(id TEXT)")
            with self.assertRaisesRegex(RuntimeError, "unsupported pipeline schema"):
                backup.pause_restored_pipelines(connection)

    def test_restore_rechecks_target_after_waiting_for_exclusive_locks(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve(); bundle = root / "bundle"; bundle.mkdir()
            snapshot = sqlite3.connect(bundle / "sqlite.db"); snapshot.execute("CREATE TABLE metadata(key TEXT,value TEXT)"); snapshot.close()
            manifest = {"format_version": 1, "guard": "flock-v1", "database": "example", "native_backup": {"disk": "backups", "name": "pc_" + "b" * 32 + ".zip"},
                        "sqlite": {"file": "sqlite.db", "sha256": backup.file_hash(bundle / "sqlite.db")}}
            (bundle / "manifest.json").write_text(json.dumps(manifest))
            target = root / "raced.db"
            arguments = argparse.Namespace(db=str(target), bundle=str(bundle), clickhouse_url="http://localhost:9", timeout=2, lock_timeout=2,
                                           acknowledge_credential_rollback=True)
            waiting = threading.Event(); failures = []
            original_lock = backup.lock

            @contextmanager
            def observed_lock(path, exclusive, timeout):
                if str(path).endswith(".writer.lock"):
                    waiting.set()
                with original_lock(path, exclusive, timeout):
                    yield

            def restore_waiter():
                try:
                    backup.restore(arguments)
                except Exception as error:
                    failures.append(error)

            with open(str(target) + ".writer.lock", "w") as holder:
                fcntl.flock(holder, fcntl.LOCK_EX)
                with patch.object(backup, "lock", observed_lock):
                    worker = threading.Thread(target=restore_waiter)
                    worker.start()
                    self.assertTrue(waiting.wait(timeout=2))
                    target.write_bytes(b"concurrent restore must survive")
                    fcntl.flock(holder, fcntl.LOCK_UN)
                    worker.join(timeout=3)
            self.assertFalse(worker.is_alive())
            self.assertEqual(len(failures), 1)
            self.assertIn("target changed", str(failures[0]))
            self.assertEqual(target.read_bytes(), b"concurrent restore must survive")


if __name__ == "__main__":
    unittest.main()
