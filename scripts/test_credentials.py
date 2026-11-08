"""Local-only contract tests for legacy and OAuth operator credential files."""
import base64
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest import mock
import urllib.error
import urllib.request
import urllib.parse

from credentials import CredentialError, load_credentials
from load import Client, Measurements, credentials_from, ingest, make_batch


@contextmanager
def token_server(callback):
    requests = []
    mutex = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            raw = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            with mutex:
                requests.append({"path": self.path, "authorization": self.headers.get("Authorization"),
                                 "body": raw, "form": urllib.parse.parse_qs(raw.decode())})
                number = len(requests)
            status, headers, body = callback(number, requests[-1])
            self.send_response(status)
            for name, value in headers.items():
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            with mutex:
                requests.append({"path": self.path})
            self.send_response(405)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield "http://127.0.0.1:" + str(server.server_port), requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def success(number, request, lifetime=60):
    return 200, {"Content-Type": "application/json"}, json.dumps({
        "access_token": "transient-token-" + str(number), "token_type": "Bearer",
        "expires_in": lifetime}).encode()


class CredentialTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "credentials.json"

    def write(self, issuer=None, mode=0o600, **changes):
        values = {"project_id": "project-a", "ingest_token": "public-ingestion"}
        if issuer:
            values["oauth"] = {"issuer": issuer,
                "admin": {"client_id": "admin+client", "client_secret": "admin secret:+"},
                "read": {"client_id": "read-client", "client_secret": "read-secret"}}
        else:
            values.update(admin_token="legacy-admin", read_token="legacy-read")
        values.update(changes)
        self.path.write_text(json.dumps(values))
        self.path.chmod(mode)
        return values

    def test_legacy_and_audience_aliases(self):
        self.write(mode=0o644)
        keys = load_credentials(self.path)
        self.assertEqual(keys["admin_token"], "legacy-admin")
        self.assertEqual(keys["mcp_admin_token"], "legacy-admin")
        self.assertEqual(keys.get("mcp_read_token"), "legacy-read")
        self.assertEqual(keys.get("missing", "fallback"), "fallback")
        self.assertNotIn("legacy-admin", repr(keys))
        with self.assertRaises(ValueError):
            credentials_from(self.path)
        self.path.chmod(0o600)
        self.assertEqual(credentials_from(self.path)["read_token"], "legacy-read")
        values = self.write()
        del values["admin_token"]
        self.path.write_text(json.dumps(values))
        self.assertEqual(load_credentials(self.path)["read_token"], "legacy-read")
        self.assertEqual(credentials_from(self.path)["read_token"], "legacy-read")

    def test_oauth_basic_resources_thread_cache_and_no_persistence(self):
        with token_server(success) as (issuer, requests):
            values = self.write(issuer)
            before = self.path.read_bytes()
            keys = load_credentials(self.path)
            for _ in range(100):
                self.assertEqual(keys["ingest_token"], "public-ingestion")
                self.assertEqual(keys["project_id"], "project-a")
            self.assertIn("admin_token", keys)
            self.assertEqual(requests, [])
            with ThreadPoolExecutor(max_workers=16) as pool:
                read_tokens = list(pool.map(lambda _: keys["read_token"], range(32)))
            self.assertEqual(len(set(read_tokens)), 1)
            self.assertEqual(len(requests), 1)
            api_read = keys["read_token"]
            mcp_read = keys["mcp_read_token"]
            api_admin = keys["admin_token"]
            mcp_admin = keys["mcp_admin_token"]
            self.assertEqual(len({api_read, mcp_read, api_admin, mcp_admin}), 4)
            self.assertEqual(len(requests), 4)
            for request, role, suffix in zip(requests,
                    ("read", "read", "admin", "admin"),
                    ("/api/v1", "/mcp", "/api/v1", "/mcp")):
                self.assertEqual(request["path"], "/oauth/token")
                self.assertEqual(request["form"], {"grant_type": ["client_credentials"],
                                                "resource": [issuer + suffix]})
                kind, encoded = request["authorization"].split(" ", 1)
                self.assertEqual(kind, "Basic")
                client_id, secret = base64.b64decode(encoded).decode().split(":", 1)
                self.assertEqual(urllib.parse.unquote_plus(client_id), values["oauth"][role]["client_id"])
                self.assertEqual(urllib.parse.unquote_plus(secret), values["oauth"][role]["client_secret"])
                self.assertNotIn(b"client_secret", request["body"])
            self.assertEqual(self.path.read_bytes(), before)
            self.assertNotIn("transient-token", self.path.read_text())
            self.assertNotIn(values["oauth"]["admin"]["client_secret"], repr(keys))

    def test_refresh_uses_expiry_safety_margin(self):
        with token_server(success) as (issuer, requests):
            self.write(issuer)
            keys = load_credentials(self.path)
            with mock.patch("credentials.time.monotonic", return_value=100):
                first = keys["read_token"]
            with mock.patch("credentials.time.monotonic", return_value=129):
                self.assertEqual(keys["read_token"], first)
            with mock.patch("credentials.time.monotonic", return_value=130):
                self.assertNotEqual(keys["read_token"], first)
            self.assertEqual(len(requests), 2)

    def test_least_privilege_server_only_writer_and_reader(self):
        with token_server(success) as (issuer, requests):
            oauth = {"issuer": issuer,
                     "write": {"client_id": "writer", "client_secret": "writer-secret"},
                     "read": {"client_id": "reader", "client_secret": "reader-secret"}}
            self.path.write_text(json.dumps({"project_id": "project-a", "oauth": oauth}))
            self.path.chmod(0o600)
            keys = credentials_from(self.path)
            self.assertNotIn("admin_token", keys)
            self.assertNotIn("ingest_token", keys)
            self.assertIn("write_token", keys)
            with self.assertRaises(KeyError):
                keys["admin_token"]
            client = Client(issuer, keys, 1)
            self.assertEqual(client.ingestion_auth, "oauth")
            body = json.loads(make_batch(keys, "test-run", 1, 1, 2, 512))
            self.assertEqual(set(body), {"batch"})
            self.assertEqual(len(body["batch"]), 2)
            self.assertNotIn("client_secret", json.dumps(body))
            self.assertEqual(requests, [])
            self.assertEqual(keys["write_token"], "transient-token-1")
            self.assertEqual(keys["read_token"], "transient-token-2")
            self.assertEqual(keys.token_exchanges(), {"write/api/v1": 1, "read/api/v1": 1})
            with self.assertRaisesRegex(ValueError, "public ingestion requires"):
                credentials_from(self.path, ingestion_auth="public")

    def test_load_origin_is_pinned_before_any_exchange(self):
        with token_server(success) as (issuer, requests):
            with token_server(success) as (other, other_requests):
                self.write(issuer)
                keys = load_credentials(self.path)
                for destination in (other, issuer + "/path", issuer + "?query=1"):
                    with self.subTest(destination=destination), self.assertRaises(CredentialError):
                        Client(destination, keys, 1)
                self.assertEqual(requests, [])
                self.assertEqual(other_requests, [])

    def test_load_records_failed_oauth_exchanges(self):
        with token_server(lambda n, req: (401, {"Content-Type": "application/json"}, b"private-secret")) as (issuer, _):
            self.write(issuer, oauth={"issuer": issuer,
                "read": {"client_id": "reader", "client_secret": "read-secret"},
                "write": {"client_id": "writer", "client_secret": "write-secret"}})
            client = Client(issuer, credentials_from(self.path), 1)
            measurements = Measurements()
            self.assertEqual(client.count("run", measurements), (None, None))
            self.assertFalse(ingest(client, measurements, b'{"batch":[]}', 1, "ingest"))
            for row in measurements.report().values():
                self.assertEqual(row["errors"], 1)
                self.assertEqual(row["error_types"], {"oauth_token_exchange": 1})
                self.assertNotIn("private-secret", json.dumps(row))

    def test_oauth_requires_private_mode_and_pinned_issuer(self):
        self.write("http://localhost", mode=0o644)
        if os.name == "posix":
            with self.assertRaisesRegex(CredentialError, "mode 600"):
                load_credentials(self.path)
        for issuer in ("http://example.com", "https://example.com/path", "https://example.com?q=x",
                       "https://example.com?", "https://example.com#", "https://user:secret@example.com",
                       "https://example.com\\bad", " https://example.com", "http://127.0.0.1.evil.test"):
            self.write(issuer)
            with self.subTest(issuer=issuer), self.assertRaises(CredentialError):
                load_credentials(self.path)
        for issuer in ("https://example.com", "http://localhost", "http://127.0.0.1", "http://[::1]"):
            self.write(issuer)
            self.assertIn("admin_token", load_credentials(self.path))

    def test_redirect_never_forwards_basic_secret(self):
        with token_server(success) as (target, target_requests):
            def redirect(number, request):
                return 302, {"Location": target + "/oauth/token"}, b""
            with token_server(redirect) as (issuer, requests):
                self.write(issuer)
                with self.assertRaisesRegex(CredentialError, "OAuth token exchange failed"):
                    load_credentials(self.path)["admin_token"]
                self.assertEqual(len(requests), 1)
                self.assertEqual(target_requests, [])

    def test_resource_bearer_never_follows_redirects_or_crosses_issuer(self):
        with token_server(success) as (other, observed):
            def redirect(number, request):
                return 302, {"Location": other + "/capture"}, b""
            with token_server(redirect) as (issuer, _):
                self.write(issuer)
                keys = load_credentials(self.path)
                def request(origin):
                    return urllib.request.Request(origin + "/api/v1/projects", data=b"{}",
                        headers={"Authorization": "Bearer test-resource-token"})
                with self.assertRaises(CredentialError):
                    keys.open_api(request(other), timeout=2)
                with self.assertRaises(urllib.error.HTTPError) as caught:
                    keys.open_api(request(issuer), timeout=2)
                self.assertEqual(caught.exception.code, 302)
                caught.exception.close()
                self.assertEqual(observed, [])

    def test_bad_or_large_token_responses_are_sanitized(self):
        variants = [
            b"private-secret-not-JSON", b"[]", b"{}",
            b'{"access_token":"a","access_token":"b","token_type":"Bearer","expires_in":60}',
            b'{"access_token":"a","token_type":"Other","expires_in":60}',
            b'{"access_token":"a","token_type":"Bearer","expires_in":true}',
            b'{"access_token":"a","token_type":"Bearer","expires_in":0}',
            b'{"access_token":"a","token_type":"Bearer","expires_in":"60"}',
            b'{"access_token":"a\\r\\nprivate-secret","token_type":"Bearer","expires_in":60}',
            b"x" * ((32 << 10) + 1),
        ]
        for response in variants:
            with self.subTest(size=len(response)):
                with token_server(lambda n, req: (200, {"Content-Type": "application/json"}, response)) as (issuer, _):
                    self.write(issuer)
                    with self.assertRaises(CredentialError) as caught:
                        load_credentials(self.path)["read_token"]
                    self.assertEqual(str(caught.exception), "OAuth token exchange failed")
                    self.assertNotIn("private-secret", str(caught.exception))
        with token_server(lambda n, req: (401, {"Content-Type": "application/json"}, b"private-secret")) as (issuer, _):
            self.write(issuer)
            with self.assertRaisesRegex(CredentialError, "OAuth token exchange failed"):
                load_credentials(self.path)["admin_token"]


if __name__ == "__main__":
    unittest.main()
