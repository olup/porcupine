#!/usr/bin/env python3
"""Run isolated HTTP checks against the real binary; never prints credentials."""
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
with tempfile.TemporaryDirectory(prefix="porcupine-smoke-") as temporary:
    binary = Path(temporary) / "porcupine"
    subprocess.run(["go", "build", "-o", str(binary), "./cmd/porcupine"], cwd=ROOT, check=True)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    base = f"http://127.0.0.1:{port}"
    token = secrets.token_urlsafe(32)
    environment = {**os.environ, "PORCUPINE_BOOTSTRAP_TOKEN": token,
                   "PORCUPINE_ADDR": f"127.0.0.1:{port}",
                   "PORCUPINE_DB": str(Path(temporary) / "events.db")}
    process = None

    def request(method, path, data=None, key=None):
        headers = {}
        if key:
            headers["Authorization"] = "Bearer " + key
        if data is not None:
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(base + path, method=method, headers=headers,
                                     data=json.dumps(data).encode() if data is not None else None)
        try:
            with urllib.request.urlopen(req, timeout=5) as response:
                raw = response.read()
                return response.status, json.loads(raw) if raw else None
        except urllib.error.HTTPError as error:
            return error.code, None

    def start():
        child = subprocess.Popen([str(binary)], env=environment, stdout=subprocess.DEVNULL,
                                 stderr=subprocess.DEVNULL)
        for _ in range(100):
            if child.poll() is not None:
                raise RuntimeError("Server exited during startup")
            try:
                if request("GET", "/readyz")[0] == 200:
                    return child
            except OSError:
                pass
            time.sleep(0.05)
        child.terminate()
        child.wait(timeout=10)
        raise RuntimeError("Server did not become ready")

    try:
        process = start()
        assert request("GET", "/api/v1/projects")[0] == 401
        status, contract = request("GET", "/openapi.json")
        assert status == 200 and "/api/v1/access-keys" in contract["paths"]
        assert request("GET", "/docs")[0] == 404
        status, listing = request("GET", "/api/v1/access-keys", key=token)
        assert status == 200 and len(listing["access_keys"]) == 1
        bootstrap_id = listing["access_keys"][0]["id"]
        assert request("DELETE", "/api/v1/access-keys/" + bootstrap_id, key=token)[0] == 409
        status, created = request("POST", "/api/v1/projects", {"name": "Smoke"}, token)
        assert status == 201, status
        project = created["project"]["id"]
        ingest = created["ingest_token"]
        event = {"api_key": ingest, "uuid": "smoke-stable-id", "event": "smoke",
                 "distinct_id": "synthetic-visitor", "properties": {"synthetic": True}}
        assert request("POST", "/capture/", event)[0] == 200
        assert request("POST", "/capture/", event)[0] == 200
        path = f"/api/v1/projects/{project}/events"
        status, result = request("GET", path, key=token)
        assert status == 200 and len(result["events"]) == 1
        assert request("GET", path, key=ingest)[0] == 403
        status, reader = request("POST", "/api/v1/access-keys", {
            "name": "Reader", "permissions": ["events:read"], "projects": [project]}, token)
        assert status == 201
        assert request("GET", path, key=reader["token"])[0] == 200
        _, other = request("POST", "/api/v1/projects", {"name": "Other"}, token)
        assert request("GET", f'/api/v1/projects/{other["project"]["id"]}/events',
                       key=reader["token"])[0] in (403, 404)
        assert request("DELETE", "/api/v1/access-keys/" + reader["id"], key=token)[0] == 204
        assert request("GET", path, key=reader["token"])[0] == 401
        process.kill()
        process.wait(timeout=10)
        process = start()
        status, result = request("GET", path, key=token)
        assert status == 200 and len(result["events"]) == 1
        assert result["events"][0]["properties"]["synthetic"] is True
        status, listing = request("GET", "/api/v1/access-keys", key=token)
        assert status == 200
        assert any(k["id"] == reader["id"] and k["revoked_at"] is not None
                   for k in listing["access_keys"])
        assert all("token" not in k and "token_hash" not in k for k in listing["access_keys"])
        print("PASS: real HTTP contracts, ingestion, deduplication, permissions, key lifecycle and restart persistence")
    finally:
        if process and process.poll() is None:
            process.terminate()
            process.wait(timeout=10)
