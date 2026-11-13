"""Build the standalone CLI and exercise it against an isolated local Porcupine."""
import contextlib
import hashlib
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

from credentials import Credentials


ROOT = Path(__file__).resolve().parents[1]


@contextlib.contextmanager
def local_server(directory, bootstrap):
    server = directory / "server"
    subprocess.run(["go", "build", "-o", server, "./cmd/porcupine"], cwd=ROOT, check=True)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        address = f"127.0.0.1:{sock.getsockname()[1]}"
    origin = "http://" + address
    env = {"PORCUPINE_ADDR": address, "PORCUPINE_DB": str(directory / "test.db"),
           "PORCUPINE_BOOTSTRAP_TOKEN": bootstrap, "PORCUPINE_OAUTH_ISSUER": origin}
    with subprocess.Popen([server], cwd=directory, env=env) as process:
        try:
            deadline = time.monotonic() + 10
            while True:
                if process.poll() is not None or time.monotonic() > deadline:
                    raise RuntimeError("test server did not start")
                try:
                    with urllib.request.urlopen(origin + "/readyz", timeout=1):
                        break
                except urllib.error.URLError:
                    time.sleep(0.05)
            yield origin
        finally:
            process.terminate()
            try:
                process.wait(timeout=12)
            except subprocess.TimeoutExpired:
                process.kill()


def check(directory, binary, origin, bootstrap):
    credentials = Credentials({}, {"issuer": origin, "admin": {
        "client_id": "bootstrap", "client_secret": bootstrap}})
    admin = credentials["admin_token"]

    def setup(method, path, data=None):
        request = urllib.request.Request(origin + "/api/v1" + path, method=method,
            data=json.dumps(data).encode() if data is not None else None,
            headers={"Authorization": "Bearer " + admin, "Content-Type": "application/json"})
        with credentials.open_api(request, timeout=5) as response:
            return json.load(response) if response.status != 204 else None

    def service_key(name, projects, permissions):
        grant = {"name": name, "projects": projects, "permissions": permissions}
        account = setup("POST", "/oauth-clients", grant)
        return setup("POST", "/oauth-clients/" + account["client_id"] + "/api-keys",
                     dict(grant, audience="api"))

    def cli(*args, key=None, stdin=None, denied=None, message=None):
        # Only the copied CLI is on PATH. No local server credentials, plugin or spec.
        env = {"PATH": str(binary.parent), "XDG_CONFIG_HOME": str(directory / "config"),
               "PORCUPINE_URL": origin, "NO_COLOR": "1"}
        if key:
            env["PORCUPINE_API_KEY"] = key
        result = subprocess.run([binary, *args], cwd=binary.parent, env=env, input=stdin,
                                capture_output=True, timeout=10)
        for secret in (key, admin, bootstrap):
            if secret and secret.encode() in result.stdout + result.stderr:
                raise AssertionError("CLI exposed a credential")
        if denied is not None:
            if not result.returncode or str(denied).encode() not in result.stderr:
                raise AssertionError(f"{args[0]} should report HTTP {denied} on stderr")
        elif result.returncode:
            raise AssertionError(f"{args[0]} failed: {result.stderr.decode(errors='replace')}")
        if message is not None:
            assert message.encode() in result.stderr, f"API error message missing from stderr: {result.stderr!r}"
        if denied is not None:
            assert not result.stdout, "error body written to stdout"
        return result.stdout

    assert b"--x-chunk-sha256" in cli("upload-posthog-import-chunk", "--help")
    assert b"login" in cli("login", "--help").lower()
    assert b"ready" in cli("--url", origin, "status")
    admin_key = service_key("project provisioner", ["*"],
                            ["management:read", "management:write", "events:write"])["token"]
    status = json.loads(cli("get-instance-status", key=admin_key))
    assert status["status"] == "ok" and status["checks"] == {
        "sqlite": "ok", "clickhouse": "not_configured", "ducklake": "not_configured"}
    assert not any(status["configuration"].values()), "unexpected optional services"
    cli("create-project", "--body", '{"name":"   "}', key=admin_key, denied=400,
        message="name must contain 1 to 200 UTF-8 bytes")
    cli("create-project", "--body", '{"name":', key=admin_key, denied=400,
        message="validation failed")
    project_file = directory / "project with spaces.json"
    ids = []
    for name in ("CLI private", "CLI other"):
        project_file.write_text(json.dumps({"name": name, "ingestion_mode": "server_only"}))
        project = json.loads(cli("create-project", "--body", "@" + str(project_file),
                                 key=admin_key))["project"]
        assert project["name"] == name
        ids.append(project["id"])
    assert set(ids) == {p["id"] for p in json.loads(cli("list-projects", key=admin_key))["projects"]}

    renamed = json.loads(cli("rename-project", "--id", ids[0], "--body",
                         '{"name":"CLI renamed"}', key=admin_key))["project"]
    assert renamed["name"] == "CLI renamed" and renamed["id"] == ids[0]
    cli("delete-project", "--id", ids[0], key=admin_key, denied=403)

    scoped = service_key("importer", [ids[0]],
                         ["management:read", "events:read", "events:write", "imports:write"])
    key = scoped["token"]
    projects = json.loads(cli("list-projects", key=key))["projects"]
    assert [p["id"] for p in projects] == [ids[0]]
    scoped_status = json.loads(cli("get-instance-status", key=key))
    assert scoped_status["checks"] == status["checks"]
    assert not any(project_id in json.dumps(scoped_status) for project_id in ids)
    cli("get-event-schema", "--id", ids[1], key=key, denied=403)

    data = ('{"uuid":"cli-demo","event":"Café","distinct_id":"cli",'
            '"timestamp":"2020-01-02T03:04:05Z","properties":{}}\n').encode()
    digest = hashlib.sha256(data).hexdigest()
    file = directory / "events.jsonl"
    file.write_bytes(data)
    manifest = {"idempotency_key": "cli-import", "files": [{"kind": "events", "path": file.name,
                "size_bytes": len(data), "sha256": digest}]}
    project_args = ["--id", ids[0]]
    job = json.loads(cli("create-posthog-import", *project_args, "--body", "@-",
                         stdin=json.dumps(manifest).encode(), key=key))
    job_args = [*project_args, "--import-id", job["id"]]
    file_args = [*job_args, "--file-id", job["files"][0]["id"]]
    for source in ("@" + str(file), "@-"):
        uploaded = json.loads(cli("upload-posthog-import-chunk", *file_args,
                              "--upload-offset", "0", "--x-chunk-sha256", digest, "--body", source,
                              stdin=data if source == "@-" else None, key=key))
        assert uploaded["files"][0]["offset_bytes"] == len(data)
        assert uploaded["counts"]["accepted"] == 1
    cli("complete-posthog-import-file", *file_args, key=key)
    finished = json.loads(cli("complete-posthog-import", *job_args, key=key))
    assert finished["state"] == "completed"
    setup("DELETE", "/access-keys/" + scoped["id"])
    cli("list-projects", key=key, denied=401)
    cli("get-instance-status", key=key, denied=401)
    cli("delete-project", "--id", ids[0], key=admin)
    cli("delete-project", "--id", ids[0], key=admin)
    remaining = json.loads(cli("list-projects", key=admin_key))["projects"]
    assert [p["id"] for p in remaining] == [ids[1]]


def main():
    with tempfile.TemporaryDirectory(prefix="porcupine-cli-check-") as tmp:
        directory = Path(tmp)
        binary = directory / "bin" / "porcupine"
        binary.parent.mkdir()
        subprocess.run(["go", "build", "-o", binary, "./cmd/porcupine"], cwd=ROOT / "cli",
                       check=True, env={**os.environ, "CGO_ENABLED": "0"})
        bootstrap = secrets.token_urlsafe(32)
        with local_server(directory, bootstrap) as origin:
            check(directory, binary, origin, bootstrap)
    print("Standalone CLI passed readiness and instance status, API errors, project rename and deletion, isolation, binary upload/retry, and key revocation.")


if __name__ == "__main__":
    main()
