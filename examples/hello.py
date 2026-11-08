"""Send a cookieless event and query it on a local quickstart instance."""
import base64
import json
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

config = dict(line.split("=", 1) for line in Path(".env").read_text().splitlines())
origin = config["PORCUPINE_OAUTH_ISSUER"]
if origin != "http://localhost:8080":
    raise SystemExit("Use the local quickstart issuer http://localhost:8080")

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None

client = urllib.request.build_opener(NoRedirect(), urllib.request.ProxyHandler({}))

def request(path, body=None, authorization=None, form=False):
    headers = {}
    if authorization:
        headers["Authorization"] = authorization
    data = None
    if body is not None:
        data = (urllib.parse.urlencode(body) if form else json.dumps(body)).encode()
        headers["Content-Type"] = ("application/x-www-form-urlencoded" if form
                                   else "application/json")
    req = urllib.request.Request(origin + path, data=data, headers=headers)
    with client.open(req, timeout=15) as response:
        return json.load(response)

def wait_for(call, ready):
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        try:
            value = call()
            if ready(value):
                return value
        except (urllib.error.URLError, TimeoutError) as error:
            if isinstance(error, urllib.error.HTTPError) and error.code != 503:
                raise
        time.sleep(1)
    raise SystemExit("Timed out: inspect docker compose logs porcupine clickhouse")

wait_for(lambda: request("/readyz"), lambda value: True)
basic = base64.b64encode(("bootstrap:" + config["PORCUPINE_BOOTSTRAP_TOKEN"]).encode()).decode()
token = request("/oauth/token", {
    "grant_type": "client_credentials", "scope": "instance:admin",
    "resource": origin + "/api/v1",
}, "Basic " + basic, form=True)["access_token"]
auth = "Bearer " + token
created = request("/api/v1/projects", {"name": "Quickstart"}, auth)
project_id = created["project"]["id"]
request("/capture/", {
    "api_key": created["ingest_token"], "uuid": str(uuid.uuid4()),
    "event": "hello", "distinct_id": "$posthog_cookieless",
    "properties": {"source": "quickstart"},
})
result = wait_for(lambda: request("/api/v1/projects/" + project_id + "/query", {
    "sql": "SELECT event, count() AS total FROM events WHERE event = {name:String} GROUP BY event",
    "parameters": {"name": "hello"},
}, auth), lambda value: bool(value["result"]["data"]))
print("Project:", project_id)
print(json.dumps(result["result"]["data"], indent=2))
