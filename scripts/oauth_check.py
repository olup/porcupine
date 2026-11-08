#!/usr/bin/env python3
"""Check OAuth API/MCP boundaries on an isolated TEST deployment using synthetic data.

Credentials and short-lived tokens stay in memory. Reports contain check names only,
never response bodies, secrets, access tokens or signed URLs. HTTPS uses system trust;
HTTP is accepted only for a loopback test origin. Redirects are always rejected.
"""
import argparse
import base64
import http.client
import ipaddress
import json
import os
from pathlib import Path
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid

from credentials import CredentialError, load_credentials


class CheckFailure(Exception):
    """A fixed check label, safe for the public report."""


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, newurl):
        return None


def origin(value):
    try:
        if not isinstance(value, str) or value != value.strip() or "\\" in value or any(char.isspace() for char in value):
            raise ValueError
        parsed = urllib.parse.urlsplit(value)
        if (not parsed.hostname or parsed.username is not None or parsed.password is not None
                or parsed.path not in ("", "/") or parsed.query or parsed.fragment
                or "?" in value or "#" in value or parsed.port is not None and not 0 < parsed.port <= 65535):
            raise ValueError
        loopback = parsed.hostname.lower() == "localhost"
        try:
            loopback = loopback or ipaddress.ip_address(parsed.hostname).is_loopback
        except ValueError:
            pass
        if parsed.scheme != "https" and not (parsed.scheme == "http" and loopback):
            raise ValueError
        return value.rstrip("/")
    except (ValueError, TypeError):
        raise CheckFailure("validated test origin") from None


def client_pair(value):
    if not isinstance(value, dict):
        raise CheckFailure("OAuth role credentials")
    pair = []
    for name, maximum in (("client_id", 256), ("client_secret", 4096)):
        text = value.get(name)
        if (not isinstance(text, str) or not text or len(text) > maximum
                or any(ord(char) < 32 or ord(char) == 127 for char in text)):
            raise CheckFailure("OAuth role credentials")
        pair.append(text)
    return pair


def contains_secret_member(value):
    if isinstance(value, dict):
        return bool({"client_secret", "secret_hash", "token_hash", "access_token", "refresh_token"}.intersection(value)) or any(contains_secret_member(item) for item in value.values())
    if isinstance(value, list):
        return any(contains_secret_member(item) for item in value)
    return False


class Checker:
    def __init__(self, root, credentials):
        self.root = origin(root)
        self.credentials = credentials
        self.checks = []
        self.lifetime = None
        self.project_id = credentials["project_id"]
        self.opener = urllib.request.build_opener(
            NoRedirect(), urllib.request.HTTPSHandler(context=ssl.create_default_context()))
        oauth = credentials.get("oauth")
        if not isinstance(oauth, dict) or origin(oauth.get("issuer")) != self.root:
            raise CheckFailure("credentials issuer matches test origin")
        self.oauth = oauth
        for role in ("admin", "read", "write"):
            client_pair(oauth.get(role))

    def check(self, name, condition):
        if not condition:
            raise CheckFailure(name)
        self.checks.append(name)

    def request(self, method, path, body=None, token=None, headers=None, form=None):
        if not path.startswith("/") or path.startswith("//") or "\\" in path:
            raise CheckFailure("request remains on pinned origin")
        hdr = {"Accept": "application/json", **(headers or {})}
        data = None
        if body is not None:
            data = json.dumps(body).encode()
            hdr["Content-Type"] = "application/json"
        if form is not None:
            data = urllib.parse.urlencode(form).encode()
            hdr["Content-Type"] = "application/x-www-form-urlencoded"
        if token is not None:
            hdr["Authorization"] = "Bearer " + token
        request = urllib.request.Request(self.root + path, method=method, data=data, headers=hdr)
        try:
            response = self.opener.open(request, timeout=15)
        except urllib.error.HTTPError as error:
            response = error
        except (OSError, http.client.HTTPException, urllib.error.URLError):
            raise CheckFailure("verified HTTPS request without redirects") from None
        try:
            with response:
                status, response_headers = response.status, response.headers
                raw = response.read((1 << 20) + 1)
        except (OSError, http.client.HTTPException):
            raise CheckFailure("bounded HTTP response") from None
        if 300 <= status < 400:
            raise CheckFailure("redirect rejected")
        if len(raw) > 1 << 20:
            raise CheckFailure("bounded HTTP response")
        try:
            payload = json.loads(raw) if raw else None
        except (ValueError, UnicodeError):
            raise CheckFailure("JSON response encoding") from None
        return status, response_headers, payload

    def exchange(self, pair, suffix="/api/v1", expected=200):
        identifier, secret = client_pair(pair)
        basic = base64.b64encode((urllib.parse.quote_plus(identifier, safe="") + ":" +
                                 urllib.parse.quote_plus(secret, safe="")).encode()).decode("ascii")
        status, headers, body = self.request("POST", "/oauth/token", form={
            "grant_type": "client_credentials", "resource": self.root + suffix},
            headers={"Authorization": "Basic " + basic, "Cache-Control": "no-store"})
        if status != expected:
            raise CheckFailure("client credentials exchange status")
        if expected != 200:
            self.check("public ingestion key cannot obtain OAuth token",
                       isinstance(body, dict) and body.get("error") == "invalid_client")
            return None
        self.check("OAuth client credentials token response",
                   isinstance(body, dict) and isinstance(body.get("access_token"), str)
                   and bool(body["access_token"]) and body.get("token_type", "").lower() == "bearer"
                   and body.get("expires_in") == 900 and not body.get("refresh_token")
                   and "no-store" in headers.get("Cache-Control", ""))
        self.lifetime = body["expires_in"]
        return body["access_token"]

    def mcp(self, method, params, token=None):
        return self.request("POST", "/mcp", {"jsonrpc": "2.0", "id": 1,
            "method": method, "params": params},
            token=token or self.credentials["mcp_read_token"], headers={
                "Accept": "application/json, text/event-stream", "MCP-Protocol-Version": "2025-11-25"})

    def run(self):
        pid = urllib.parse.quote(self.project_id, safe="")
        base = "/api/v1/projects/" + pid
        marker = "oauth-check-" + uuid.uuid4().hex
        status, _, discovery = self.request("GET", "/.well-known/oauth-authorization-server")
        self.check("pinned authorization server discovery", status == 200 and
                   discovery.get("issuer") == self.root and
                   discovery.get("token_endpoint") == self.root + "/oauth/token" and
                   discovery.get("grant_types_supported") == ["client_credentials"] and
                   discovery.get("token_endpoint_auth_methods_supported") == ["client_secret_basic"])
        for suffix in ("/api/v1", "/mcp"):
            status, _, metadata = self.request("GET", "/.well-known/oauth-protected-resource" + suffix)
            self.check("pinned protected resource " + suffix, status == 200 and
                       metadata.get("resource") == self.root + suffix and
                       metadata.get("authorization_servers") == [self.root])
        status, headers, _ = self.request("GET", base + "/schema")
        self.check("anonymous API OAuth resource challenge", status == 401 and
                   self.root + "/.well-known/oauth-protected-resource/api/v1" in headers.get("WWW-Authenticate", ""))
        status, _, document = self.request("GET", "/openapi.json")
        scheme = document.get("components", {}).get("securitySchemes", {}).get("oauth2", {})
        flow = scheme.get("flows", {}).get("clientCredentials", {})
        self.check("OpenAPI client credentials flow", status == 200 and scheme.get("type") == "oauth2" and
                   flow.get("tokenUrl") == self.root + "/oauth/token" and not scheme.get("flows", {}).get("authorizationCode"))
        admin = self.credentials["admin_token"]
        reader = self.credentials["read_token"]
        status, _, projects = self.request("GET", "/api/v1/projects", token=admin)
        self.check("administrator API access", status == 200 and
                   any(project.get("id") == self.project_id for project in projects.get("projects", [])))
        self.check("reader API access", self.request("GET", base + "/schema", token=reader)[0] == 200)
        self.check("reader cannot create projects", self.request("POST", "/api/v1/projects", {"name": marker}, token=reader)[0] == 403)
        status, _, other = self.request("POST", "/api/v1/projects", {"name": marker + "-isolation"}, token=admin)
        self.check("isolated synthetic comparison project", status == 201 and isinstance(other.get("project", {}).get("id"), str))
        other_id = urllib.parse.quote(other["project"]["id"], safe="")
        self.check("reader project boundary", self.request("GET", "/api/v1/projects/" + other_id + "/schema", token=reader)[0] == 403)
        secret = self.oauth["read"]["client_secret"]
        self.check("client secret is not an API bearer", self.request("GET", base + "/schema", token=secret)[0] == 401)
        self.check("client secret is not an MCP bearer", self.request("GET", "/mcp", token=secret)[0] == 401)
        self.exchange({"client_id": self.project_id, "client_secret": self.credentials["ingest_token"]}, expected=401)
        writer = self.exchange(self.oauth["write"])
        event = {"uuid": marker + "-public", "event": "oauth_check", "distinct_id": "oauth-check-synthetic-user",
                 "properties": {"synthetic": True, "run_id": marker, "transport": "public-key"}}
        self.check("public PostHog capture unchanged", self.request("POST", "/capture/", {**event, "api_key": self.credentials["ingest_token"]})[0] == 200)
        backend = {**event, "uuid": marker + "-backend", "properties": {"synthetic": True, "run_id": marker, "transport": "oauth"}}
        self.check("backend OAuth write without public key", self.request("POST", "/capture/", backend, token=writer)[0] == 200)
        self.check("reader cannot capture events", self.request("POST", "/capture/", {**backend, "uuid": marker + "-denied"}, token=reader)[0] == 403)
        self.check("client secret is not an ingestion key", self.request("POST", "/capture/", {**backend, "uuid": marker + "-secret-denied", "api_key": self.oauth["write"]["client_secret"]})[0] == 401)
        status, _, captured = self.request("GET", base + "/events/" + backend["uuid"], token=reader)
        self.check("OAuth client does not replace event identity", status == 200 and
                   captured.get("event", {}).get("distinct_id") == "oauth-check-synthetic-user")
        mcp_reader = self.credentials["mcp_read_token"]
        self.check("MCP token cannot access API resource", self.request("GET", base + "/schema", token=mcp_reader)[0] == 401)
        self.check("API token cannot access MCP resource", self.mcp("initialize", {}, token=reader)[0] == 401)
        status, _, initialized = self.mcp("initialize", {"protocolVersion": "2025-11-25", "capabilities": {},
            "clientInfo": {"name": "porcupine-oauth-check", "version": "1"}})
        self.check("MCP OAuth initialization", status == 200 and isinstance(initialized.get("result"), dict))
        status, _, tools = self.mcp("tools/list", {})
        self.check("MCP OAuth tools discovery", status == 200 and
                   {tool.get("name") for tool in tools.get("result", {}).get("tools", [])} == {"porcupine_docs", "porcupine_call"})
        status, _, schema = self.mcp("tools/call", {"name": "porcupine_call", "arguments": {
            "operation_id": "get-event-schema", "path": {"id": self.project_id}}})
        result = schema.get("result", {})
        self.check("MCP trusted dispatch to API schema", status == 200 and not result.get("isError") and
                   result.get("structuredContent", {}).get("status") == 200 and
                   isinstance(result.get("structuredContent", {}).get("body", {}).get("tables"), list))
        status, _, denied = self.mcp("tools/call", {"name": "porcupine_call", "arguments": {
            "operation_id": "get-event-schema", "path": {"id": other["project"]["id"]}}})
        self.check("MCP reader project boundary", status == 200 and denied.get("result", {}).get("isError") is True and
                   denied["result"].get("structuredContent", {}).get("status") == 403)
        status, _, clients = self.request("GET", "/api/v1/oauth-clients", token=admin)
        listed = clients.get("oauth_clients", [])
        self.check("OAuth client listing has no secrets", status == 200 and isinstance(listed, list) and
                   all(isinstance(client, dict) for client in listed) and not contains_secret_member(clients))
        temporary_id = None
        try:
            status, headers, temporary = self.request("POST", "/api/v1/oauth-clients", {
                "name": marker + "-revocation", "permissions": ["events:read"], "projects": [self.project_id]}, token=admin)
            self.check("ephemeral OAuth client creation", status == 201 and "no-store" in headers.get("Cache-Control", ""))
            temporary_id, _ = client_pair(temporary)
            temporary_token = self.exchange(temporary)
            self.check("ephemeral token initially authorized", self.request("GET", base + "/schema", token=temporary_token)[0] == 200)
            status, _, _ = self.request("DELETE", "/api/v1/oauth-clients/" + urllib.parse.quote(temporary_id, safe=""), token=admin)
            self.check("OAuth client revocation", status == 204)
            temporary_id = None
            self.check("revocation invalidates issued token immediately", self.request("GET", base + "/schema", token=temporary_token)[0] == 401)
        finally:
            if temporary_id is not None:
                # Best effort cleanup uses only the known newly created principal.
                try:
                    self.request("DELETE", "/api/v1/oauth-clients/" + urllib.parse.quote(temporary_id, safe=""), token=self.credentials["admin_token"])
                except Exception:
                    pass

    def report(self, passed, failure=None):
        report = {"passed": passed, "checks": self.checks, "token_lifetime_seconds": self.lifetime,
                  "project_id": self.project_id, "production_connected": False}
        if failure is not None:
            report["failed_check"] = failure
        return report


def write_report(path, report):
    text = json.dumps(report, indent=2) + "\n"
    if path:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_NOFOLLOW", 0), 0o600)
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "w") as output:
                output.write(text)
        except Exception:
            try:
                os.close(fd)
            except OSError:
                pass
            raise
    print(text, end="")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--credentials", required=True, type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    checker = None
    output_path = args.output
    try:
        if args.output and args.output.resolve() == args.credentials.resolve():
            output_path = None
            raise CheckFailure("report path differs from credentials")
        credentials = load_credentials(args.credentials)
        checker = Checker(args.url, credentials)
        checker.run()
        report = checker.report(True)
    except CheckFailure as error:
        report = checker.report(False, str(error)) if checker else {
            "passed": False, "checks": [], "token_lifetime_seconds": None, "failed_check": str(error), "production_connected": False}
    except CredentialError:
        report = checker.report(False, "private OAuth credentials and token exchange") if checker else {
            "passed": False, "checks": [], "token_lifetime_seconds": None, "failed_check": "private OAuth credentials", "production_connected": False}
    except Exception:
        # Exceptions may carry HTTP URLs or secrets. Never serialize their values.
        report = checker.report(False, "unexpected checker failure") if checker else {
            "passed": False, "checks": [], "token_lifetime_seconds": None, "failed_check": "unexpected checker failure", "production_connected": False}
    try:
        write_report(output_path, report)
    except Exception:
        print(json.dumps({"passed": False, "checks": [], "token_lifetime_seconds": None, "failed_check": "private report write", "production_connected": False}))
        return 1
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
