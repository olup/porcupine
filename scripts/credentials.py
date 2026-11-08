"""Read private operator credentials; keep OAuth access tokens only in memory."""
import base64
from collections.abc import Mapping
import ipaddress
import http.client
import json
import os
from pathlib import Path
import ssl
import stat
import threading
import time
import urllib.error
import urllib.parse
import urllib.request


TOKEN_RESPONSE_LIMIT = 32 << 10
CREDENTIAL_FILE_LIMIT = 64 << 10
TOKEN_TIMEOUT = 10
_TOKEN_KEYS = {
    "admin_token": ("admin", "/api/v1"),
    "read_token": ("read", "/api/v1"),
    "write_token": ("write", "/api/v1"),
    "mcp_admin_token": ("admin", "/mcp"),
    "mcp_read_token": ("read", "/mcp"),
}


class CredentialError(ValueError):
    """A sanitized credential failure, safe to include in a tool report."""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, newurl):
        return None


def _unique_object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate JSON member")
        value[key] = item
    return value


def _string(value, maximum=4096):
    return (isinstance(value, str) and bool(value) and len(value) <= maximum
            and not any(ord(char) < 32 or ord(char) == 127 for char in value))


def _issuer(value):
    if (not _string(value) or value != value.strip() or "\\" in value
            or any(char.isspace() for char in value)):
        raise CredentialError("invalid OAuth issuer")
    try:
        parsed = urllib.parse.urlsplit(value)
        port = parsed.port
        if (not parsed.hostname or parsed.username is not None or parsed.password is not None
                or parsed.query or parsed.fragment or "?" in value or "#" in value
                or parsed.path not in ("", "/") or port is not None and not 0 < port <= 65535):
            raise ValueError
        loopback = parsed.hostname.lower() == "localhost"
        try:
            loopback = loopback or ipaddress.ip_address(parsed.hostname).is_loopback
        except ValueError:
            pass
        if parsed.scheme != "https" and not (parsed.scheme == "http" and loopback):
            raise ValueError
    except ValueError:
        raise CredentialError("invalid OAuth issuer") from None
    return value.rstrip("/")


class Credentials(Mapping):
    """Dictionary-like settings with lazy API/MCP token lookup and refresh."""

    def __init__(self, values, oauth, ca_file=None):
        self._values = values
        self._oauth = oauth
        self._tokens = {}
        self._exchanges = {}
        self._lock = threading.Lock()
        self._opener = urllib.request.build_opener(
            _NoRedirect(), urllib.request.HTTPSHandler(
                context=ssl.create_default_context(cafile=ca_file)))

    def __repr__(self):
        return "<Credentials mode=" + ("oauth" if self._oauth else "legacy") + ">"

    def __iter__(self):
        keys = list(self._values)
        if self._oauth:
            keys.extend(key for key, (role, _) in _TOKEN_KEYS.items()
                        if role in self._oauth and key not in self._values)
        else:
            keys.extend(key for key in ("mcp_admin_token", "mcp_read_token")
                        if key not in self._values and key.removeprefix("mcp_") in self._values)
        return iter(keys)

    def __len__(self):
        return sum(1 for _ in self)

    def __contains__(self, key):
        return key in self._values or (key in _TOKEN_KEYS and
            (self._oauth is not None and _TOKEN_KEYS[key][0] in self._oauth
             or key.startswith("mcp_") and key[4:] in self._values))

    def __getitem__(self, key):
        if self._oauth is not None and key in _TOKEN_KEYS:
            if _TOKEN_KEYS[key][0] not in self._oauth:
                raise KeyError(key)
            return self._token(*_TOKEN_KEYS[key])
        if key in self._values:
            return self._values[key]
        if key in ("mcp_admin_token", "mcp_read_token"):
            return self._values[key[4:]]
        raise KeyError(key)

    def validate_target(self, value):
        """Reject a different OAuth destination before any token is requested."""
        if self._oauth is not None and _issuer(value) != self._oauth["issuer"]:
            raise CredentialError("target origin does not match OAuth credential issuer")

    def open_api(self, request, timeout):
        """Pin authenticated API requests and reject redirects carrying credentials."""
        parsed = urllib.parse.urlsplit(request.full_url)
        self.validate_target(urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, "", "", "")))
        return self._opener.open(request, timeout=timeout)

    def token_exchanges(self):
        """Safe counters only, never tokens, client identifiers or secrets."""
        with self._lock:
            return dict(self._exchanges)

    def _token(self, role, suffix):
        with self._lock:
            started = time.monotonic()
            cached = self._tokens.get((role, suffix))
            if cached is not None and started < cached[1]:
                return cached[0]
            client = self._oauth[role]
            encoded = (urllib.parse.quote_plus(client["client_id"], safe="") + ":" +
                       urllib.parse.quote_plus(client["client_secret"], safe=""))
            basic = base64.b64encode(encoded.encode()).decode("ascii")
            form = urllib.parse.urlencode({
                "grant_type": "client_credentials",
                "resource": self._oauth["issuer"] + suffix,
            }).encode()
            request = urllib.request.Request(self._oauth["issuer"] + "/oauth/token",
                data=form, method="POST", headers={
                    "Authorization": "Basic " + basic,
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Accept": "application/json", "Cache-Control": "no-store"})
            try:
                with self._opener.open(request, timeout=TOKEN_TIMEOUT) as response:
                    if (response.status != 200 or response.headers.get_content_type() != "application/json"):
                        raise ValueError
                    raw = response.read(TOKEN_RESPONSE_LIMIT + 1)
                    if len(raw) > TOKEN_RESPONSE_LIMIT:
                        raise ValueError
                    body = json.loads(raw, object_pairs_hook=_unique_object)
                    token, kind, lifetime = body["access_token"], body["token_type"], body["expires_in"]
                    if (not _string(token) or not token.isascii() or any(char.isspace() for char in token)
                            or not isinstance(kind, str) or kind.lower() != "bearer"
                            or type(lifetime) is not int or not 0 < lifetime <= 86400):
                        raise ValueError
            except urllib.error.HTTPError as error:
                error.close()
                raise CredentialError("OAuth token exchange failed") from None
            except (OSError, ValueError, TypeError, KeyError, http.client.HTTPException, urllib.error.URLError):
                raise CredentialError("OAuth token exchange failed") from None
            self._tokens[(role, suffix)] = (token, started + lifetime - 30)
            label = role + suffix
            self._exchanges[label] = self._exchanges.get(label, 0) + 1
            return token


def load_credentials(path, ca_file=None):
    """Load legacy or OAuth JSON without exchanging tokens or writing the file."""
    try:
        with Path(path).open("rb") as source:
            mode = stat.S_IMODE(os.fstat(source.fileno()).st_mode)
            raw = source.read(CREDENTIAL_FILE_LIMIT + 1)
        if len(raw) > CREDENTIAL_FILE_LIMIT:
            raise ValueError
        values = json.loads(raw, object_pairs_hook=_unique_object)
        if not isinstance(values, dict):
            raise ValueError
        if not _string(values.get("project_id")):
            raise ValueError
        if "ingest_token" in values and not _string(values["ingest_token"]):
            raise ValueError
        oauth = values.get("oauth")
        if oauth is not None:
            if os.name == "posix" and mode != 0o600:
                raise CredentialError("OAuth credentials file must have mode 600")
            if not isinstance(oauth, dict):
                raise ValueError
            oauth = {**oauth, "issuer": _issuer(oauth.get("issuer"))}
            if not any(role in oauth for role in ("admin", "read", "write")):
                raise ValueError
            for role in ("admin", "read", "write"):
                if role not in oauth:
                    continue
                client = oauth[role]
                if (not isinstance(client, dict) or not _string(client.get("client_id"), 256)
                        or not _string(client.get("client_secret"))):
                    raise ValueError
        elif not (_string(values.get("admin_token")) or _string(values.get("read_token"))):
            raise ValueError
        for name in _TOKEN_KEYS:
            if name in values and not _string(values[name]):
                raise ValueError
        return Credentials(values, oauth, ca_file=ca_file)
    except CredentialError:
        raise
    except (OSError, ValueError, TypeError):
        raise CredentialError("cannot read a valid credentials JSON file") from None
