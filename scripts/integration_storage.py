#!/usr/bin/env python3
"""Isolated MinIO helpers for the ClickHouse integration harness. Never print credentials."""
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import hmac
import os
from pathlib import Path
import secrets
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request

MINIO_IMAGE = "minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"
ROOT = Path(__file__).resolve().parents[1]


def _s3_request(method, url, access_key, secret_key, region="us-east-1"):
    """Sign an empty-payload S3 request with AWS Signature Version 4."""
    now = datetime.now(timezone.utc)
    timestamp, date = now.strftime("%Y%m%dT%H%M%SZ"), now.strftime("%Y%m%d")
    parsed = urllib.parse.urlsplit(url)
    digest = hashlib.sha256(b"").hexdigest()
    signed_headers = "host;x-amz-content-sha256;x-amz-date"
    canonical_headers = (f"host:{parsed.netloc}\n"
                         f"x-amz-content-sha256:{digest}\n"
                         f"x-amz-date:{timestamp}\n")
    query = urllib.parse.urlencode(sorted(urllib.parse.parse_qsl(parsed.query)),
                                   quote_via=urllib.parse.quote, safe="~")
    canonical = "\n".join([method, parsed.path or "/", query, canonical_headers,
                            signed_headers, digest])
    scope = date + "/" + region + "/s3/aws4_request"
    to_sign = "AWS4-HMAC-SHA256\n" + timestamp + "\n" + scope + "\n" + hashlib.sha256(canonical.encode()).hexdigest()
    key = ("AWS4" + secret_key).encode()
    for part in [date, region, "s3", "aws4_request"]:
        key = hmac.new(key, part.encode(), hashlib.sha256).digest()
    signature = hmac.new(key, to_sign.encode(), hashlib.sha256).hexdigest()
    authorization = f"AWS4-HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
    request = urllib.request.Request(url, method=method, data=b"" if method == "PUT" else None, headers={
        "X-Amz-Date": timestamp, "X-Amz-Content-Sha256": digest, "Authorization": authorization})
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.read()
    except (OSError, urllib.error.HTTPError) as error:
        raise RuntimeError("MinIO S3 request failed") from None


@contextmanager
def start_minio():
    """Yield operator S3 settings and a private Docker network; remove both on exit.

    endpoint is the container-reachable bucket/prefix endpoint for ClickHouse.
    host_endpoint is used only for host-side bucket inspection in this harness.
    environment can be merged into docker run's environment; secrets stay out of argv.
    """
    suffix = secrets.token_hex(6)
    container, network = "porcupine-s3-" + suffix, "porcupine-s3-network-" + suffix
    access_key, secret_key = "pc_" + secrets.token_hex(10), secrets.token_hex(32)
    bucket = "porcupine-" + suffix
    environment = {**os.environ, "MINIO_ROOT_USER": access_key, "MINIO_ROOT_PASSWORD": secret_key}
    network_created = container_created = False
    try:
        subprocess.run(["docker", "network", "create", "--label", "porcupine.integration=true", network],
                       check=True, stdout=subprocess.DEVNULL)
        network_created = True
        subprocess.run(["docker", "run", "-d", "--name", container, "--network", network,
                        "--label", "porcupine.integration=true", "--memory", "512m", "--cpus", "1",
                        "-p", "127.0.0.1::9000", "-e", "MINIO_ROOT_USER", "-e", "MINIO_ROOT_PASSWORD",
                        MINIO_IMAGE, "server", "/data", "--address", ":9000"],
                       env=environment, check=True, stdout=subprocess.DEVNULL)
        container_created = True
        port = subprocess.check_output(["docker", "port", container, "9000/tcp"], text=True).strip().rsplit(":", 1)[1]
        host_endpoint = "http://127.0.0.1:" + port
        for attempt in range(150):
            try:
                with urllib.request.urlopen(host_endpoint + "/minio/health/live", timeout=1) as response:
                    if response.status == 200:
                        break
            except (OSError, urllib.error.HTTPError):
                pass
            time.sleep(0.2)
        else:
            raise RuntimeError("MinIO did not become ready")
        _s3_request("PUT", host_endpoint + "/" + bucket, access_key, secret_key)
        endpoint = "http://" + container + ":9000/" + bucket + "/clickhouse/"
        yield {"network": network, "container": container, "endpoint": endpoint,
               "host_endpoint": host_endpoint, "access_key": access_key, "secret_key": secret_key,
               "bucket": bucket, "environment": {
                   "PORCUPINE_S3_ENDPOINT": endpoint, "AWS_REGION": "us-east-1",
                   "AWS_ACCESS_KEY_ID": access_key, "AWS_SECRET_ACCESS_KEY": secret_key,
                   "AWS_SESSION_TOKEN": ""}}
    finally:
        if container_created:
            subprocess.run(["docker", "rm", "-fv", container], check=True, stdout=subprocess.DEVNULL)
        if network_created:
            subprocess.run(["docker", "network", "rm", network], check=True, stdout=subprocess.DEVNULL)


def clickhouse_s3_docker_args(config):
    """Docker flags; merge config['environment'] into the docker process environment."""
    args = ["--network", config["network"], "--mount",
            "type=bind,src=" + str(ROOT / "deployment/clickhouse/storage.xml") +
            ",dst=/etc/clickhouse-server/config.d/porcupine-storage.xml,readonly"]
    for name in config["environment"]:
        args.extend(["-e", name])
    return args


def minio_objects(config):
    """Return the bucket listing XML for assertions that cold parts have S3 objects."""
    return _s3_request("GET", config["host_endpoint"] + "/" + config["bucket"] + "?list-type=2",
                       config["access_key"], config["secret_key"])


if __name__ == "__main__":
    with start_minio() as config:
        if b"ListBucketResult" not in minio_objects(config):
            raise RuntimeError("MinIO bucket verification failed")
        print("Isolated MinIO bucket and Signature V4 helper passed")
