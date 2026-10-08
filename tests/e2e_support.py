"""Shared helpers for the black-box e2e suite (tests/test_e2e_*.py).

Runs against an already running compose.yaml stack, through Envoy only, with the
service users provisioned by auth-bootstrap. Skipped unless BNPB_E2E=1.
Tokens and secrets are never printed.
"""

import base64
import http.client
import http.server
import json
import os
from pathlib import Path
import socket
import threading
import time
import unittest
from urllib.parse import urlencode


ROOT = Path(__file__).resolve().parents[1]
HOST = os.environ.get("E2E_HOST", "127.0.0.1")
PORT = int(os.environ.get("E2E_PORT", "8080"))
# Envoy routes by Host; connecting to an IP keeps *.localhost resolution out of the way.
APP_HOST = os.environ.get("E2E_APP_HOST", "localhost:8080")
AUTH_HOST = os.environ.get("E2E_AUTH_HOST", "auth.localhost:8080")
SECRETS_DIR = Path(os.environ.get("E2E_SECRETS_DIR", ROOT / ".cache" / "auth"))
TIMEOUT = float(os.environ.get("E2E_TIMEOUT", "120"))
CALLBACK_HOST = os.environ.get("E2E_CALLBACK_HOST", "host.docker.internal")

USERS = {"MEDIA": "svc-media", "FIELD_TEAM": "svc-lapangan", "BNPB_INTERNAL": "svc-bnpb"}
FULL_ONLY = {"source_ref_id", "latitude", "longitude", "attributes"}


def require_stack():
    if os.environ.get("BNPB_E2E") != "1":
        raise unittest.SkipTest("set BNPB_E2E=1 with the compose.yaml stack running")
    missing = [name for name in ["project-id", *(f"{user}.secret" for user in USERS.values())]
               if not (SECRETS_DIR / name).is_file()]
    if missing:
        raise AssertionError(f"missing {', '.join(missing)} in {SECRETS_DIR}; run docker compose up first")


def _read(name):
    return (SECRETS_DIR / name).read_text(encoding="utf-8").strip()


def _request(method, path, *, host, body=None, headers=None, timeout=30):
    connection = http.client.HTTPConnection(HOST, PORT, timeout=timeout)
    try:
        connection.request(method, path, body=body, headers={"Host": host, **(headers or {})})
        response = connection.getresponse()
        return response.status, {k.lower(): v for k, v in response.getheaders()}, response.read()
    finally:
        connection.close()


_tokens = {}


def token(user):
    """Client-credentials access token for a service user, cached until shortly before expiry."""
    cached = _tokens.get(user)
    if cached and cached[1] > time.monotonic():
        return cached[0]
    basic = base64.b64encode(f"{user}:{_read(f'{user}.secret')}".encode()).decode()
    scope = f"openid urn:zitadel:iam:org:project:id:{_read('project-id')}:aud"
    status, _, raw = _request("POST", "/oauth/v2/token", host=AUTH_HOST,
                              body=urlencode({"grant_type": "client_credentials", "scope": scope}),
                              headers={"Authorization": f"Basic {basic}",
                                       "Content-Type": "application/x-www-form-urlencoded"})
    if status != 200:
        # The body may echo request details; report only the status and error code.
        error = json.loads(raw or b"{}").get("error", "") if raw.startswith(b"{") else ""
        raise AssertionError(f"token request for {user} failed: HTTP {status} {error}")
    payload = json.loads(raw)
    _tokens[user] = (payload["access_token"], time.monotonic() + max(0, payload.get("expires_in", 60) - 10))
    return payload["access_token"]


def api(method, path, *, user=None, query=None, body=None, headers=None):
    """Request through Envoy. Returns (status, lowercase headers, parsed JSON or None)."""
    sent = dict(headers or {})
    if user:
        sent["Authorization"] = f"Bearer {token(user)}"
    raw_body = None
    if body is not None:
        raw_body = json.dumps(body)
        sent["Content-Type"] = "application/json"
    if query:
        path = f"{path}?{urlencode(query)}"
    status, response_headers, raw = _request(method, path, host=APP_HOST, body=raw_body, headers=sent)
    try:
        parsed = json.loads(raw) if raw else None
    except ValueError:
        parsed = None
    return status, response_headers, parsed


def wait_for(probe, timeout=TIMEOUT, interval=3, message="condition"):
    """Poll probe() until it returns a truthy value; return that value."""
    deadline = time.monotonic() + timeout
    while True:
        value = probe()
        if value:
            return value
        if time.monotonic() >= deadline:
            raise AssertionError(f"timed out after {timeout:.0f}s waiting for {message}")
        time.sleep(interval)


class SseReader:
    """Minimal text/event-stream client. Comment lines (heartbeats) are skipped."""

    def __init__(self, path, *, user, query=None, timeout=TIMEOUT):
        if query:
            path = f"{path}?{urlencode(query)}"
        self.connection = http.client.HTTPConnection(HOST, PORT, timeout=timeout)
        self.connection.request("GET", path, headers={
            "Host": APP_HOST, "Accept": "text/event-stream", "Authorization": f"Bearer {token(user)}"})
        self.response = self.connection.getresponse()
        self.status = self.response.status
        self.headers = {k.lower(): v for k, v in self.response.getheaders()}

    def next_event(self, deadline):
        """Next complete event as dict(id, event, data) before the monotonic deadline."""
        event = {}
        while time.monotonic() < deadline:
            try:
                line = self.response.fp.readline()
            except socket.timeout:
                break
            if not line:
                break
            line = line.decode("utf-8").rstrip("\r\n")
            if not line:
                if "data" in event:
                    return event
                event = {}
            elif line.startswith(":"):
                continue
            else:
                field, _, value = line.partition(":")
                value = value[1:] if value.startswith(" ") else value
                event[field] = event[field] + "\n" + value if field == "data" and "data" in event else value
        raise AssertionError("no SSE event before the deadline")

    def close(self):
        self.connection.close()


class WebhookListener:
    """Records POSTs from webhook-provider. Bound on all interfaces so containers can reach it."""

    def __init__(self):
        self.received = []
        self.lock = threading.Lock()
        listener = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                with listener.lock:
                    listener.received.append(({k.lower(): v for k, v in self.headers.items()}, body))
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, *args):
                pass

        self.server = http.server.ThreadingHTTPServer(("0.0.0.0", 0), Handler)
        self.url = f"http://{CALLBACK_HOST}:{self.server.server_address[1]}/hook"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def deliveries(self):
        with self.lock:
            return list(self.received)

    def close(self):
        self.server.shutdown()
        self.server.server_close()
