"""Shared support and utility functions for Problem Set (P1-P5) automated tests.

Provides:
- Configuration reading from .env and .env.auth
- Mock service HTTP client and admin controls (PVMBG schema version, outage)
- RabbitMQ Management API and AMQP inspection helpers
- Docker container inspection and management helpers
- Benchmark / latency statistical calculations (p50, p90, p95, p99)
- Secret scanning utilities for repository compliance checks
"""

import base64
from datetime import datetime, timezone
import http.client
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

# Import base e2e support functions
from e2e_support import (
    APP_HOST,
    AUTH_HOST,
    CALLBACK_HOST,
    FULL_ONLY,
    HOST,
    PORT,
    ROOT,
    SECRETS_DIR,
    TIMEOUT,
    USERS,
    SseReader,
    WebhookListener,
    api,
    require_stack,
    token,
    wait_for,
)


def read_env_file(filepath: Path) -> Dict[str, str]:
    """Parse key=value pairs from an env file without shell evaluation."""
    env = {}
    if filepath.is_file():
        for line in filepath.read_text(encoding="utf-8-sig").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            env[key.strip()] = val.strip().strip("\"'")
    return env


def get_config() -> Dict[str, str]:
    """Load configuration from .env, .env.auth, and os.environ."""
    config = {}
    config.update(read_env_file(ROOT / ".env"))
    config.update(read_env_file(ROOT / ".env.auth"))
    config.update({k: v for k, v in os.environ.items() if v})

    # Set defaults for required keys
    config.setdefault("BMKG_HTTP_PORT", "8081")
    config.setdefault("PVMBG_HTTP_PORT", "8082")
    config.setdefault("RABBITMQ_DEFAULT_USER", "bnpb_app")
    config.setdefault("RABBITMQ_DEFAULT_PASS", "bnpb_app_password")
    config.setdefault("RABBITMQ_DEFAULT_VHOST", "bnpb")
    config.setdefault("RABBITMQ_MGMT_PORT", "15672")
    config.setdefault("AGGREGATOR_GRPC_PORT", "9091")
    config.setdefault("RABBITMQ_EXCHANGE", "bnpb.hazards.v1")
    return config


CONFIG = get_config()


# ---------------------------------------------------------------------------
# Upstream Mock Services (BMKG & PVMBG)
# ---------------------------------------------------------------------------

def mock_request(
    source: str,
    path: str,
    *,
    method: str = "GET",
    body: Optional[Dict[str, Any]] = None,
    auth: bool = True,
    custom_auth_header: Optional[Dict[str, str]] = None,
    timeout: float = 10.0,
) -> Tuple[int, Dict[str, str], Any]:
    """Make direct HTTP request to BMKG (8081) or PVMBG (8082) mock."""
    source = source.lower()
    port = int(CONFIG.get(f"{source.upper()}_HTTP_PORT", "8081" if source == "bmkg" else "8082"))
    
    headers = {"Host": f"localhost:{port}"}
    if custom_auth_header is not None:
        headers.update(custom_auth_header)
    elif auth:
        if source == "bmkg":
            key = CONFIG.get("BMKG_API_KEY", "")
            headers["X-BMKG-Key"] = key
        else:
            token_val = CONFIG.get("PVMBG_BEARER_TOKEN", "")
            headers["Authorization"] = f"Bearer {token_val}"

    raw_body = None
    if body is not None:
        raw_body = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"

    conn = http.client.HTTPConnection(HOST, port, timeout=timeout)
    try:
        conn.request(method, path, body=raw_body, headers=headers)
        resp = conn.getresponse()
        resp_headers = {k.lower(): v for k, v in resp.getheaders()}
        resp_body = resp.read()
        try:
            parsed = json.loads(resp_body) if resp_body else None
        except Exception:
            parsed = resp_body.decode("utf-8", errors="replace")
        return resp.status, resp_headers, parsed
    finally:
        conn.close()


def set_pvmbg_schema_version(version: int) -> Dict[str, Any]:
    """Change PVMBG mock schema version via POST /admin/schema-version."""
    status, _, data = mock_request("pvmbg", "/admin/schema-version", method="POST", body={"version": version})
    if status != 200:
        raise AssertionError(f"Failed to set PVMBG schema version to {version}: HTTP {status} {data}")
    return data


def set_pvmbg_outage(enabled: bool) -> Dict[str, Any]:
    """Toggle PVMBG mock outage via POST /admin/outage."""
    status, _, data = mock_request("pvmbg", "/admin/outage", method="POST", body={"enabled": enabled})
    if status != 200:
        raise AssertionError(f"Failed to set PVMBG outage to {enabled}: HTTP {status} {data}")
    return data


# ---------------------------------------------------------------------------
# RabbitMQ HTTP Management API
# ---------------------------------------------------------------------------

class RabbitMQClient:
    """Client for RabbitMQ Management HTTP API (default port 15672)."""

    def __init__(self, host: str = HOST, port: int = 15672, user: str = None, password: str = None, vhost: str = None):
        self.host = host
        self.port = port
        self.user = user or CONFIG.get("RABBITMQ_DEFAULT_USER", "bnpb_app")
        self.password = password or CONFIG.get("RABBITMQ_DEFAULT_PASS", "bnpb_app_password")
        self.vhost = vhost or CONFIG.get("RABBITMQ_DEFAULT_VHOST", "bnpb")
        basic = base64.b64encode(f"{self.user}:{self.password}".encode("utf-8")).decode("utf-8")
        self.auth_header = f"Basic {basic}"

    def _req(self, method: str, path: str, body: Optional[Dict[str, Any]] = None) -> Tuple[int, Any]:
        conn = http.client.HTTPConnection(self.host, self.port, timeout=10)
        headers = {
            "Authorization": self.auth_header,
            "Content-Type": "application/json",
        }
        raw = json.dumps(body).encode("utf-8") if body is not None else None
        try:
            conn.request(method, path, body=raw, headers=headers)
            resp = conn.getresponse()
            raw_res = resp.read()
            try:
                parsed = json.loads(raw_res) if raw_res else None
            except Exception:
                parsed = raw_res.decode("utf-8", errors="replace")
            return resp.status, parsed
        finally:
            conn.close()

    def get_overview(self) -> Dict[str, Any]:
        status, data = self._req("GET", "/api/overview")
        if status != 200:
            raise AssertionError(f"RabbitMQ overview failed: HTTP {status} {data}")
        return data

    def list_queues(self) -> List[Dict[str, Any]]:
        status, data = self._req("GET", f"/api/queues")
        return data if status == 200 and isinstance(data, list) else []

    def list_exchanges(self) -> List[Dict[str, Any]]:
        status, data = self._req("GET", f"/api/exchanges")
        return data if status == 200 and isinstance(data, list) else []

    def list_bindings(self) -> List[Dict[str, Any]]:
        status, data = self._req("GET", f"/api/bindings")
        return data if status == 200 and isinstance(data, list) else []

    def declare_queue(self, queue_name: str, durable: bool = False, auto_delete: bool = True) -> bool:
        vhost_esc = "%2F" if self.vhost == "/" else self.vhost
        path = f"/api/queues/{vhost_esc}/{queue_name}"
        status, _ = self._req("PUT", path, body={"auto_delete": auto_delete, "durable": durable})
        return status in (201, 204)

    def bind_queue(self, exchange_name: str, queue_name: str, routing_key: str = "#") -> bool:
        vhost_esc = "%2F" if self.vhost == "/" else self.vhost
        path = f"/api/bindings/{vhost_esc}/e/{exchange_name}/q/{queue_name}"
        status, _ = self._req("POST", path, body={"routing_key": routing_key})
        return status in (201, 204)

    def delete_queue(self, queue_name: str) -> bool:
        vhost_esc = "%2F" if self.vhost == "/" else self.vhost
        path = f"/api/queues/{vhost_esc}/{queue_name}"
        status, _ = self._req("DELETE", path)
        return status in (200, 204)

    def get_messages_from_queue(self, queue_name: str, count: int = 5, ackmode: str = "ack_requeue_true") -> List[Dict[str, Any]]:
        vhost_esc = "%2F" if self.vhost == "/" else self.vhost
        path = f"/api/queues/{vhost_esc}/{queue_name}/get"
        body = {"count": count, "ackmode": ackmode, "encoding": "auto", "truncate": 50000}
        status, data = self._req("POST", path, body=body)
        return data if status == 200 and isinstance(data, list) else []


# ---------------------------------------------------------------------------
# Statistics & Percentiles
# ---------------------------------------------------------------------------

def calculate_percentiles(durations_ms: List[float]) -> Dict[str, float]:
    """Calculate p50, p90, p95, p99, min, max, avg latencies from list of ms."""
    if not durations_ms:
        return {"count": 0, "p50": 0.0, "p90": 0.0, "p95": 0.0, "p99": 0.0, "avg": 0.0, "max": 0.0, "min": 0.0}
    s = sorted(durations_ms)
    n = len(s)

    def pct(p):
        idx = int(round((p / 100.0) * (n - 1)))
        return s[max(0, min(n - 1, idx))]

    return {
        "count": n,
        "min": round(s[0], 2),
        "max": round(s[-1], 2),
        "avg": round(sum(s) / n, 2),
        "p50": round(pct(50), 2),
        "p90": round(pct(90), 2),
        "p95": round(pct(95), 2),
        "p99": round(pct(99), 2),
    }


# ---------------------------------------------------------------------------
# Docker Helpers
# ---------------------------------------------------------------------------

def docker_exec(cmd: List[str]) -> Tuple[int, str]:
    """Execute docker command and return exit code and combined stdout/stderr."""
    try:
        proc = subprocess.run(
            ["docker", *cmd],
            cwd=str(ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=30,
        )
        return proc.returncode, proc.stdout
    except Exception as e:
        return -1, str(e)


def is_docker_available() -> bool:
    code, out = docker_exec(["compose", "ps"])
    return code == 0


def get_running_containers() -> List[str]:
    code, out = docker_exec(["compose", "ps", "--format", "json"])
    if code != 0 or not out.strip():
        return []
    try:
        containers = []
        for line in out.splitlines():
            line = line.strip()
            if not line:
                continue
            item = json.loads(line)
            containers.append(item.get("Service", item.get("Name", "")))
        return containers
    except Exception:
        return []
