"""Standalone BMKG pull-only REST mock, using the Python standard library."""

import hmac
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import re
import signal
import sqlite3
import sys
import threading
import time
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

from config import Config, ConfigurationError
from storage import Store, timestamp, timestamp_us


CORRELATION = re.compile(r"[A-Za-z0-9._:-]{1,128}", re.ASCII)
TRACEPARENT = re.compile(r"00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})")
ROUTES = {"/seismic-events": "list_seismic_events", "/tsunami-warnings": "list_tsunami_warnings", "/health": "health"}
LOG_LOCK = threading.Lock()


def log(level: str, operation: str, status: int | str, *, duration_ms: float = 0, correlation_id: str | None = None, traceparent: str | None = None, **fields) -> None:
    entry = {
        "timestamp": timestamp(), "level": level, "service": "bmkg-mock",
        "correlation_id": correlation_id, "operation": operation,
        "duration_ms": round(duration_ms, 3), "status": status,
    }
    if traceparent:
        entry["traceparent"] = traceparent
    entry.update(fields)
    with LOG_LOCK:
        print(json.dumps(entry, ensure_ascii=True, separators=(",", ":")), flush=True)


class Generator(threading.Thread):
    def __init__(self, store: Store, interval_ms: int, stop: threading.Event):
        super().__init__(name="bmkg-generator", daemon=True)
        self.store = store
        self.interval = interval_ms / 1000
        self.stop = stop
        self.state_lock = threading.Lock()
        self.last_success = time.monotonic()
        self.last_attempt_ok = True

    def healthy(self) -> bool:
        with self.state_lock:
            return self.is_alive() and not self.stop.is_set() and self.last_attempt_ok and time.monotonic() - self.last_success <= self.interval + 6

    def run(self) -> None:
        index = 0
        deadline = time.monotonic() + self.interval
        while not self.stop.wait(max(0, deadline - time.monotonic())):
            started = time.monotonic()
            try:
                self.store.generate(index)
            except Exception as exc:
                with self.state_lock:
                    self.last_attempt_ok = False
                log("ERROR", "generate_event", "failed", duration_ms=(time.monotonic() - started) * 1000, error_type=type(exc).__name__)
            else:
                index += 1
                with self.state_lock:
                    self.last_success = time.monotonic()
                    self.last_attempt_ok = True
                log("INFO", "generate_event", "ok", duration_ms=(time.monotonic() - started) * 1000)
            deadline = max(deadline + self.interval, time.monotonic())


class Server(ThreadingHTTPServer):
    daemon_threads = False
    allow_reuse_address = True
    request_queue_size = 128

    def __init__(self, config: Config, store: Store, generator: Generator):
        self.config = config
        self.store = store
        self.generator = generator
        super().__init__(("0.0.0.0", config.port), Handler)

    def get_request(self):
        request, address = super().get_request()
        request.settimeout(5)
        return request, address

    def handle_error(self, request, client_address):
        log("ERROR", "http_connection", "failed")


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "BMKGMock"
    sys_version = ""

    def log_message(self, format, *args):
        # Suppress the stdlib access log, which includes the raw query string.
        pass

    def _context(self) -> bool:
        self.started = time.monotonic()
        self.traceparent = None
        self.instance = "/"
        self.operation = "http_request"
        headers = getattr(self, "headers", None)
        supplied = headers.get_all("X-Correlation-ID", []) if headers else []
        valid = len(supplied) <= 1 and (not supplied or CORRELATION.fullmatch(supplied[0]) is not None)
        self.correlation_id = supplied[0] if supplied and valid else str(uuid4())
        traces = headers.get_all("traceparent", []) if headers else []
        if len(traces) == 1:
            match = TRACEPARENT.fullmatch(traces[0])
            if match and int(match.group(1), 16) and int(match.group(2), 16):
                self.traceparent = traces[0]
        return valid

    def _write(self, status: int, payload: dict, *, problem: bool = False, headers: dict | None = None) -> None:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
        self.close_connection = True
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/problem+json" if problem else "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("X-Correlation-ID", self.correlation_id)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Connection", "close")
            for name, value in (headers or {}).items():
                self.send_header(name, value)
            self.end_headers()
            if getattr(self, "command", "") != "HEAD":
                self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, TimeoutError):
            pass
        finally:
            log("ERROR" if status >= 500 else "INFO", self.operation, status,
                duration_ms=(time.monotonic() - self.started) * 1000,
                correlation_id=self.correlation_id, traceparent=self.traceparent)

    def _problem(self, status: int, detail: str, headers: dict | None = None) -> None:
        if status == 503:
            headers = {"Retry-After": "1", **(headers or {})}
        self._write(status, {
            "type": "about:blank", "title": HTTPStatus(status).phrase,
            "status": status, "detail": detail, "instance": self.instance,
            "correlation_id": self.correlation_id,
        }, problem=True, headers=headers)

    def send_error(self, code, message=None, explain=None):
        # Protocol failures follow the same safe JSON/correlation rules.
        self._context()
        self._problem(code, "The HTTP request could not be processed.")

    def _dispatch(self) -> None:
        valid_correlation = self._context()
        try:
            parsed = urlsplit(self.path)
            if parsed.path in ROUTES:
                self.instance = parsed.path
                self.operation = ROUTES[parsed.path]
            if not valid_correlation:
                self._problem(400, "X-Correlation-ID must contain 1 to 128 ASCII letters, digits, dots, underscores, colons, or hyphens.")
                return
            if parsed.scheme or parsed.netloc or parsed.fragment or not self.path.startswith("/"):
                self._problem(400, "Use a valid origin-form request target.")
                return
            if parsed.path not in ROUTES:
                self._problem(404, "The requested resource does not exist.")
                return
            if self.command != "GET":
                self._problem(405, "This resource only supports GET.", {"Allow": "GET"})
                return
            if parsed.path != "/health":
                keys = self.headers.get_all("X-BMKG-Key", [])
                if len(keys) != 1 or not hmac.compare_digest(keys[0].encode("utf-8"), self.server.config.api_key.encode("utf-8")):
                    self._problem(401, "A valid BMKG API key is required.", {"WWW-Authenticate": 'ApiKey realm="bmkg"'})
                    return
            if re.search(r"%(?![0-9A-Fa-f]{2})", parsed.query):
                raise ValueError("Invalid query encoding")
            parameters = parse_qs(parsed.query, keep_blank_values=True, strict_parsing=True, encoding="utf-8", errors="strict", max_num_fields=16)
            allowed = {"since"} if parsed.path != "/health" else set()
            if set(parameters) - allowed or any(len(values) != 1 for values in parameters.values()):
                raise ValueError("Unknown or repeated query parameter")
            since_us = timestamp_us(parameters["since"][0]) if "since" in parameters else None
        except (ValueError, OverflowError, UnicodeError):
            self._problem(400, "Query parameters must be supported, occur once, and use an RFC3339 since timestamp with a timezone.")
            return

        try:
            if parsed.path == "/health":
                self.server.store.check_health()
                if not self.server.generator.healthy():
                    self._problem(503, "The BMKG mock is temporarily unavailable.")
                    return
                self._write(200, {"status": "ok"})
            else:
                time.sleep(self.server.config.response_delay_ms / 1000)
                items = self.server.store.list_events(since_us) if parsed.path == "/seismic-events" else self.server.store.list_warnings(since_us)
                self._write(200, {"items": items})
        except (sqlite3.Error, OSError, ValueError, TypeError, KeyError):
            self._problem(503, "The BMKG mock is temporarily unavailable.")

    do_GET = _dispatch
    do_HEAD = _dispatch
    do_POST = _dispatch
    do_PUT = _dispatch
    do_PATCH = _dispatch
    do_DELETE = _dispatch
    do_OPTIONS = _dispatch


def main() -> int:
    try:
        config = Config.from_environment()
    except (ConfigurationError, OSError, ValueError):
        log("ERROR", "startup", "invalid_configuration", detail="Check BMKG environment variables and their documented ranges.")
        return 1
    stop = threading.Event()
    store = Store(config.database_path)
    generator = Generator(store, config.event_interval_ms, stop)
    server = None
    try:
        store.initialize(config.seed_count)
        server = Server(config, store, generator)
    except (sqlite3.Error, OSError, ValueError):
        log("ERROR", "startup", "unavailable", detail="BMKG persistence or HTTP listener could not be initialized.")
        if server:
            server.server_close()
        return 1

    def shutdown(signum, frame):
        if not stop.is_set():
            stop.set()
            log("INFO", "shutdown", "requested")
            # shutdown() must run outside the serve_forever thread.
            threading.Thread(target=server.shutdown, name="bmkg-http-shutdown", daemon=True).start()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    generator.start()
    log("INFO", "startup", "ready", port=config.port)
    try:
        server.serve_forever(poll_interval=0.1)
    finally:
        stop.set()
        server.server_close()
        generator.join(timeout=6)
        log("INFO", "shutdown", "complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
