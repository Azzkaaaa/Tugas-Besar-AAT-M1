"""Independent PVMBG REST mock, implemented entirely with Python's standard library."""

from datetime import datetime, timezone
import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import random
import re
import signal
import sqlite3
import sys
import threading
import time
from urllib.parse import parse_qs, urlsplit
import uuid

from config import Config
from storage import Store, timestamp_us


CORRELATION_ID = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
TRACEPARENT = re.compile(r"^00-([0-9a-f]{32})-([0-9a-f]{16})-[0-9a-f]{2}$")
MAX_BODY_BYTES = 4096
LOG_LOCK = threading.Lock()


def log_event(level: str, operation: str, status: int, correlation_id: str = "-",
              duration_ms: float = 0, traceparent: str | None = None) -> None:
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "level": level,
        "service": "pvmbg-mock",
        "correlation_id": correlation_id,
        "operation": operation,
        "duration_ms": round(duration_ms, 3),
        "status": status,
    }
    if traceparent is not None:
        record["traceparent"] = traceparent
    with LOG_LOCK:
        print(json.dumps(record, separators=(",", ":")), flush=True)


def strict_object(pairs: list[tuple[str, object]]) -> dict:
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate JSON property")
        value[key] = item
    return value


def invalid_number(value: str):
    raise ValueError("non-finite JSON number")


class Application:
    def __init__(self, config: Config):
        self.config = config
        self.store = Store(config.database)
        self.store.initialize(config.seed_count)
        self.lock = threading.RLock()
        self.stop = threading.Event()
        # Controls reset on each process startup; reports persist.
        self.version = 1
        self.outage = False
        self.generator_ok = True
        self.generator = threading.Thread(target=self.generate, name="pvmbg-generator")

    def generate(self) -> None:
        interval = self.config.interval_ms / 1000
        deadline = time.monotonic() + interval
        while not self.stop.wait(max(0, deadline - time.monotonic())):
            started = time.monotonic()
            with self.lock:
                version = self.version
            try:
                # Keep recording events during an injected outage.
                self.store.append(version)
            except Exception:
                with self.lock:
                    self.generator_ok = False
                log_event("error", "generate_report", 503, duration_ms=(time.monotonic() - started) * 1000)
            else:
                with self.lock:
                    recovered = not self.generator_ok
                    self.generator_ok = True
                log_event("info", "generator_recovered" if recovered else "generate_report", 201,
                          duration_ms=(time.monotonic() - started) * 1000)
            deadline += interval
            if deadline < time.monotonic():
                deadline = time.monotonic() + interval


class Server(ThreadingHTTPServer):
    daemon_threads = False
    block_on_close = True
    allow_reuse_address = True
    request_queue_size = 128

    def __init__(self, application: Application):
        self.application = application
        super().__init__(("0.0.0.0", application.config.port), Handler)


class Handler(BaseHTTPRequestHandler):
    server_version = "PVMBGMock/1.0"
    sys_version = ""

    def setup(self):
        super().setup()
        self.connection.settimeout(5)

    def log_message(self, format, *args):
        # The default implementation logs the raw request target. Emit only
        # structured, allowlisted fields from dispatch instead.
        return

    def send_error(self, code, message=None, explain=None):
        # Parser-level failures and unsupported methods also get the same
        # correlation/error envelope instead of the default HTML response.
        self.correlation_id = str(uuid.uuid4())
        self.route = "/"
        headers = getattr(self, "headers", None)
        if headers is not None:
            supplied = headers.get_all("X-Correlation-ID", [])
            if len(supplied) == 1 and CORRELATION_ID.fullmatch(supplied[0]):
                self.correlation_id = supplied[0]
        title = self.responses.get(code, ("HTTP error",))[0]
        self.problem(code, title, "The HTTP request could not be processed.", "invalid-http-request")
        log_event("warning", "invalid_http_request", code, self.correlation_id)

    def do_GET(self):
        self.dispatch()

    def do_POST(self):
        self.dispatch()

    def do_PUT(self):
        self.dispatch()

    def do_DELETE(self):
        self.dispatch()

    def do_PATCH(self):
        self.dispatch()

    def do_HEAD(self):
        self.dispatch()

    def do_OPTIONS(self):
        self.dispatch()

    @property
    def app(self) -> Application:
        return self.server.application

    def send_json(self, status: int, value: dict, headers: dict | None = None,
                  content_type: str = "application/json") -> None:
        self.response_status = status
        payload = json.dumps(value, separators=(",", ":"), allow_nan=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("X-Correlation-ID", self.correlation_id)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        for key, value in (headers or {}).items():
            self.send_header(key, str(value))
        self.end_headers()
        self.close_connection = True
        if self.command != "HEAD":
            self.wfile.write(payload)

    def problem(self, status: int, title: str, detail: str, code: str,
                headers: dict | None = None) -> None:
        self.send_json(status, {
            "type": f"urn:pvmbg:problem:{code}",
            "title": title,
            "status": status,
            "detail": detail,
            "instance": self.route,
            "correlation_id": self.correlation_id,
        }, headers, "application/problem+json")

    def unavailable(self):
        self.problem(503, "Source unavailable", "PVMBG mock is temporarily unavailable.",
                     "source-unavailable", {"Retry-After": "1"})

    def authorize(self) -> bool:
        values = self.headers.get_all("Authorization", [])
        valid = False
        if len(values) == 1:
            scheme, separator, token = values[0].partition(" ")
            if scheme.lower() == "bearer" and separator and token:
                valid = hmac.compare_digest(token.encode("utf-8"), self.app.config.bearer_token.encode("utf-8"))
        if not valid:
            self.problem(401, "Unauthorized", "A valid PVMBG bearer token is required.", "unauthorized",
                         {"WWW-Authenticate": 'Bearer realm="pvmbg"'})
        return valid

    def body(self) -> dict:
        lengths = self.headers.get_all("Content-Length", [])
        if self.headers.get_all("Transfer-Encoding") or len(lengths) != 1 or not re.fullmatch(r"[0-9]+", lengths[0]):
            raise ValueError("A valid Content-Length is required")
        length = int(lengths[0])
        if not 0 < length <= MAX_BODY_BYTES:
            raise ValueError("JSON body must contain between 1 and 4096 bytes")
        types = self.headers.get_all("Content-Type", [])
        if len(types) != 1 or types[0].split(";", 1)[0].strip().lower() != "application/json":
            raise ValueError("Content-Type must be application/json")
        try:
            raw = self.rfile.read(length)
            if len(raw) != length:
                raise ValueError("JSON body is shorter than Content-Length")
            value = json.loads(raw.decode("utf-8"), object_pairs_hook=strict_object, parse_constant=invalid_number)
        except (UnicodeDecodeError, json.JSONDecodeError, TimeoutError, RecursionError) as exc:
            raise ValueError("Body must contain a valid JSON object") from exc
        if not isinstance(value, dict):
            raise ValueError("Body must contain a JSON object")
        return value

    def dispatch(self):
        started = time.monotonic()
        self.correlation_id = str(uuid.uuid4())
        self.response_status = 500
        self.route = "/"
        self.operation = "unknown_route"
        traceparent = None
        trace_values = self.headers.get_all("traceparent", [])
        if len(trace_values) == 1:
            match = TRACEPARENT.fullmatch(trace_values[0])
            if match and int(match[1], 16) and int(match[2], 16):
                traceparent = trace_values[0]
        routes = {
            "/volcanic-reports": ("GET", "list_volcanic_reports"),
            "/admin/schema-version": ("POST", "set_schema_version"),
            "/admin/outage": ("POST", "set_outage"),
            "/health": ("GET", "health"),
        }
        try:
            target = urlsplit(self.path)
            if target.path in routes:
                self.route = target.path
                self.operation = routes[self.route][1]
            correlation = self.headers.get_all("X-Correlation-ID", [])
            if correlation:
                if len(correlation) != 1 or not CORRELATION_ID.fullmatch(correlation[0]):
                    raise ValueError("X-Correlation-ID must contain 1 to 128 letters, digits, dots, underscores, colons or hyphens")
                self.correlation_id = correlation[0]
            if target.scheme or target.netloc or target.fragment or "#" in self.path:
                raise ValueError("Use a relative request path without a URI fragment")
            if target.path not in routes:
                self.problem(404, "Not found", "This endpoint does not exist.", "not-found")
                return
            self.route = target.path
            expected_method, self.operation = routes[self.route]
            if self.route != "/health" and not self.authorize():
                return
            if self.command != expected_method:
                self.problem(405, "Method not allowed", "This HTTP method is not supported by this endpoint.",
                             "method-not-allowed", {"Allow": expected_method})
                return
            if self.route != "/volcanic-reports" and target.query:
                raise ValueError("This endpoint does not accept query parameters")
            if self.route == "/volcanic-reports":
                self.list_reports(target.query)
            elif self.route == "/health":
                self.health()
            else:
                self.admin()
        except ValueError as exc:
            self.problem(400, "Bad request", str(exc), "bad-request")
        except (sqlite3.Error, OSError):
            try:
                self.unavailable()
            except OSError:
                pass
        except Exception:
            # Never expose exception text, credentials, stored payloads, or bodies.
            try:
                self.unavailable()
            except OSError:
                pass
        finally:
            log_event("info" if self.response_status < 400 else "warning", self.operation,
                      self.response_status, self.correlation_id, (time.monotonic() - started) * 1000, traceparent)

    def list_reports(self, query: str):
        with self.app.lock:
            outage = self.app.outage
        if outage:
            self.unavailable()
            return
        if re.search(r"%(?![0-9A-Fa-f]{2})", query):
            raise ValueError("Invalid query encoding")
        try:
            params = parse_qs(query, keep_blank_values=True, strict_parsing=True, encoding="utf-8", errors="strict")
        except ValueError as exc:
            raise ValueError("Query parameters must use valid URL encoding and key=value syntax") from exc
        if set(params) - {"since"} or len(params.get("since", [])) > 1:
            raise ValueError("Only one optional since query parameter is supported")
        since_us = timestamp_us(params["since"][0]) if "since" in params else None
        delay = random.uniform(self.app.config.delay_min_ms, self.app.config.delay_max_ms) / 1000
        if self.app.stop.wait(delay):
            self.unavailable()
            return
        reports = self.app.store.reports(since_us)
        # Project a single active version under the lock so header and every
        # item describe the same snapshot, even if an admin command races a read.
        with self.app.lock:
            outage = self.app.outage
            if not outage:
                version = self.app.version
                if version == 1:
                    for report in reports:
                        report.pop("confidence_level", None)
        if outage:
            self.unavailable()
            return
        self.send_json(200, {"items": reports}, {"X-PVMBG-Schema-Version": str(version)})

    def health(self):
        with self.app.lock:
            unhealthy = self.app.outage or not self.app.generator_ok or not self.app.generator.is_alive()
        if unhealthy or self.app.stop.is_set():
            self.unavailable()
            return
        self.app.store.probe()
        self.send_json(200, {"status": "ok"})

    def admin(self):
        value = self.body()
        if self.route == "/admin/schema-version":
            if set(value) != {"version"} or type(value["version"]) is not int or value["version"] not in (1, 2):
                raise ValueError("Body must contain only version, an integer equal to 1 or 2")
            with self.app.lock:
                self.app.version = value["version"]
                result = {"version": self.app.version}
        else:
            if set(value) != {"enabled"} or type(value["enabled"]) is not bool:
                raise ValueError("Body must contain only enabled, a boolean")
            with self.app.lock:
                self.app.outage = value["enabled"]
                result = {"enabled": self.app.outage}
        self.send_json(200, result)


def main() -> int:
    try:
        config = Config.from_env()
    except ValueError:
        log_event("error", "configuration_error", 500)
        return 2
    try:
        application = Application(config)
        server = Server(application)
    except (sqlite3.Error, OSError):
        log_event("error", "startup", 503)
        return 1

    def stop(signum, frame):
        application.stop.set()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    serving = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.1}, name="pvmbg-http")
    application.generator.start()
    serving.start()
    log_event("info", "startup", 200)
    try:
        while not application.stop.wait(0.5):
            if not serving.is_alive():
                application.stop.set()
                log_event("error", "http_server_stopped", 503)
    finally:
        application.stop.set()
        server.shutdown()
        serving.join()
        server.server_close()
        application.generator.join()
        log_event("info", "shutdown", 200)
    return 0


if __name__ == "__main__":
    sys.exit(main())
