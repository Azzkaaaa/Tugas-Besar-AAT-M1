"""Black-box tests: real HTTP, independent processes, temporary SQLite files."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime, timedelta, timezone
import http.client
import json
import os
from pathlib import Path
import secrets
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from urllib.parse import urlencode


ROOT = Path(__file__).resolve().parents[1]


def instant(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00").replace("z", "+00:00"))


class RunningMock:
    def __init__(self, source, **overrides):
        self.source = source
        self.prefix = source.upper()
        self.temp = tempfile.TemporaryDirectory(prefix=f"{source}-test-")
        self.directory = Path(self.temp.name)
        self.database = self.directory / f"{source}.sqlite3"
        self.log_path = self.directory / "server.log"
        self.secret = secrets.token_urlsafe(32)
        self.secret_name = "BMKG_API_KEY" if source == "bmkg" else "PVMBG_BEARER_TOKEN"
        self.env = {
            k: v for k, v in os.environ.items()
            if not k.startswith(("BMKG_", "PVMBG_", "MOCK_"))
        }
        self.env.update({
            self.secret_name: self.secret,
            f"{self.prefix}_STORE_DSN": "sqlite:///" + self.database.as_posix(),
            "MOCK_SEED_COUNT": "20", "MOCK_EVENT_INTERVAL_MS": "10000",
            "BMKG_RESPONSE_DELAY_MS": "100",
            "PVMBG_RESPONSE_DELAY_MIN_MS": "5", "PVMBG_RESPONSE_DELAY_MAX_MS": "10",
        })
        self.env.update({k: str(v) for k, v in overrides.items()})
        # Provision only variables owned by the child process.
        other = "PVMBG_" if source == "bmkg" else "BMKG_"
        self.env = {k: v for k, v in self.env.items() if not k.startswith(other)}
        self.process = None
        self.log_file = None

    @property
    def auth(self):
        if self.source == "bmkg":
            return {"X-BMKG-Key": self.secret}
        return {"Authorization": f"Bearer {self.secret}"}

    def start(self):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
        self.env[f"{self.prefix}_HTTP_PORT"] = str(self.port)
        self.log_file = self.log_path.open("ab")
        self.process = subprocess.Popen(
            [sys.executable, "-u", str(ROOT / "services" / f"{self.source}-mock" / "app.py")],
            cwd=ROOT, env=self.env, stdout=self.log_file, stderr=subprocess.STDOUT,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
        )
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise AssertionError("Mock failed to start: " + self.log_path.read_text(errors="replace"))
            try:
                if self.request("/health", auth=False)[0] == 200:
                    return self
            except (OSError, http.client.HTTPException):
                pass
            time.sleep(0.03)
        raise AssertionError("Mock did not become healthy")

    def stop(self):
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=6)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=3)
        if self.log_file:
            self.log_file.close()
            self.log_file = None

    def close(self):
        self.stop()
        self.temp.cleanup()

    def request(self, path, *, method="GET", body=None, raw=None, headers=None, auth=True):
        request_headers = self.auth if auth else {}
        request_headers.update(headers or {})
        if body is not None:
            raw = json.dumps(body).encode()
            request_headers["Content-Type"] = "application/json"
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=8)
        try:
            connection.request(method, path, body=raw, headers=request_headers)
            response = connection.getresponse()
            payload = json.loads(response.read())
            return response.status, dict(response.getheaders()), payload
        finally:
            connection.close()

    def items(self, path):
        status, _, payload = self.request(path)
        if status != 200:
            raise AssertionError(f"Unexpected status {status}: {payload}")
        return payload["items"]

    def admin(self, path, body):
        status, _, payload = self.request("/admin/" + path, method="POST", body=body)
        if status != 200:
            raise AssertionError(f"Admin returned {status}: {payload}")
        return payload


class MockContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bmkg = RunningMock("bmkg")
        cls.pvmbg = RunningMock("pvmbg")
        cls.addClassCleanup(cls.bmkg.close)
        cls.addClassCleanup(cls.pvmbg.close)
        cls.bmkg.start()
        cls.pvmbg.start()

    def setUp(self):
        self.pvmbg.admin("outage", {"enabled": False})
        self.pvmbg.admin("schema-version", {"version": 1})

    def assert_problem(self, response, status):
        code, headers, body = response
        self.assertEqual(code, status)
        self.assertEqual(headers["Content-Type"].split(";")[0], "application/problem+json")
        self.assertEqual(set(body), {"type", "title", "status", "detail", "instance", "correlation_id"})
        self.assertEqual(body["status"], status)
        self.assertEqual(headers["X-Correlation-ID"], body["correlation_id"])
        self.assertRegex(body["correlation_id"], r"^[A-Za-z0-9._:-]{1,128}$")
        self.assertNotIn(self.bmkg.secret, json.dumps(body))
        self.assertNotIn(self.pvmbg.secret, json.dumps(body))
        if status == 401:
            self.assertIn("WWW-Authenticate", headers)
        if status == 503:
            self.assertGreaterEqual(int(headers["Retry-After"]), 1)

    def test_seed_payloads_and_warning_relationship(self):
        events = self.bmkg.items("/seismic-events")
        reports = self.pvmbg.items("/volcanic-reports")
        warnings = self.bmkg.items("/tsunami-warnings")
        self.assertGreaterEqual(len(events), 20)
        self.assertGreaterEqual(len(reports), 20)
        self.assertTrue(warnings)
        for values, key in ((events, "event_id"), (reports, "report_id"), (warnings, "warning_id")):
            self.assertEqual(len(values), len({item[key] for item in values}))
        event_map = {event["event_id"]: event for event in events}
        for event in events:
            self.assertTrue({"event_id", "magnitude", "depth_km", "epicenter_lat", "epicenter_lon",
                             "region_name", "occurred_at", "potential_tsunami"} <= event.keys())
            self.assertIs(type(event["potential_tsunami"]), bool)
            self.assertGreaterEqual(event["depth_km"], 0)
            self.assertTrue(-90 <= event["epicenter_lat"] <= 90)
            self.assertTrue(-180 <= event["epicenter_lon"] <= 180)
            self.assertIsNotNone(instant(event["occurred_at"]).tzinfo)
        for warning in warnings:
            self.assertTrue(event_map[warning["related_event_id"]]["potential_tsunami"])
            self.assertIn(warning["threat_level"], {"Waspada", "Siaga", "Awas"})
            self.assertIsInstance(warning["affected_zones"], list)
            self.assertNotIn("updated_at", warning)
        self.assertEqual({r["alert_level"] for r in reports}, {"Normal", "Waspada", "Siaga", "Awas"})
        for report in reports:
            self.assertTrue({"report_id", "volcano_id", "alert_level", "eruption_count_24h",
                             "ash_column_height_m", "reported_at"} <= report.keys())
            self.assertNotIn("confidence_level", report)
            self.assertIs(type(report["eruption_count_24h"]), int)
            self.assertGreaterEqual(report["ash_column_height_m"], 0)

    def test_cross_credentials_rejected_even_in_expected_header(self):
        cases = [
            (self.bmkg, "/seismic-events", {}),
            (self.pvmbg, "/volcanic-reports", {}),
            (self.bmkg, "/seismic-events", self.pvmbg.auth),
            (self.pvmbg, "/volcanic-reports", self.bmkg.auth),
            (self.bmkg, "/seismic-events", {"X-BMKG-Key": self.pvmbg.secret}),
            (self.pvmbg, "/volcanic-reports", {"Authorization": f"Bearer {self.bmkg.secret}"}),
            (self.bmkg, "/seismic-events", {"X-BMKG-Key": "é"}),
            (self.pvmbg, "/volcanic-reports", {"Authorization": "Bearer é"}),
        ]
        for service, path, headers in cases:
            with self.subTest(source=service.source, path=path):
                self.assert_problem(service.request(path, auth=False, headers=headers), 401)
        self.assert_problem(self.pvmbg.request("/admin/outage", method="POST", body={"enabled": True}, auth=False), 401)

    def test_health_is_public_and_correlation_propagated(self):
        for service in (self.bmkg, self.pvmbg):
            status, headers, body = service.request("/health", auth=False, headers={"X-Correlation-ID": "integration.demo:42"})
            self.assertEqual((status, body), (200, {"status": "ok"}))
            self.assertEqual(headers["X-Correlation-ID"], "integration.demo:42")
            for correlation in ("invalid id", "", "a" * 129):
                self.assert_problem(service.request("/health", headers={"X-Correlation-ID": correlation}), 400)

    def test_since_is_inclusive_sorted_and_timezone_normalized(self):
        for service, path, date_key, id_key in (
            (self.bmkg, "/seismic-events", "occurred_at", "event_id"),
            (self.pvmbg, "/volcanic-reports", "reported_at", "report_id"),
        ):
            all_items = service.items(path)
            self.assertEqual(all_items, sorted(all_items, key=lambda item: (instant(item[date_key]), item[id_key])))
            boundary = all_items[len(all_items) // 2]
            cutoff = instant(boundary[date_key])
            expected = [item[id_key] for item in all_items if instant(item[date_key]) >= cutoff]
            for stamp in (boundary[date_key], cutoff.astimezone(timezone(timedelta(hours=7))).isoformat()):
                actual = service.items(path + "?" + urlencode({"since": stamp}))
                # Generation can append concurrently, but cannot remove/reorder snapshot IDs.
                self.assertEqual([item[id_key] for item in actual][:len(expected)], expected)
            future = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
            self.assertEqual(service.items(path + "?" + urlencode({"since": future})), [])

    def test_invalid_since_and_duplicate_query_are_bad_requests(self):
        queries = ["since=", "since=2026-09-29", "since=2026-09-29T12:00:00",
                   "since=2026-02-30T12:00:00Z", "since=2026-09-29T12:00:00%2B25:00",
                   "since=2026-09-29T12:00:00Z&since=2026-09-29T12:00:00Z", "limit=1"]
        for service, path in ((self.bmkg, "/seismic-events"), (self.bmkg, "/tsunami-warnings"),
                              (self.pvmbg, "/volcanic-reports")):
            for query in queries:
                with self.subTest(source=service.source, query=query):
                    self.assert_problem(service.request(path + "?" + query), 400)

    def test_admin_strict_types_and_malformed_json(self):
        for body in ({"version": True}, {"version": "2"}, {"version": 2.0}, {"version": 3},
                     {"version": 2, "extra": 1}, {}, []):
            self.assert_problem(self.pvmbg.request("/admin/schema-version", method="POST", body=body), 400)
        for body in ({"enabled": 1}, {"enabled": "false"}, {"enabled": False, "extra": 1}, {}):
            self.assert_problem(self.pvmbg.request("/admin/outage", method="POST", body=body), 400)
        for raw in (b'{', b'{"version":1,"version":2}', b'{"version":NaN}'):
            self.assert_problem(self.pvmbg.request("/admin/schema-version", method="POST", raw=raw,
                                                 headers={"Content-Type": "application/json"}), 400)

    def test_outage_recovery_and_auth_precedence(self):
        original = {r["report_id"] for r in self.pvmbg.items("/volcanic-reports")}
        self.assertEqual(self.pvmbg.admin("outage", {"enabled": True}), {"enabled": True})
        self.assert_problem(self.pvmbg.request("/health", auth=False), 503)
        self.assert_problem(self.pvmbg.request("/volcanic-reports"), 503)
        self.assert_problem(self.pvmbg.request("/volcanic-reports", auth=False), 401)
        self.assertEqual(self.bmkg.request("/seismic-events")[0], 200)
        self.assertEqual(self.pvmbg.admin("schema-version", {"version": 2}), {"version": 2})
        self.pvmbg.admin("outage", {"enabled": False})
        status, headers, body = self.pvmbg.request("/volcanic-reports")
        self.assertEqual(status, 200)
        self.assertEqual(headers["X-PVMBG-Schema-Version"], "2")
        self.assertTrue(original <= {r["report_id"] for r in body["items"]})

    def test_restart_preserves_rows_and_resets_runtime_controls(self):
        for service, path, key in ((self.bmkg, "/seismic-events", "event_id"),
                                   (self.pvmbg, "/volcanic-reports", "report_id")):
            records = {r[key]: r for r in service.items(path)}
            if service.source == "pvmbg":
                service.admin("schema-version", {"version": 2})
                service.admin("outage", {"enabled": True})
            service.stop()
            self.assertTrue(service.database.exists())
            with closing(sqlite3.connect(service.database)) as db:
                self.assertEqual(db.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            service.start()
            current = {r[key]: r for r in service.items(path)}
            for identifier, record in records.items():
                self.assertEqual(current[identifier], record)
            # No extra batch of historical seed records on restart.
            self.assertLessEqual(len(current) - len(records), 2)
            if service.source == "pvmbg":
                self.assertEqual(service.request(path)[1]["X-PVMBG-Schema-Version"], "1")

    def test_logs_have_correlation_duration_and_no_credentials(self):
        for service, path in ((self.bmkg, "/seismic-events"), (self.pvmbg, "/volcanic-reports")):
            marker = "log-check-" + secrets.token_hex(8)
            traceparent = "00-" + secrets.token_hex(16) + "-" + secrets.token_hex(8) + "-01"
            service.request(path, headers={"X-Correlation-ID": marker, "traceparent": traceparent})
            deadline = time.monotonic() + 1
            while time.monotonic() < deadline:
                logs = service.log_path.read_text(encoding="utf-8", errors="replace")
                if marker in logs:
                    break
                time.sleep(0.01)
            self.assertNotIn(service.secret, logs)
            records = [json.loads(line) for line in logs.splitlines() if line.startswith("{")]
            matching = [line for line in records if line.get("correlation_id") == marker]
            self.assertTrue(matching)
            self.assertTrue({"timestamp", "level", "service", "correlation_id", "operation",
                             "duration_ms", "status"} <= matching[-1].keys())
            self.assertGreaterEqual(matching[-1]["duration_ms"], 0)


class DynamicBehaviorTests(unittest.TestCase):
    def start_mock(self, source, **env):
        mock = RunningMock(source, **env)
        self.addCleanup(mock.close)
        return mock.start()

    def eventually(self, fn, timeout=4):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            result = fn()
            if result:
                return result
            time.sleep(0.03)
        self.fail("Expected background state did not appear")

    def test_generation_schema_evolution_outage_catchup_and_persistence(self):
        service = self.start_mock("pvmbg", MOCK_EVENT_INTERVAL_MS=100)
        initial = service.items("/volcanic-reports")
        service.admin("schema-version", {"version": 2})
        v2 = self.eventually(lambda: [r for r in service.items("/volcanic-reports") if "confidence_level" in r])
        for report in v2:
            self.assertTrue(0 <= report["confidence_level"] <= 1)
        service.admin("schema-version", {"version": 1})
        self.assertTrue(all("confidence_level" not in r for r in service.items("/volcanic-reports")))
        service.admin("schema-version", {"version": 2})
        visible = {r["report_id"]: r for r in service.items("/volcanic-reports")}
        for record in v2:
            self.assertEqual(visible[record["report_id"]], record)
        self.assertTrue(any("confidence_level" not in r for r in visible.values()))
        service.admin("outage", {"enabled": True})
        time.sleep(0.4)
        service.admin("outage", {"enabled": False})
        self.assertGreater(len(service.items("/volcanic-reports")), len(visible))
        service.stop()
        service.start()
        self.assertTrue(all("confidence_level" not in r for r in service.items("/volcanic-reports")))
        service.admin("schema-version", {"version": 2})
        restored = {r["report_id"]: r for r in service.items("/volcanic-reports")}
        for report in initial + v2:
            self.assertEqual(restored[report["report_id"]], report)

    def test_bmkg_generator_and_warning_watermark_not_arrival(self):
        service = self.start_mock("bmkg", MOCK_EVENT_INTERVAL_MS=100, BMKG_RESPONSE_DELAY_MS=50)
        initial = service.items("/seismic-events")
        started = datetime.now(timezone.utc).isoformat()
        new_warnings = self.eventually(lambda: service.items("/tsunami-warnings?" + urlencode({"since": started})))
        self.assertGreater(len(service.items("/seismic-events")), len(initial))
        for warning in new_warnings:
            self.assertGreater(instant(warning["estimated_arrival"]), instant(started))
        # Future arrival is not the update time, so these warnings must be excluded.
        future = (datetime.now(timezone.utc) + timedelta(minutes=1)).isoformat()
        self.assertEqual(service.items("/tsunami-warnings?" + urlencode({"since": future})), [])

    def test_slow_pvmbg_does_not_block_health_admin_or_bmkg(self):
        bmkg = self.start_mock("bmkg")
        pvmbg = self.start_mock("pvmbg", PVMBG_RESPONSE_DELAY_MIN_MS=3000, PVMBG_RESPONSE_DELAY_MAX_MS=3000)
        with ThreadPoolExecutor(max_workers=4) as executor:
            slow = [executor.submit(pvmbg.request, "/volcanic-reports") for _ in range(3)]
            time.sleep(0.1)
            started = time.monotonic()
            self.assertEqual(pvmbg.request("/health", auth=False)[0], 200)
            pvmbg.admin("schema-version", {"version": 2})
            self.assertEqual(bmkg.request("/seismic-events")[0], 200)
            self.assertLess(time.monotonic() - started, 1.0)
            self.assertTrue(all(not result.done() for result in slow))
            pvmbg.admin("outage", {"enabled": True})
            for result in slow:
                # Even reads that were already sleeping must observe the outage.
                self.assertEqual(result.result()[0], 503)

    def test_retained_collection_is_not_capped_at_seed_default(self):
        for source, path in (("bmkg", "/seismic-events"), ("pvmbg", "/volcanic-reports")):
            service = self.start_mock(source, MOCK_SEED_COUNT=37)
            self.assertEqual(len(service.items(path)), 37)

    def test_bmkg_configured_delay_is_applied(self):
        service = self.start_mock("bmkg", BMKG_RESPONSE_DELAY_MS=150)
        started = time.monotonic()
        service.items("/seismic-events")
        self.assertGreaterEqual(time.monotonic() - started, 0.14)

    def test_invalid_configuration_fails_before_listening(self):
        cases = [
            ("bmkg", {"BMKG_API_KEY": "REPLACE_BMKG_ONLY"}),
            ("pvmbg", {"PVMBG_BEARER_TOKEN": ""}),
            ("bmkg", {"BMKG_RESPONSE_DELAY_MS": "49"}),
            ("pvmbg", {"PVMBG_RESPONSE_DELAY_MIN_MS": "100", "PVMBG_RESPONSE_DELAY_MAX_MS": "99"}),
            ("bmkg", {"MOCK_SEED_COUNT": "19"}),
            ("pvmbg", {"MOCK_EVENT_INTERVAL_MS": "10001"}),
            ("bmkg", {"BMKG_STORE_DSN": "sqlite:///:memory:"}),
            ("pvmbg", {"PVMBG_STORE_DSN": "sqlite:///:memory:"}),
        ]
        for source, overrides in cases:
            with self.subTest(source=source, configuration=list(overrides)):
                mock = RunningMock(source, **overrides)
                try:
                    result = subprocess.run(
                        [sys.executable, str(ROOT / "services" / f"{source}-mock" / "app.py")],
                        cwd=ROOT, env=mock.env, capture_output=True, timeout=10,
                        creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
                    )
                    self.assertNotEqual(result.returncode, 0)
                    self.assertNotIn(mock.secret.encode(), result.stdout + result.stderr)
                finally:
                    mock.close()


if __name__ == "__main__":
    unittest.main()
