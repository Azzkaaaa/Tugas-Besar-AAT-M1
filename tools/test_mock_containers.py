"""Test running mock containers through HTTP; optionally verify Docker restarts.

Reads owned settings from .env (shell overrides), never prints credentials.
Temporarily changes PVMBG controls, then restores schema v1 / outage disabled.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import http.client
import json
import os
from pathlib import Path
import secrets
import statistics
import subprocess
import sys
import time
from urllib.parse import urlencode


ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ["docker", "compose", "-f", str(ROOT / "compose.mocks.yaml")]
NAMES = {
    "BMKG_API_KEY", "PVMBG_BEARER_TOKEN", "BMKG_HTTP_PORT", "PVMBG_HTTP_PORT",
    "MOCK_EVENT_INTERVAL_MS", "BMKG_RESPONSE_DELAY_MS",
    "PVMBG_RESPONSE_DELAY_MIN_MS", "PVMBG_RESPONSE_DELAY_MAX_MS",
}


def configuration():
    values = {}
    path = ROOT / ".env"
    if path.exists():
        for line in path.read_text(encoding="utf-8-sig").splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                key, value = line.split("=", 1)
                if key.strip() in NAMES:
                    values[key.strip()] = value.strip().strip("\"'")
    values.update({key: os.environ[key] for key in NAMES if key in os.environ})
    for name in ("BMKG_API_KEY", "PVMBG_BEARER_TOKEN"):
        if not values.get(name) or values[name].startswith("REPLACE_"):
            raise ValueError("Run python tools/setup_mock_env.py before testing.")
    if values["BMKG_API_KEY"] == values["PVMBG_BEARER_TOKEN"]:
        raise ValueError("BMKG and PVMBG must have different credentials.")
    return values


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def parsed_time(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class Client:
    def __init__(self, source, port, credential, timeout):
        self.source, self.port, self.credential, self.timeout = source, port, credential, timeout

    def request(self, path, *, body=None, auth=True, headers=None):
        sent = {}
        if auth:
            sent = ({"X-BMKG-Key": self.credential} if self.source == "bmkg"
                    else {"Authorization": f"Bearer {self.credential}"})
        sent.update(headers or {})
        raw = None
        if body is not None:
            raw = json.dumps(body).encode()
            sent["Content-Type"] = "application/json"
        started = time.perf_counter()
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=self.timeout)
        try:
            connection.request("POST" if body is not None else "GET", path, body=raw, headers=sent)
            response = connection.getresponse()
            payload = json.loads(response.read())
            return response.status, dict(response.getheaders()), payload, (time.perf_counter() - started) * 1000
        finally:
            connection.close()

    def items(self, path):
        response = self.request(path)
        require(response[0] == 200, f"{self.source} {path.split('?')[0]} must return 200")
        require(isinstance(response[2].get("items"), list), "Missing items array")
        return response[2]["items"]

    def admin(self, control, body):
        response = self.request("/admin/" + control, body=body)
        require(response[0] == 200 and response[2] == body, "PVMBG admin control failed")
        return response


class Suite:
    def __init__(self, config):
        self.config = config
        self.interval = int(config.get("MOCK_EVENT_INTERVAL_MS", "10000")) / 1000
        self.max_delay = int(config.get("PVMBG_RESPONSE_DELAY_MAX_MS", "3000")) / 1000
        timeout = max(10, self.max_delay + 5)
        self.bmkg = Client("bmkg", int(config.get("BMKG_HTTP_PORT", "8081")), config["BMKG_API_KEY"], timeout)
        self.pvmbg = Client("pvmbg", int(config.get("PVMBG_HTTP_PORT", "8082")), config["PVMBG_BEARER_TOKEN"], timeout)
        self.results = []

    def check(self, name, test):
        started = time.perf_counter()
        details = test() or {}
        result = {"name": name, "status": "passed", "duration_ms": round((time.perf_counter() - started) * 1000, 2),
                  "details": details}
        self.results.append(result)
        print(f"PASS {name}", flush=True)

    def health(self):
        for client in (self.bmkg, self.pvmbg):
            response = client.request("/health", auth=False)
            require(response[0] == 200 and response[2] == {"status": "ok"}, "Public health must return 200/ok")

    def payloads(self):
        events = self.bmkg.items("/seismic-events")
        warnings = self.bmkg.items("/tsunami-warnings")
        reports = self.pvmbg.items("/volcanic-reports")
        require(len(events) >= 20 and len(reports) >= 20, "Each source must retain at least 20 records")
        mapped = {event["event_id"]: event for event in events}
        require(warnings, "Seed must include tsunami warnings")
        for warning in warnings:
            # A generator may commit a new event between these two HTTP reads.
            if warning["related_event_id"] not in mapped:
                mapped.update({event["event_id"]: event for event in self.bmkg.items("/seismic-events")})
            require(mapped[warning["related_event_id"]]["potential_tsunami"] is True, "Warning references an invalid earthquake")
            require("updated_at" not in warning, "Internal warning watermark leaked into payload")
        require(all("confidence_level" not in report for report in reports), "v1 must hide confidence")
        return {"seismic_events": len(events), "tsunami_warnings": len(warnings), "volcanic_reports": len(reports)}

    def authentication(self):
        cases = [
            (self.bmkg, "/seismic-events", {}), (self.pvmbg, "/volcanic-reports", {}),
            (self.bmkg, "/seismic-events", {"Authorization": f"Bearer {self.pvmbg.credential}"}),
            (self.pvmbg, "/volcanic-reports", {"X-BMKG-Key": self.bmkg.credential}),
            (self.bmkg, "/seismic-events", {"X-BMKG-Key": self.pvmbg.credential}),
            (self.pvmbg, "/volcanic-reports", {"Authorization": f"Bearer {self.bmkg.credential}"}),
        ]
        for client, path, headers in cases:
            response = client.request(path, auth=False, headers=headers)
            require(response[0] == 401 and "WWW-Authenticate" in response[1], "Missing/cross credential must be rejected")
        require(self.pvmbg.request("/admin/outage", body={"enabled": True}, auth=False)[0] == 401,
                "Unauthenticated admin must be rejected")
        return {"rejected_requests": len(cases) + 1}

    def validation(self):
        for client, path in ((self.bmkg, "/seismic-events"), (self.pvmbg, "/volcanic-reports")):
            correlation = "docker-test-" + secrets.token_hex(8)
            success = client.request(path, headers={"X-Correlation-ID": correlation})
            require(success[1].get("X-Correlation-ID") == correlation, "Correlation header not propagated")
            failure = client.request(path + "?since=invalid", headers={"X-Correlation-ID": correlation})
            require(failure[0] == 400, "Invalid since must return 400")
            require(failure[1]["Content-Type"].split(";")[0] == "application/problem+json", "Invalid error media type")
            require(failure[2]["correlation_id"] == correlation, "Problem correlation does not match header")
            require(set(failure[2]) == {"type", "title", "status", "detail", "instance", "correlation_id"}, "Invalid problem envelope")
        for path, body in (("/admin/schema-version", {"version": True}), ("/admin/outage", {"enabled": "true"})):
            require(self.pvmbg.request(path, body=body)[0] == 400, "Admin must enforce strict JSON types")

    def filtering(self):
        for client, path, date, key in ((self.bmkg, "/seismic-events", "occurred_at", "event_id"),
                                        (self.pvmbg, "/volcanic-reports", "reported_at", "report_id")):
            records = client.items(path)
            require(records == sorted(records, key=lambda item: (parsed_time(item[date]), item[key])), "Invalid record ordering")
            boundary = records[len(records) // 2]
            instant = parsed_time(boundary[date])
            offset = instant.astimezone(timezone(timedelta(hours=7))).isoformat()
            selected = client.items(path + "?" + urlencode({"since": offset}))
            require(boundary[key] in {record[key] for record in selected}, "Inclusive lower bound omitted boundary record")
            require(all(parsed_time(record[date]) >= instant for record in selected), "since admitted an older record")
        future = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
        require(self.bmkg.items("/tsunami-warnings?" + urlencode({"since": future})) == [], "Warning watermark is not update time")

    def generation_and_schema(self):
        before_eq = {row["event_id"] for row in self.bmkg.items("/seismic-events")}
        before_reports = {row["report_id"] for row in self.pvmbg.items("/volcanic-reports")}
        self.pvmbg.admin("schema-version", {"version": 2})
        deadline = time.monotonic() + self.interval + 2 * self.max_delay + 5
        added = []
        while time.monotonic() < deadline:
            response = self.pvmbg.request("/volcanic-reports")
            require(response[0] == 200 and response[1].get("X-PVMBG-Schema-Version") == "2", "Active schema header must be 2")
            added = [row for row in response[2]["items"] if row["report_id"] not in before_reports and "confidence_level" in row]
            if added:
                break
            time.sleep(0.2)
        require(added, "No new report with confidence appeared within the configured interval plus request budget")
        require(all(0 <= row["confidence_level"] <= 1 for row in added), "Confidence outside [0,1]")
        now_eq = {row["event_id"] for row in self.bmkg.items("/seismic-events")}
        # Each container has its own timer phase; a PVMBG tick does not imply
        # that BMKG has already had its next tick since its earlier snapshot.
        while not now_eq - before_eq and time.monotonic() < deadline:
            time.sleep(0.2)
            now_eq = {row["event_id"] for row in self.bmkg.items("/seismic-events")}
        require(now_eq - before_eq, "BMKG did not generate a new event")
        self.pvmbg.admin("schema-version", {"version": 1})
        require(all("confidence_level" not in row for row in self.pvmbg.items("/volcanic-reports")), "v1 projection leaked confidence")
        self.pvmbg.admin("schema-version", {"version": 2})
        visible = {row["report_id"]: row for row in self.pvmbg.items("/volcanic-reports")}
        require(all(visible[row["report_id"]] == row for row in added), "Switching schema deleted stored confidence")
        self.saved_v2 = added
        return {"new_earthquakes": len(now_eq - before_eq), "new_v2_reports": len(added)}

    def outage(self):
        before = {row["report_id"] for row in self.pvmbg.items("/volcanic-reports")}
        self.pvmbg.admin("outage", {"enabled": True})
        started = datetime.now(timezone.utc)
        for path in ("/health", "/volcanic-reports"):
            response = self.pvmbg.request(path, auth=path != "/health")
            require(response[0] == 503 and int(response[1].get("Retry-After", "0")) >= 1, "Outage must return 503 with Retry-After")
        require(self.pvmbg.request("/volcanic-reports", auth=False)[0] == 401, "Outage must not bypass auth")
        self.bmkg.items("/seismic-events")
        self.pvmbg.admin("schema-version", {"version": 2})
        time.sleep(self.interval + 0.5)
        ended = datetime.now(timezone.utc)
        self.pvmbg.admin("outage", {"enabled": False})
        after = self.pvmbg.items("/volcanic-reports")
        require(before <= {row["report_id"] for row in after}, "Outage lost retained data")
        during = [row for row in after if started <= parsed_time(row["reported_at"]) <= ended]
        require(during, "Generator did not persist reports during the outage")
        return {"reports_generated_during_outage": len(during), "outage_seconds": round((ended - started).total_seconds(), 2)}

    def latency(self):
        stats = {}
        for client, path, minimum in ((self.bmkg, "/seismic-events", int(self.config.get("BMKG_RESPONSE_DELAY_MS", "100"))),
                                      (self.pvmbg, "/volcanic-reports", int(self.config.get("PVMBG_RESPONSE_DELAY_MIN_MS", "500")))):
            samples = []
            for _ in range(6):
                response = client.request(path)
                require(response[0] == 200, "Latency sample request failed")
                samples.append(response[3])
            require(min(samples) >= minimum - 20, "Configured artificial delay appears absent")
            stats[client.source] = {"samples": len(samples), "min_ms": round(min(samples), 2),
                                    "median_ms": round(statistics.median(samples), 2), "max_ms": round(max(samples), 2)}
        return stats

    def concurrency(self):
        with ThreadPoolExecutor(max_workers=5) as executor:
            pending = [executor.submit(self.pvmbg.request, "/volcanic-reports") for _ in range(4)]
            time.sleep(0.1)
            health = self.pvmbg.request("/health", auth=False)
            admin = self.pvmbg.admin("schema-version", {"version": 2})
            quake = self.bmkg.request("/seismic-events")
            require(health[0] == 200 and quake[0] == 200, "Independent requests failed during PVMBG reads")
            require(health[3] < 1000 and admin[3] < 1000, "Health/admin blocked on data delay")
            require(all(future.result()[0] == 200 for future in pending), "Concurrent read failed")
        return {"pvmbg_parallel_reads": 4, "pvmbg_health_ms": round(health[3], 2),
                "pvmbg_admin_ms": round(admin[3], 2), "bmkg_data_ms": round(quake[3], 2)}

    def restarts(self):
        details = {}
        for client, path, key, other in ((self.bmkg, "/seismic-events", "event_id", self.pvmbg),
                                          (self.pvmbg, "/volcanic-reports", "report_id", self.bmkg)):
            snapshot = {row[key]: row for row in client.items(path)}
            if client.source == "pvmbg":
                self.pvmbg.admin("outage", {"enabled": True})
            with ThreadPoolExecutor(max_workers=1) as executor:
                restarting = executor.submit(subprocess.run, COMPOSE + ["restart", client.source + "-mock"],
                                             cwd=ROOT, capture_output=True, timeout=45)
                independent_checks = 0
                while not restarting.done():
                    require(other.request("/health", auth=False)[0] == 200, "Restart disrupted the other service")
                    independent_checks += 1
                    time.sleep(0.1)
                require(restarting.result().returncode == 0, "Docker restart failed; inspect docker compose ps/logs")
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                try:
                    if client.request("/health", auth=False)[0] == 200:
                        break
                except (OSError, http.client.HTTPException):
                    pass
                time.sleep(0.1)
            else:
                raise AssertionError("Service did not recover after restart")
            if client.source == "pvmbg":
                response = client.request(path)
                require(response[1].get("X-PVMBG-Schema-Version") == "1", "Restart must reset schema to v1")
                require(all("confidence_level" not in row for row in response[2]["items"]), "v1 confidence leaked after restart")
                client.admin("schema-version", {"version": 2})
            restored = {row[key]: row for row in client.items(path)}
            require(all(restored.get(identifier) == row for identifier, row in snapshot.items()), "Restart lost or changed persisted records")
            details[client.source] = {"records_preserved": len(snapshot), "other_service_health_checks": independent_checks}
        return details


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--restart-check", action="store_true", help="Also restart each mock container and verify persistence/isolation")
    parser.add_argument("--output", type=Path, default=ROOT / ".cache" / "mock-container-test-results.json")
    args = parser.parse_args()
    try:
        suite = Suite(configuration())
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    report = {"started_at": datetime.now(timezone.utc).isoformat(), "transport": "HTTP to published Docker ports",
              "restart_checks": args.restart_check, "results": suite.results}
    failed = False
    active = "initial_controls"
    try:
        suite.pvmbg.admin("outage", {"enabled": False})
        suite.pvmbg.admin("schema-version", {"version": 1})
        checks = [("public_health", suite.health), ("seed_payloads_and_relationships", suite.payloads),
                  ("authentication_isolation", suite.authentication), ("validation_and_correlation", suite.validation),
                  ("inclusive_timezone_filtering", suite.filtering), ("generation_and_schema_evolution", suite.generation_and_schema),
                  ("outage_recovery_and_catchup", suite.outage), ("response_latency_samples", suite.latency),
                  ("concurrent_request_isolation", suite.concurrency)]
        if args.restart_check:
            checks.append(("container_restart_persistence_and_isolation", suite.restarts))
        for active, test in checks:
            suite.check(active, test)
    except Exception as exc:
        failed = True
        # Assertions contain only controlled messages; unexpected exception text
        # may contain environment-specific details, so print its type only.
        message = str(exc) if isinstance(exc, AssertionError) else type(exc).__name__
        suite.results.append({"name": active, "status": "failed", "error": message})
        print(f"FAIL {active}: {message}", flush=True)
    finally:
        try:
            suite.pvmbg.admin("outage", {"enabled": False})
            suite.pvmbg.admin("schema-version", {"version": 1})
            suite.health()
            report["final_state"] = {"health": "both ok", "pvmbg_schema": 1, "pvmbg_outage": False}
        except Exception:
            failed = True
            report["final_state"] = "recovery_failed"
            print("FAIL cleanup: check containers and restore PVMBG controls manually", flush=True)
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        report["status"] = "failed" if failed else "passed"
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"{len(suite.results)} checks; {report['status']}. Report: {args.output}", flush=True)
    return int(failed)


if __name__ == "__main__":
    sys.exit(main())
