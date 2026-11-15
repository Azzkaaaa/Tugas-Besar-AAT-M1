"""Submit, retry, and reconcile a small batch of Milestone 2 field reports."""

import argparse
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4


def request(url, token, *, body=None, key=None):
    headers = {"Authorization": f"Bearer {token}"}
    if body is not None:
        headers.update({"Idempotency-Key": key, "Content-Type": "application/json"})
        body = json.dumps(body, ensure_ascii=False).encode()
    req = Request(url, data=body, headers=headers)
    started = time.monotonic()
    try:
        with urlopen(req, timeout=5) as response:
            data = json.load(response)
            duration = round((time.monotonic() - started) * 1000, 2)
            return response.status, data, duration, response.headers.get("X-Correlation-ID")
    except HTTPError as error:
        raise RuntimeError(f"{req.get_method()} {req.selector}: HTTP {error.code}") from None


def submit(base, receipts, count):
    token = os.environ["FIELD_TOKEN"]
    existing = receipts.read_text(encoding="utf-8").splitlines() if receipts.exists() else []
    if existing:
        raise RuntimeError("use a new receipts file for each submission batch")
    durations = []
    with receipts.open("a", encoding="utf-8") as out:
        for index in range(count):
            key = str(uuid4())
            body = {"observed_at": datetime.now(timezone.utc).isoformat(), "area_name": "Kota Padang",
                    "description": f"M2 P3 demo report {index + 1}"}
            status, value, latency, correlation = request(f"{base}/field-reports", token, body=body, key=key)
            if status != 202 or not value.get("report_id"):
                raise RuntimeError(f"unexpected receipt response: HTTP {status}")
            print(json.dumps({"id": value["report_id"], "key": key, "body": body,
                              "ms": latency, "correlation_id": correlation}), file=out, flush=True)
            durations.append(latency)
    p95 = sorted(durations)[math.ceil(len(durations) * .95) - 1]
    print(json.dumps({"accepted": len(durations), "p95_ms": p95, "target_ms": 300}))


def retry(base, rows, count):
    token = os.environ["FIELD_TOKEN"]
    for row in rows[:count]:
        status, value, _, _ = request(f"{base}/field-reports", token, body=row["body"], key=row["key"])
        if status != 202 or value.get("report_id") != row["id"]:
            raise RuntimeError(f"receipt changed for report {row['id']}")
    print(json.dumps({"stable_receipts": min(len(rows), count)}))


def verify(base, rows):
    token = os.environ["OPS_TOKEN"]
    by_id = {row["id"]: row for row in rows}
    if len(by_id) != len(rows):
        raise RuntimeError("the receipt file contains duplicate report IDs")
    results = {}
    for report_id, row in by_id.items():
        _, report, _, _ = request(f"{base}/internal/ops/field-reports/{report_id}", token)
        if report.get("report_id") != report_id or not report.get("processed_at") or any(
            report.get(field) != row["body"][field] for field in ("area_name", "description")
        ):
            raise RuntimeError(f"unexpected processed report for {report_id}")
        results[report_id] = report["processed_at"]
    print(json.dumps({"acknowledged_ids": sorted(by_id), "processed_ids": sorted(results),
                      "processed_at": results}, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("submit", "retry", "verify"))
    parser.add_argument("base_url")
    parser.add_argument("receipts", type=Path)
    parser.add_argument("--count", type=int, default=20)
    args = parser.parse_args()
    if args.count < 1:
        parser.error("--count must be positive")
    base = args.base_url.rstrip("/")
    if args.action == "submit":
        submit(base, args.receipts, args.count)
        return
    rows = [json.loads(line) for line in args.receipts.read_text(encoding="utf-8").splitlines()]
    if not rows:
        parser.error("the receipts file is empty")
    if args.action == "retry":
        retry(base, rows, args.count)
    else:
        verify(base, rows)


if __name__ == "__main__":
    main()
