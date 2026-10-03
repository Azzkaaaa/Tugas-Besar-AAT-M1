"""Private persistent report storage. No BNPB or BMKG database is accessed."""

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import random
import re
import sqlite3
import uuid


VOLCANO_IDS = ("merapi", "semeru", "anak-krakatau", "marapi", "lewotobi-laki-laki")
ALERT_LEVELS = ("Normal", "Waspada", "Siaga", "Awas")
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
RFC3339 = re.compile(
    r"^\d{4}-\d{2}-\d{2}[Tt]\d{2}:\d{2}:\d{2}(?:\.(\d+))?"
    r"(?:[Zz]|[+-](?:[01]\d|2[0-3]):[0-5]\d)$"
)


def timestamp_us(value: str) -> int:
    """Parse an inclusive lower bound without floating-point ordering errors."""
    match = RFC3339.fullmatch(value)
    if not match:
        raise ValueError("since must be an RFC 3339 timestamp with timezone")
    try:
        parsed = datetime.fromisoformat(value.upper().replace("Z", "+00:00"))
        delta = parsed.astimezone(timezone.utc) - EPOCH
    except (ValueError, OverflowError) as exc:
        raise ValueError("since must be a valid RFC 3339 timestamp") from exc
    fraction = match.group(1) or ""
    # For sub-microsecond lower bounds, ceil instead of truncate: a report at
    # .123456 is strictly older than since=.1234561 and must be excluded.
    ceiling = 1 if any(digit != "0" for digit in fraction[6:]) else 0
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds + ceiling


def utc_now_us() -> int:
    delta = datetime.now(timezone.utc) - EPOCH
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def format_timestamp(value: int) -> str:
    return (EPOCH + timedelta(microseconds=value)).isoformat(timespec="microseconds").replace("+00:00", "Z")


def make_report(report_id: str, ordinal: int, reported_us: int, version: int) -> dict:
    level_index = ordinal % len(ALERT_LEVELS)
    report = {
        "report_id": report_id,
        "volcano_id": VOLCANO_IDS[ordinal % len(VOLCANO_IDS)],
        "alert_level": ALERT_LEVELS[level_index],
        "eruption_count_24h": 0 if level_index == 0 else level_index + ordinal % 7,
        "ash_column_height_m": 0.0 if level_index == 0 else float(level_index * 500 + ordinal % 6 * 100),
        "reported_at": format_timestamp(reported_us),
    }
    if version == 2:
        report["confidence_level"] = round(random.uniform(0.0, 1.0), 3)
    return report


class Store:
    def __init__(self, database: Path):
        self.database = database

    @contextmanager
    def connection(self):
        connection = sqlite3.connect(self.database, timeout=5.0)
        try:
            connection.execute("PRAGMA busy_timeout=5000")
            yield connection
        finally:
            connection.close()

    def initialize(self, seed_count: int) -> None:
        self.database.parent.mkdir(parents=True, exist_ok=True)
        now_us = utc_now_us()
        with self.connection() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            with connection:
                connection.execute("""
                    CREATE TABLE IF NOT EXISTS volcanic_reports (
                        report_id TEXT PRIMARY KEY,
                        reported_at_us INTEGER NOT NULL,
                        payload TEXT NOT NULL
                    )
                """)
                connection.execute("""
                    CREATE INDEX IF NOT EXISTS volcanic_reports_time
                    ON volcanic_reports(reported_at_us, report_id)
                """)
                for ordinal in range(seed_count):
                    report_id = f"pvmbg-seed-{ordinal + 1:06d}"
                    reported_us = now_us - (seed_count - ordinal) * 60_000_000
                    payload = make_report(report_id, ordinal, reported_us, version=1)
                    connection.execute(
                        "INSERT OR IGNORE INTO volcanic_reports VALUES (?, ?, ?)",
                        (report_id, reported_us, json.dumps(payload, separators=(",", ":"))),
                    )

    def append(self, version: int) -> None:
        with self.connection() as connection, connection:
            # A backwards clock step cannot hide newly generated reports behind
            # a previously observed incremental-polling watermark.
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT MAX(reported_at_us) FROM volcanic_reports").fetchone()
            reported_us = max(utc_now_us(), (row[0] or 0) + 1)
            report_id = f"pvmbg-{uuid.uuid4().hex}"
            payload = make_report(report_id, random.randrange(1000000), reported_us, version)
            connection.execute(
                "INSERT INTO volcanic_reports VALUES (?, ?, ?)",
                (report_id, reported_us, json.dumps(payload, separators=(",", ":"))),
            )

    def reports(self, since_us: int | None) -> list[dict]:
        with self.connection() as connection:
            if since_us is None:
                cursor = connection.execute(
                    "SELECT payload FROM volcanic_reports ORDER BY reported_at_us, report_id"
                )
            else:
                cursor = connection.execute(
                    "SELECT payload FROM volcanic_reports WHERE reported_at_us >= ? "
                    "ORDER BY reported_at_us, report_id", (since_us,),
                )
            try:
                reports = [json.loads(row[0]) for row in cursor]
                if any(not isinstance(report, dict) for report in reports):
                    raise ValueError("Invalid stored report")
                return reports
            except (ValueError, TypeError) as exc:
                raise sqlite3.DatabaseError("Stored report could not be decoded") from exc

    def probe(self) -> None:
        with self.connection() as connection:
            connection.execute("SELECT report_id FROM volcanic_reports LIMIT 1").fetchone()
