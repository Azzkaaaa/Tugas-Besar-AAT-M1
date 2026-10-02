"""SQLite persistence and deterministic mock data owned exclusively by BMKG."""

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import re
import sqlite3
from uuid import uuid4


UTC = timezone.utc
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
RFC3339 = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}[Tt][0-9]{2}:[0-9]{2}:[0-5][0-9]"
    r"(?:\.([0-9]+))?(?:[Zz]|[+-](?:[01][0-9]|2[0-3]):[0-5][0-9])$"
)
THREAT_LEVELS = ("Waspada", "Siaga", "Awas")
REGIONS = (
    ("Laut Banda", -6.2, 129.4, "Pesisir Banda"),
    ("Selatan Jawa", -8.8, 110.2, "Pesisir Selatan Jawa"),
    ("Barat Sumatra", -1.2, 98.5, "Pesisir Barat Sumatra"),
    ("Laut Flores", -7.4, 122.3, "Pesisir Flores"),
    ("Utara Sulawesi", 1.8, 124.8, "Pesisir Utara Sulawesi"),
)


def timestamp(value: datetime | None = None) -> str:
    return (value or datetime.now(UTC)).astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def timestamp_us(value: str) -> int:
    """Normalize strict RFC3339 to integer microseconds, rounding a cutoff up."""
    match = RFC3339.fullmatch(value)
    if not match:
        raise ValueError("Expected RFC3339 with a timezone")
    parsed = datetime.fromisoformat(value.replace("t", "T").replace("z", "+00:00").replace("Z", "+00:00"))
    delta = parsed.astimezone(UTC) - EPOCH
    micros = (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds
    fraction = match.group(1) or ""
    # A watermark between two stored microseconds must not include the older one.
    return micros + int(any(digit != "0" for digit in fraction[6:]))


def _json(payload: dict) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _event(identifier: str, occurred_at: datetime, index: int, potential: bool) -> dict:
    region, latitude, longitude, _ = REGIONS[index % len(REGIONS)]
    return {
        "event_id": identifier,
        "magnitude": (6.2, 6.8, 7.4)[(index // 4) % 3] if potential else (3.2, 4.5, 5.6)[index % 3],
        "depth_km": float((12, 20, 35, 70, 120)[index % 5]),
        "epicenter_lat": latitude,
        "epicenter_lon": longitude,
        "region_name": region,
        "occurred_at": timestamp(occurred_at),
        "potential_tsunami": potential,
    }


def _warning(identifier: str, event: dict, index: int) -> dict:
    occurred_at = datetime.fromisoformat(event["occurred_at"].replace("Z", "+00:00"))
    return {
        "warning_id": identifier,
        "related_event_id": event["event_id"],
        "threat_level": THREAT_LEVELS[(index // 4) % 3],
        "affected_zones": [REGIONS[index % len(REGIONS)][3]],
        "estimated_arrival": timestamp(occurred_at + timedelta(minutes=30)),
    }


class Store:
    def __init__(self, path: Path):
        self.path = path

    @contextmanager
    def connection(self):
        connection = sqlite3.connect(self.path, timeout=5.0)
        try:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA busy_timeout = 5000")
            yield connection
        finally:
            connection.close()

    def initialize(self, seed_count: int) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connection() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS seismic_events (
                    event_id TEXT PRIMARY KEY,
                    occurred_at_us INTEGER NOT NULL,
                    payload TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS seismic_events_poll_idx
                    ON seismic_events(occurred_at_us, event_id);
                CREATE TABLE IF NOT EXISTS tsunami_warnings (
                    warning_id TEXT PRIMARY KEY,
                    related_event_id TEXT NOT NULL REFERENCES seismic_events(event_id),
                    updated_at_us INTEGER NOT NULL,
                    payload TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS tsunami_warnings_poll_idx
                    ON tsunami_warnings(updated_at_us, warning_id);
            """)
            seed_start = datetime.now(UTC) - timedelta(minutes=seed_count + 1)
            with connection:
                for index in range(seed_count):
                    event = _event(f"bmkg-eq-seed-{index + 1:06d}", seed_start + timedelta(minutes=index), index, index % 4 == 0)
                    inserted = connection.execute(
                        "INSERT OR IGNORE INTO seismic_events(event_id, occurred_at_us, payload) VALUES (?, ?, ?)",
                        (event["event_id"], timestamp_us(event["occurred_at"]), _json(event)),
                    ).rowcount
                    if inserted and event["potential_tsunami"]:
                        self._upsert_warning(connection, _warning(f"bmkg-ts-seed-{index + 1:06d}", event, index), timestamp_us(event["occurred_at"]))

    def list_events(self, since_us: int | None) -> list[dict]:
        return self._list("seismic_events", "occurred_at_us", "event_id", since_us)

    def list_warnings(self, since_us: int | None) -> list[dict]:
        return self._list("tsunami_warnings", "updated_at_us", "warning_id", since_us)

    def _list(self, table: str, clock: str, identifier: str, since_us: int | None) -> list[dict]:
        # Table/column identifiers are internal constants, never request input.
        query = f"SELECT payload FROM {table}"
        parameters = ()
        if since_us is not None:
            query += f" WHERE {clock} >= ?"
            parameters = (since_us,)
        query += f" ORDER BY {clock} ASC, {identifier} ASC"
        with self.connection() as connection:
            rows = connection.execute(query, parameters).fetchall()
        return [json.loads(row[0]) for row in rows]

    def generate(self, index: int) -> str:
        event = _event(f"bmkg-eq-{uuid4().hex}", datetime.now(UTC), index, index % 4 == 0)
        with self.connection() as connection, connection:
            connection.execute("INSERT INTO seismic_events VALUES (?, ?, ?)", (
                event["event_id"], timestamp_us(event["occurred_at"]), _json(event),
            ))
            if event["potential_tsunami"]:
                self._upsert_warning(connection, _warning(f"bmkg-ts-{uuid4().hex}", event, index), timestamp_us(timestamp()))
        return event["event_id"]

    def upsert_warning(self, warning: dict) -> None:
        """Internal ingestion/update API; every material change moves the watermark."""
        with self.connection() as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            self._upsert_warning(connection, warning, timestamp_us(timestamp()))

    @staticmethod
    def _upsert_warning(connection: sqlite3.Connection, warning: dict, updated_at_us: int) -> None:
        if warning["threat_level"] not in THREAT_LEVELS:
            raise ValueError("Invalid tsunami threat level")
        timestamp_us(warning["estimated_arrival"])
        event = connection.execute("SELECT payload FROM seismic_events WHERE event_id = ?", (warning["related_event_id"],)).fetchone()
        if event is None or json.loads(event[0])["potential_tsunami"] is not True:
            raise ValueError("A tsunami warning requires an event with tsunami potential")
        payload = _json(warning)
        previous = connection.execute("SELECT payload, updated_at_us FROM tsunami_warnings WHERE warning_id = ?", (warning["warning_id"],)).fetchone()
        if previous is not None:
            if previous[0] == payload:
                return
            updated_at_us = max(updated_at_us, previous[1] + 1)
        connection.execute("""
            INSERT INTO tsunami_warnings(warning_id, related_event_id, updated_at_us, payload)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(warning_id) DO UPDATE SET
                related_event_id = excluded.related_event_id,
                updated_at_us = excluded.updated_at_us,
                payload = excluded.payload
        """, (warning["warning_id"], warning["related_event_id"], updated_at_us, payload))

    def check_health(self) -> None:
        with self.connection() as connection:
            connection.execute("SELECT event_id FROM seismic_events LIMIT 1").fetchone()
            connection.execute("SELECT warning_id FROM tsunami_warnings LIMIT 1").fetchone()
