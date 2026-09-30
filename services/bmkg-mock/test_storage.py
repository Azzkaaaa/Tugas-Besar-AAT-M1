"""Rules not exposed by the public, read-only BMKG HTTP endpoints."""

from copy import deepcopy
from pathlib import Path
import tempfile
import unittest

from storage import Store, timestamp_us


class StorageTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="bmkg-storage-test-")
        self.addCleanup(self.directory.cleanup)
        self.store = Store(Path(self.directory.name) / "bmkg.sqlite3")
        self.store.initialize(20)

    def warning_timestamp(self, warning_id: str) -> int:
        with self.store.connection() as connection:
            return connection.execute(
                "SELECT updated_at_us FROM tsunami_warnings WHERE warning_id = ?", (warning_id,)
            ).fetchone()[0]

    def test_restart_preserves_seed_and_warning_watermarks(self):
        events = self.store.list_events(None)
        warnings = self.store.list_warnings(None)
        timestamps = {warning["warning_id"]: self.warning_timestamp(warning["warning_id"]) for warning in warnings}
        self.store.initialize(20)
        self.assertEqual(self.store.list_events(None), events)
        self.assertEqual(self.store.list_warnings(None), warnings)
        self.assertEqual(
            {warning["warning_id"]: self.warning_timestamp(warning["warning_id"]) for warning in warnings}, timestamps
        )

    def test_identical_warning_update_preserves_watermark(self):
        warning = self.store.list_warnings(None)[0]
        before = self.warning_timestamp(warning["warning_id"])
        self.store.upsert_warning(deepcopy(warning))
        self.assertEqual(self.warning_timestamp(warning["warning_id"]), before)

    def test_material_update_advances_watermark_and_is_pollable(self):
        warning = self.store.list_warnings(None)[0]
        before = self.warning_timestamp(warning["warning_id"])
        warning["affected_zones"].append("Pesisir tambahan")
        self.store.upsert_warning(warning)
        updated = self.warning_timestamp(warning["warning_id"])
        self.assertGreater(updated, before)
        self.assertEqual(self.store.list_warnings(updated), [warning])
        self.assertEqual(self.store.list_warnings(updated + 1), [])
        self.assertNotIn("updated_at", self.store.list_warnings(updated)[0])

    def test_warning_rejects_non_tsunami_event_without_partial_write(self):
        warning = self.store.list_warnings(None)[0]
        warning["warning_id"] = "invalid-warning"
        warning["related_event_id"] = next(
            event["event_id"] for event in self.store.list_events(None) if not event["potential_tsunami"]
        )
        before = self.store.list_warnings(None)
        with self.assertRaises(ValueError):
            self.store.upsert_warning(warning)
        self.assertEqual(self.store.list_warnings(None), before)

    def test_warning_rejects_unknown_event_or_threat_level(self):
        original = self.store.list_warnings(None)[0]
        for changes in ({"related_event_id": "missing-event"}, {"threat_level": "Unknown"}):
            with self.subTest(changes=changes):
                warning = {**original, **changes}
                with self.assertRaises(ValueError):
                    self.store.upsert_warning(warning)
        self.assertEqual(self.store.list_warnings(None)[0], original)

    def test_submicrosecond_lower_bound_excludes_older_microsecond(self):
        first = self.store.list_events(None)[0]
        exact = timestamp_us(first["occurred_at"])
        finer = timestamp_us(first["occurred_at"].replace("Z", "1Z"))
        self.assertEqual(finer, exact + 1)
        self.assertEqual(self.store.list_events(exact)[0], first)
        self.assertNotIn(first, self.store.list_events(finer))
        zero_tail = timestamp_us(first["occurred_at"].replace("Z", "000Z"))
        self.assertEqual(zero_tail, exact)

    def test_timezone_offsets_normalize_to_same_integer(self):
        utc = timestamp_us("2026-01-01T00:00:00.123456Z")
        self.assertEqual(timestamp_us("2026-01-01T07:00:00.123456+07:00"), utc)
        self.assertEqual(timestamp_us("2025-12-31T19:00:00.123456-05:00"), utc)
        self.assertEqual(timestamp_us("2026-01-01t00:00:00.123456z"), utc)


if __name__ == "__main__":
    unittest.main()
