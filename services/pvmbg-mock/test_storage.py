"""Rules not exposed by the public, read-only PVMBG HTTP endpoints."""

import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from storage import ALERT_LEVELS, VOLCANO_IDS, Store, make_report, timestamp_us


class StorageTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="pvmbg-storage-test-")
        self.addCleanup(self.directory.cleanup)
        self.store = Store(Path(self.directory.name) / "pvmbg.sqlite3")
        self.store.initialize(20)

    def insert_raw(self, report_id: str, reported_us: int, payload: str) -> None:
        with self.store.connection() as connection, connection:
            connection.execute("INSERT INTO volcanic_reports VALUES (?, ?, ?)", (report_id, reported_us, payload))

    def test_seed_is_stable_across_restarts(self):
        reports = self.store.reports(None)
        self.assertEqual(len(reports), 20)
        self.assertEqual(reports[0]["report_id"], "pvmbg-seed-000001")
        self.store.initialize(20)
        self.assertEqual(self.store.reports(None), reports)

    def test_reports_are_sorted_by_time_then_id_and_since_is_inclusive(self):
        same_us = timestamp_us(self.store.reports(None)[-1]["reported_at"]) + 1_000_000
        for report_id in ("pvmbg-b", "pvmbg-a"):
            payload = make_report(report_id, 0, same_us, version=1)
            self.insert_raw(report_id, same_us, json.dumps(payload))
        reports = self.store.reports(None)
        keys = [(timestamp_us(report["reported_at"]), report["report_id"]) for report in reports]
        self.assertEqual(keys, sorted(keys))
        self.assertEqual([report["report_id"] for report in self.store.reports(same_us)], ["pvmbg-a", "pvmbg-b"])
        self.assertEqual(self.store.reports(same_us + 1), [])

    def test_append_never_goes_behind_the_newest_report(self):
        # A report stamped far ahead stands in for a clock that stepped backwards.
        future_us = timestamp_us("2999-01-01T00:00:00Z")
        self.insert_raw("pvmbg-future", future_us, json.dumps(make_report("pvmbg-future", 0, future_us, version=1)))
        self.store.append(version=1)
        appended = self.store.reports(future_us + 1)
        self.assertEqual(len(appended), 1)
        self.assertEqual(timestamp_us(appended[0]["reported_at"]), future_us + 1)

    def test_only_version_2_reports_store_confidence(self):
        newest_us = timestamp_us(self.store.reports(None)[-1]["reported_at"]) + 1
        self.store.append(version=1)
        self.store.append(version=2)
        first, second = self.store.reports(newest_us)
        self.assertNotIn("confidence_level", first)
        self.assertGreaterEqual(second["confidence_level"], 0.0)
        self.assertLessEqual(second["confidence_level"], 1.0)
        self.assertTrue(all("confidence_level" not in report for report in self.store.reports(None)[:20]))

    def test_generated_reports_are_consistent(self):
        for ordinal in range(len(ALERT_LEVELS) * len(VOLCANO_IDS)):
            report = make_report("pvmbg-x", ordinal, 0, version=1)
            with self.subTest(ordinal=ordinal):
                self.assertIn(report["volcano_id"], VOLCANO_IDS)
                self.assertIn(report["alert_level"], ALERT_LEVELS)
                if report["alert_level"] == "Normal":
                    self.assertEqual((report["eruption_count_24h"], report["ash_column_height_m"]), (0, 0.0))
                else:
                    self.assertGreater(report["eruption_count_24h"], 0)
                    self.assertGreater(report["ash_column_height_m"], 0.0)
        self.assertEqual(make_report("pvmbg-x", 0, 0, version=1)["reported_at"], "1970-01-01T00:00:00.000000Z")

    def test_undecodable_stored_report_is_a_database_error(self):
        for payload in ("not json", "[1, 2]"):
            with self.subTest(payload=payload):
                self.insert_raw(f"pvmbg-bad-{len(payload)}", 1, payload)
                with self.assertRaises(sqlite3.DatabaseError):
                    self.store.reports(None)
                with self.store.connection() as connection, connection:
                    connection.execute("DELETE FROM volcanic_reports WHERE report_id LIKE 'pvmbg-bad-%'")

    def test_submicrosecond_lower_bound_rounds_up(self):
        exact = timestamp_us("2026-01-01T00:00:00.123456Z")
        self.assertEqual(timestamp_us("2026-01-01T00:00:00.1234561Z"), exact + 1)
        self.assertEqual(timestamp_us("2026-01-01T00:00:00.123456000Z"), exact)

    def test_timezone_offsets_normalize_to_same_integer(self):
        utc = timestamp_us("2026-01-01T00:00:00.123456Z")
        self.assertEqual(timestamp_us("2026-01-01T07:00:00.123456+07:00"), utc)
        self.assertEqual(timestamp_us("2025-12-31T19:00:00.123456-05:00"), utc)
        self.assertEqual(timestamp_us("2026-01-01t00:00:00.123456z"), utc)

    def test_since_requires_a_valid_timestamp_with_timezone(self):
        for value in ("2026-01-01T00:00:00", "2026-01-01", "2026-02-30T00:00:00Z", "2026-01-01T00:00:00+24:00", "yesterday"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                timestamp_us(value)


if __name__ == "__main__":
    unittest.main()
