"""Verify the HTTP response projection without starting a server or a container."""

from copy import deepcopy
from threading import Event, Lock
from types import SimpleNamespace
import unittest

from app import Handler
from storage import make_report


class SchemaModesTests(unittest.TestCase):
    def test_modes_project_reports_without_mutating_storage(self):
        normal = make_report("normal", 0, 1_000_000, version=2)
        awas = make_report("awas", 3, 2_000_000, version=2)

        for version in range(1, 6):
            with self.subTest(version=version):
                captured = []
                handler = object.__new__(Handler)
                handler.server = SimpleNamespace(application=SimpleNamespace(lock=Lock(), stop=Event(), outage=False,
                    version=version, config=SimpleNamespace(delay_min_ms=0, delay_max_ms=0),
                    store=SimpleNamespace(reports=lambda _since: deepcopy([normal, awas]))))
                handler.send_json = lambda *args: captured.append(args)
                Handler.list_reports(handler, "")
                status, response, headers = captured[0]
                self.assertEqual(status, 200)
                self.assertEqual(headers["X-PVMBG-Schema-Version"], str(version))
                first, second = response["items"]
                self.assertEqual(first["report_id"], "normal")
                self.assertEqual(first["reported_at"], normal["reported_at"])
                if version == 1:
                    self.assertNotIn("confidence_level", first)
                if version == 2:
                    self.assertEqual(first, normal)
                if version == 3:
                    self.assertNotIn("alert_level", first)
                    self.assertEqual(first["volcano_alert"], "Normal")
                if version == 4:
                    self.assertEqual(first["eruption_count_24h"], "0")
                if version == 5:
                    self.assertNotIn("ash_column_height_m", first)
                    self.assertEqual(second["ash_column_height_m"], awas["ash_column_height_m"])
                self.assertIn("ash_column_height_m", normal)
                self.assertIn("alert_level", normal)
