"""Automated Tests for Problem 1: Arsitektur & Interoperabilitas Data.

Specification criteria:
1. Jalankan sistem dengan PVMBG pada skema awal (v1), lalu tunjukkan Aggregator
   menghasilkan HazardEvent yang sesuai dengan aturan pemetaan untuk kedua sumber.
2. Picu perubahan skema PVMBG melalui POST /admin/schema-version saat sistem
   sedang berjalan, tanpa restart atau redeploy service mana pun.
3. Tunjukkan Aggregator tetap menghasilkan HazardEvent yang valid setelah perubahan
   tersebut, tanpa crash dan tanpa intervensi manual pada kode.
4. Deklarasikan kebijakan penanganan field tak dikenal: diteruskan ke attributes.
   Tunjukkan perilaku sistem konsisten dengan kebijakan yang dideklarasikan.
5. Verifikasi koeksistensi data sebelum (v1) dan sesudah (v2) perubahan skema.
"""

import time
import unittest

from problem_set_support import (
    FULL_ONLY,
    USERS,
    api,
    mock_request,
    require_stack,
    set_pvmbg_schema_version,
    wait_for,
)

HAZARDS_ENDPOINT = "/api/v1/hazards"
FIELD_USER = USERS["FIELD_TEAM"]
BNPB_USER = USERS["BNPB_INTERNAL"]
MEDIA_USER = USERS["MEDIA"]


def setUpModule():
    require_stack()


class TestProblem1DataInteroperability(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Ensure PVMBG starts in schema version 1
        set_pvmbg_schema_version(1)
        # Wait for data ingestion from both BMKG and PVMBG
        wait_for(
            lambda: cls._get_hazards(source="BMKG"),
            timeout=90,
            message="initial BMKG seismic hazards",
        )
        wait_for(
            lambda: cls._get_hazards(source="PVMBG"),
            timeout=90,
            message="initial PVMBG volcanic hazards",
        )

    @classmethod
    def tearDownClass(cls):
        # Reset schema version to 1 upon suite completion
        try:
            set_pvmbg_schema_version(1)
        except Exception:
            pass

    @classmethod
    def _get_hazards(cls, source=None, view="full", user=FIELD_USER):
        query = {"view": view, "page_size": 50}
        if source:
            query["source"] = source
        status, _, body = api("GET", HAZARDS_ENDPOINT, user=user, query=query)
        if status == 200 and body and "items" in body:
            return body["items"]
        return []

    def test_01_initial_schema_canonical_mapping(self):
        """Criterion 1: Verify canonical HazardEvent mapping from BMKG and PVMBG v1."""
        items = self._get_hazards()
        self.assertTrue(len(items) > 0, "Canonical store must contain hazards")

        bmkg_events = [h for h in items if h.get("source") == "BMKG"]
        pvmbg_events = [h for h in items if h.get("source") == "PVMBG"]

        self.assertTrue(len(bmkg_events) > 0, "Must have BMKG seismic hazard events")
        self.assertTrue(len(pvmbg_events) > 0, "Must have PVMBG volcanic hazard events")

        # 1. Verify BMKG Mapping rules
        for item in bmkg_events:
            self.assertEqual(item["hazard_type"], "SEISMIC")
            self.assertTrue(item.get("hazard_id"), "hazard_id must be populated")
            self.assertTrue(item.get("source_ref_id"), "source_ref_id must match event_id")
            self.assertTrue(item.get("area_name"), "area_name must match region_name")
            self.assertIn(item.get("severity"), ["NORMAL", "WASPADA", "SIAGA", "AWAS"])
            self.assertIsInstance(item.get("latitude"), (int, float))
            self.assertIsInstance(item.get("longitude"), (int, float))
            self.assertTrue(item.get("occurred_at"), "occurred_at timestamp required")
            self.assertTrue(item.get("ingested_at"), "ingested_at timestamp required")

            # Check attributes contain seismic specific metrics
            attrs = item.get("attributes", {})
            self.assertIsInstance(attrs, dict)
            self.assertIn("magnitude", attrs, "attributes must contain magnitude")
            self.assertIn("depth_km", attrs, "attributes must contain depth_km")
            self.assertIn("potential_tsunami", attrs, "attributes must contain potential_tsunami")

        # 2. Verify PVMBG Mapping rules (v1)
        for item in pvmbg_events:
            self.assertEqual(item["hazard_type"], "VOLCANIC")
            self.assertTrue(item.get("hazard_id"), "hazard_id must be populated")
            self.assertTrue(item.get("source_ref_id"), "source_ref_id must match report_id")
            self.assertTrue(item.get("area_name"), "area_name must be mapped from volcano_id")
            self.assertIn(item.get("severity"), ["NORMAL", "WASPADA", "SIAGA", "AWAS"])
            self.assertIsInstance(item.get("latitude"), (int, float))
            self.assertIsInstance(item.get("longitude"), (int, float))

            # Attributes contain volcanic metrics
            attrs = item.get("attributes", {})
            self.assertIsInstance(attrs, dict)
            self.assertIn("eruption_count_24h", attrs)
            self.assertIn("ash_column_height_m", attrs)

    def test_02_live_schema_change_trigger(self):
        """Criterion 2: Trigger schema change to version 2 (with confidence_level) on the fly."""
        resp = set_pvmbg_schema_version(2)
        self.assertEqual(resp.get("version"), 2, "PVMBG schema version must be updated to 2")

        # Verify PVMBG upstream directly returns confidence_level on new reports
        def check_upstream_v2():
            status, _, body = mock_request("pvmbg", "/volcanic-reports")
            if status == 200 and body and "items" in body:
                for r in body["items"]:
                    if "confidence_level" in r:
                        return r
            return None

        v2_raw = wait_for(check_upstream_v2, timeout=25, interval=2, message="upstream v2 volcanic report")
        self.assertIn("confidence_level", v2_raw, "Upstream PVMBG report must include confidence_level")
        self.assertIsInstance(v2_raw["confidence_level"], (int, float))
        self.assertGreaterEqual(v2_raw["confidence_level"], 0.0)
        self.assertLessEqual(v2_raw["confidence_level"], 1.0)

    def test_03_aggregator_resilience_and_no_crash(self):
        """Criterion 3: Aggregator continues processing without crashes or manual code changes."""
        # Query source statuses to ensure ingestion is still healthy
        status, _, body = api("GET", "/api/v1/hazards/sources/statuses", user=BNPB_USER)
        self.assertEqual(status, 200)
        self.assertIsInstance(body.get("freshness"), list)
        pvmbg_status = next((s for s in body["freshness"] if s.get("source") == "PVMBG"), None)
        self.assertIsNotNone(pvmbg_status)
        self.assertIn(pvmbg_status.get("status"), ["FRESH", "STALE"])

    def test_04_unknown_fields_forwarded_to_attributes_policy(self):
        """Criterion 4: Declared policy for unknown/additive fields (forwarded to attributes)."""
        # Poll until the aggregator ingests a record with confidence_level in attributes
        def find_v2_canonical():
            pvmbg_items = self._get_hazards(source="PVMBG")
            for h in pvmbg_items:
                attrs = h.get("attributes", {})
                if "confidence_level" in attrs:
                    return h
            return None

        v2_hazard = wait_for(
            find_v2_canonical,
            timeout=30,
            interval=2,
            message="canonical hazard containing confidence_level in attributes",
        )

        attrs = v2_hazard["attributes"]
        self.assertIn("confidence_level", attrs, "Dynamic field confidence_level must be preserved in attributes")
        val = attrs["confidence_level"]
        self.assertIsInstance(val, (int, float))
        self.assertGreaterEqual(val, 0.0)
        self.assertLessEqual(val, 1.0)

        # Confirm other core fields were not distorted
        self.assertEqual(v2_hazard["hazard_type"], "VOLCANIC")
        self.assertTrue(v2_hazard["hazard_id"].startswith("hzd_"))
        self.assertTrue(v2_hazard["area_name"])

    def test_05_v1_and_v2_coexistence_in_canonical_store(self):
        """Criterion 5: Both v1 records (without confidence_level) and v2 records coexist seamlessly."""
        pvmbg_items = self._get_hazards(source="PVMBG")
        v1_records = [h for h in pvmbg_items if "confidence_level" not in h.get("attributes", {})]
        v2_records = [h for h in pvmbg_items if "confidence_level" in h.get("attributes", {})]

        self.assertTrue(len(v1_records) > 0, "Must have historical v1 records without confidence_level")
        self.assertTrue(len(v2_records) > 0, "Must have newly ingested v2 records with confidence_level")

        # Verify summary projection still works for both
        status, _, body = api("GET", HAZARDS_ENDPOINT, user=MEDIA_USER, query={"source": "PVMBG"})
        self.assertEqual(status, 200)
        for item in body.get("items", []):
            self.assertFalse(FULL_ONLY & item.keys(), "Summary view must not leak raw attributes")


if __name__ == "__main__":
    unittest.main(verbosity=2)
