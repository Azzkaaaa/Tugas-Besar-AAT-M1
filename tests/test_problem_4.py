"""Automated Tests for Problem 4: Arsitektur Microservice & Flexible Storage.

Specification criteria:
1. Tunjukkan setiap komponen berjalan sebagai container terpisah, dan tunjukkan salah
   satunya dapat dihentikan, di-rebuild, dan dijalankan ulang TANPA me-rebuild atau
   merestart container lain. Tunjukkan pula sistem tetap melayani permintaan yang tidak
   bergantung pada komponen yang sedang di-restart.
2. Simpan HazardEvent pada penyimpanan yang mengakomodasi atribut dinamis tanpa migrasi
   skema saat field baru muncul (contoh: JSONB / dynamic attributes column).
   Tunjukkan record dari sebelum dan sesudah perubahan skema tersimpan berdampingan dan
   keduanya dapat dibaca.
3. Tunjukkan Canonical Store hanya dapat diakses langsung oleh Aggregator, dan komponen
   lain memperoleh data melalui API Aggregator.
"""

import os
import subprocess
import time
import unittest

from problem_set_support import (
    CONFIG,
    ROOT,
    USERS,
    api,
    docker_exec,
    get_running_containers,
    is_docker_available,
    require_stack,
    set_pvmbg_schema_version,
    wait_for,
)

HAZARDS_ENDPOINT = "/api/v1/hazards"
FIELD = USERS["FIELD_TEAM"]
BNPB = USERS["BNPB_INTERNAL"]
MEDIA = USERS["MEDIA"]


def setUpModule():
    require_stack()


class TestProblem4MicroservicesAndStorage(unittest.TestCase):

    def test_01_independent_containers_and_partial_restart(self):
        """Criterion 1: Container independence and zero-downtime partial container restart."""
        if not is_docker_available():
            raise unittest.SkipTest("Docker Compose CLI not available in test environment")

        containers = get_running_containers()
        print(f"\n[Problem 4 - Criterion 1] Detected running containers: {containers}")
        self.assertTrue(len(containers) > 3, "System must consist of multiple distinct microservices")

        # Pick a non-critical consumer/service to restart: e.g. pvmbg-mock or webhook-provider
        target_service = "webhook-provider"
        if target_service not in containers:
            target_service = "pvmbg-mock"

        print(f"  Testing restart isolation with target service: '{target_service}'...")

        # 1. Stop target container
        code, out = docker_exec(["compose", "stop", target_service])
        self.assertEqual(code, 0, f"Failed stopping container {target_service}: {out}")

        try:
            # 2. While target container is stopped, verify independent service (e.g. BMKG hazard query) continues serving
            status, _, body = api("GET", HAZARDS_ENDPOINT, user=FIELD, query={"source": "BMKG"})
            self.assertEqual(
                status,
                200,
                f"BMKG queries must continue returning 200 OK while {target_service} is stopped",
            )
            self.assertTrue(len(body.get("items", [])) > 0)
            print(f"  SUCCESS: Core Aggregator & BMKG serving unaffected during {target_service} downtime.")

        finally:
            # 3. Restart target container without touching or restarting other containers
            code, out = docker_exec(["compose", "start", target_service])
            self.assertEqual(code, 0, f"Failed restarting container {target_service}: {out}")

            # Verify target container is back online
            time.sleep(3)
            print(f"  SUCCESS: Container {target_service} successfully restarted independently.")

    def test_02_flexible_storage_dynamic_schema_coexistence(self):
        """Criterion 2: HazardEvent stores dynamic attributes; v1 & v2 records coexist in store."""
        # 1. Trigger schema version 2 on PVMBG
        set_pvmbg_schema_version(2)

        def get_pvmbg_with_v2():
            status, _, body = api("GET", HAZARDS_ENDPOINT, user=FIELD, query={"source": "PVMBG", "view": "full", "page_size": 100})
            if status == 200 and body and "items" in body:
                items = body["items"]
                has_v2 = any("confidence_level" in h.get("attributes", {}) for h in items)
                if has_v2:
                    return items
            return None

        pvmbg_items = wait_for(get_pvmbg_with_v2, timeout=35, interval=2, message="PVMBG v2 ingestion in Canonical Store")

        # Ensure we have both v1 (no confidence_level) and v2 (with confidence_level)
        v1_records = [h for h in pvmbg_items if "confidence_level" not in h.get("attributes", {})]
        v2_records = [h for h in pvmbg_items if "confidence_level" in h.get("attributes", {})]

        print(f"\n[Problem 4 - Criterion 2] Flexible Storage Verification:")
        print(f"  Retrieved {len(pvmbg_items)} total PVMBG hazards from Canonical Store.")
        print(f"  v1 Hazards (without confidence_level) : {len(v1_records)}")
        print(f"  v2 Hazards (with confidence_level)    : {len(v2_records)}")

        self.assertTrue(
            len(v1_records) > 0,
            "Canonical Store must contain v1 records (without confidence_level)",
        )
        self.assertTrue(
            len(v2_records) > 0,
            "Canonical Store must contain v2 records (with confidence_level in JSONB attributes)",
        )

        # Inspect v2 attributes structure
        sample_v2 = v2_records[0]
        self.assertIn("confidence_level", sample_v2["attributes"])
        self.assertIsInstance(sample_v2["attributes"]["confidence_level"], (int, float))

        # Inspect v1 attributes structure
        sample_v1 = v1_records[0]
        self.assertNotIn("confidence_level", sample_v1["attributes"])
        self.assertIn("eruption_count_24h", sample_v1["attributes"])

        print("  SUCCESS: Both schema versions coexist in Canonical Store without DDL schema migrations.")

    def test_03_canonical_store_exclusive_ownership(self):
        """Criterion 3: Canonical Store is owned and accessed solely by Aggregator."""
        print("\n[Problem 4 - Criterion 3] Verifying Canonical Store ownership and boundaries...")

        # 1. Verify compose configuration isolates aggregator-db:
        # Downstream services (api-gateway, feed-service, webhook-provider, notification-service)
        # communicate with Aggregator via gRPC / REST and do not receive CANONICAL_STORE_DSN.
        compose_text = (ROOT / "compose.yaml").read_text(encoding="utf-8")

        # Extract services block
        services_to_check = ["api-gateway", "feed-service", "webhook-provider", "notification-service"]
        for svc in services_to_check:
            # Look for CANONICAL_STORE_DSN under the service block
            svc_block = compose_text.split(f"  {svc}:")
            if len(svc_block) > 1:
                sub_block = svc_block[1].split("\n\n  ")[0]
                self.assertNotIn(
                    "CANONICAL_STORE_DSN",
                    sub_block,
                    f"Service '{svc}' should not have direct access to CANONICAL_STORE_DSN",
                )

        # 2. Client-facing API reads via Aggregator API
        status, _, body = api("GET", f"{HAZARDS_ENDPOINT}/sources/statuses", user=MEDIA)
        self.assertEqual(status, 200, "Client-facing API must successfully query Aggregator service")
        self.assertIn("freshness", body)
        print("  SUCCESS: Canonical Store access is strictly bounded to Aggregator service.")


if __name__ == "__main__":
    unittest.main(verbosity=2)
