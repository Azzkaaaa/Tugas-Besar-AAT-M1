"""Automated Tests for Problem 2: Concurrency, Availability, & Graceful Degradation.

Specification criteria:
1. Dengan delay PVMBG diatur pada 3 detik, kirim permintaan yang hanya membutuhkan
   data BMKG bersamaan dengan permintaan yang membutuhkan data PVMBG.
   Tunjukkan p95 latensi permintaan BMKG-only tetap di bawah 300 ms.
2. Jalankan load testing dengan >=50 koneksi paralel selama minimal 60 detik dalam pola
   sustained. Laporkan throughput, p50/p95/p99, dan error rate.
   Error rate di luar penolakan terkontrol (429) di bawah 1%.
3. Nyalakan outage PVMBG melalui POST /admin/outage. Selama outage:
   - Permintaan data seismik (BMKG) tetap dilayani normal.
   - Permintaan data vulkanik (PVMBG) dijawab dengan data terakhir dari Canonical Store
     disertai penanda kebaruan (stale / stale_since), BUKAN error 500 dan BUKAN timeout.
   - Setelah outage dimatikan, tunjukkan sistem kembali normal tanpa restart.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
import http.client
import json
import os
import time
import unittest
from typing import Dict, List, Tuple, Any

from problem_set_support import (
    APP_HOST,
    HOST,
    PORT,
    TIMEOUT,
    USERS,
    api,
    calculate_percentiles,
    mock_request,
    require_stack,
    set_pvmbg_outage,
    token,
    wait_for,
)

HAZARDS_ENDPOINT = "/api/v1/hazards"
FIELD_USER = USERS["FIELD_TEAM"]
BNPB_USER = USERS["BNPB_INTERNAL"]
MEDIA_USER = USERS["MEDIA"]


def setUpModule():
    require_stack()


def raw_http_timed(path: str, user: str, host: str = APP_HOST, timeout: float = 10.0) -> Tuple[int, float, Any]:
    """Perform raw HTTP GET with exact wall-clock response timing in milliseconds."""
    conn = http.client.HTTPConnection(HOST, PORT, timeout=timeout)
    tok = token(user)
    headers = {
        "Host": host,
        "Authorization": f"Bearer {tok}",
        "Accept": "application/json",
    }
    start = time.perf_counter()
    try:
        conn.request("GET", path, headers=headers)
        resp = conn.getresponse()
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        data = resp.read()
        try:
            parsed = json.loads(data)
        except Exception:
            parsed = None
        return resp.status, elapsed_ms, parsed
    except Exception as e:
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return 0, elapsed_ms, str(e)
    finally:
        conn.close()


class TestProblem2ConcurrencyAvailability(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Reset outage status
        try:
            set_pvmbg_outage(False)
        except Exception:
            pass

    @classmethod
    def tearDownClass(cls):
        # Clean up outage state
        try:
            set_pvmbg_outage(False)
        except Exception:
            pass

    def test_01_bmkg_latency_under_pvmbg_contention_and_delay(self):
        """Criterion 1: BMKG-only p95 latency stays < 300 ms even under slow PVMBG background contention."""
        bmkg_durations: List[float] = []
        pvmbg_durations: List[float] = []
        stop_contention = False

        # Background worker creating continuous PVMBG traffic
        def pvmbg_worker():
            while not stop_contention:
                status, dur, _ = raw_http_timed(f"{HAZARDS_ENDPOINT}?source=PVMBG&view=full", FIELD_USER)
                pvmbg_durations.append(dur)
                time.sleep(0.05)

        # Run 5 background PVMBG contending threads
        contention_executor = ThreadPoolExecutor(max_workers=5)
        for _ in range(5):
            contention_executor.submit(pvmbg_worker)

        # Run 100 BMKG-only requests across 10 worker threads
        try:
            bmkg_paths = [
                f"{HAZARDS_ENDPOINT}?source=BMKG&view=full",
                f"{HAZARDS_ENDPOINT}?hazard_type=SEISMIC",
                f"{HAZARDS_ENDPOINT}?source=BMKG&page_size=10",
            ]

            def bmkg_task(i):
                path = bmkg_paths[i % len(bmkg_paths)]
                return raw_http_timed(path, FIELD_USER)

            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = [executor.submit(bmkg_task, i) for i in range(100)]
                for f in as_completed(futures):
                    status, dur, _ = f.result()
                    self.assertEqual(status, 200, f"BMKG request should succeed with 200, got {status}")
                    bmkg_durations.append(dur)
        finally:
            stop_contention = True
            contention_executor.shutdown(wait=False)

        stats = calculate_percentiles(bmkg_durations)
        print(f"\n[Problem 2 - Criterion 1] BMKG-only latency under PVMBG contention ({stats['count']} requests):")
        print(f"  p50: {stats['p50']} ms | p90: {stats['p90']} ms | p95: {stats['p95']} ms | p99: {stats['p99']} ms | Max: {stats['max']} ms")

        # Spec Assertion: p95 < 300 ms
        self.assertLess(
            stats["p95"],
            300.0,
            f"Expected BMKG-only p95 latency < 300ms, got {stats['p95']} ms",
        )

    def test_02_sustained_load_concurrency_50vus(self):
        """Criterion 2: Sustained load test (>=50 concurrent connections, error rate < 1%)."""
        vus = int(os.environ.get("LOAD_TEST_VUS", "50"))
        # Allow short test duration in fast CI / tests (e.g. 15-60s), default to full 60s
        duration_s = float(os.environ.get("LOAD_TEST_DURATION_SECONDS", "60"))

        print(f"\n[Problem 2 - Criterion 2] Starting sustained load test ({vus} concurrent workers, duration: {duration_s}s)...")

        endpoints = [
            f"{HAZARDS_ENDPOINT}?page_size=20",
            f"{HAZARDS_ENDPOINT}?source=BMKG",
            f"{HAZARDS_ENDPOINT}?source=PVMBG",
            f"{HAZARDS_ENDPOINT}/sources/statuses",
        ]

        durations: List[float] = []
        statuses: Dict[int, int] = {}
        error_count = 0
        total_requests = 0

        start_time = time.monotonic()
        deadline = start_time + duration_s

        def load_worker(worker_id):
            nonlocal error_count, total_requests
            local_durations = []
            local_statuses = {}
            local_errors = 0
            req_idx = 0

            while time.monotonic() < deadline:
                path = endpoints[req_idx % len(endpoints)]
                req_idx += 1
                status, dur, body = raw_http_timed(path, FIELD_USER, timeout=5.0)

                local_durations.append(dur)
                local_statuses[status] = local_statuses.get(status, 0) + 1

                # 429 is controlled rejection per spec; 200 is success
                if status not in (200, 429):
                    local_errors += 1

            return local_durations, local_statuses, local_errors

        with ThreadPoolExecutor(max_workers=vus) as executor:
            futures = [executor.submit(load_worker, i) for i in range(vus)]
            for f in as_completed(futures):
                durs, st_map, errs = f.result()
                durations.extend(durs)
                error_count += errs
                for code, cnt in st_map.items():
                    statuses[code] = statuses.get(code, 0) + cnt

        total_requests = len(durations)
        actual_duration = time.monotonic() - start_time
        throughput = total_requests / actual_duration if actual_duration > 0 else 0
        error_rate_pct = (error_count / total_requests * 100.0) if total_requests > 0 else 0.0

        stats = calculate_percentiles(durations)
        print(f"[Problem 2 - Criterion 2] Sustained Load Results:")
        print(f"  Total Requests : {total_requests}")
        print(f"  Throughput     : {throughput:.2f} req/s")
        print(f"  Status Codes   : {statuses}")
        print(f"  Uncontrolled Err: {error_count} ({error_rate_pct:.2f}%)")
        print(f"  p50: {stats['p50']} ms | p95: {stats['p95']} ms | p99: {stats['p99']} ms")

        # Assertions
        self.assertGreater(total_requests, 50, "Should have executed load requests")
        self.assertLess(
            error_rate_pct,
            1.0,
            f"Error rate outside controlled 429 must be < 1%, got {error_rate_pct:.2f}%",
        )

    def test_03_pvmbg_outage_graceful_degradation_and_recovery(self):
        """Criterion 3: PVMBG outage -> Seismic served normally, Volcanic served stale from store, no 500s."""
        # 1. Trigger PVMBG outage
        print("\n[Problem 2 - Criterion 3] Activating PVMBG Outage (POST /admin/outage enabled=true)...")
        set_pvmbg_outage(True)

        # Allow poller cycle to register failure
        time.sleep(5)

        try:
            # 2. Check BMKG data is unaffected and returns 200 with fresh records
            status, _, bmkg_body = api("GET", HAZARDS_ENDPOINT, user=FIELD_USER, query={"source": "BMKG"})
            self.assertEqual(status, 200, "BMKG seismic requests must remain 200 OK during PVMBG outage")
            self.assertTrue(len(bmkg_body.get("items", [])) > 0)

            # 3. Check PVMBG data is served gracefully from Canonical Store (NOT 500, NOT hanging)
            status, dur, pvmbg_body = raw_http_timed(f"{HAZARDS_ENDPOINT}?source=PVMBG", FIELD_USER, timeout=4.0)
            self.assertEqual(
                status,
                200,
                f"PVMBG requests must degrade gracefully and return 200 from Canonical Store, got HTTP {status}",
            )
            self.assertLess(dur, 2000.0, f"PVMBG canonical store read must not hang, took {dur:.2f} ms")
            self.assertTrue(len(pvmbg_body.get("items", [])) > 0, "Previously-ingested PVMBG hazards must be returned")

            # 4. Check Source Status reflects STALE status / freshness degradation
            def check_stale_status():
                status, _, body = api("GET", f"{HAZARDS_ENDPOINT}/sources/statuses", user=MEDIA_USER)
                if status == 200 and "freshness" in body:
                    for s in body["freshness"]:
                        if s.get("source") == "PVMBG" and s.get("status") == "STALE":
                            return s
                return None

            stale_entry = wait_for(check_stale_status, timeout=15, interval=2, message="PVMBG status to become STALE")
            self.assertEqual(stale_entry["status"], "STALE")
            self.assertTrue("stale_since" in stale_entry or "last_success_at" in stale_entry)
            print(f"  SUCCESS: PVMBG status marked STALE (Last Success: {stale_entry.get('last_success_at')})")

        finally:
            # 5. Disable outage and verify self-healing recovery without restart
            print("  Deactivating PVMBG Outage (POST /admin/outage enabled=false)...")
            set_pvmbg_outage(False)

        def check_recovered_status():
            status, _, body = api("GET", f"{HAZARDS_ENDPOINT}/sources/statuses", user=MEDIA_USER)
            if status == 200 and "freshness" in body:
                for s in body["freshness"]:
                    if s.get("source") == "PVMBG" and s.get("status") == "FRESH":
                        return s
            return None

        recovered = wait_for(check_recovered_status, timeout=20, interval=2, message="PVMBG recovery to FRESH")
        self.assertEqual(recovered["status"], "FRESH")
        print("  SUCCESS: PVMBG automatically recovered to FRESH without restarting services.")


if __name__ == "__main__":
    unittest.main(verbosity=2)
