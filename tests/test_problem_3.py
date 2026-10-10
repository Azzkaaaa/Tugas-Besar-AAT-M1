"""Automated Tests for Problem 3: Service-to-Service Authentication & Trust.

Specification criteria:
1. Tunjukkan kredensial yang valid untuk BMKG ditolak dengan 401/403 saat dipakai
   mengakses PVMBG, dan sebaliknya.
2. Tunjukkan client ber-scope Media hanya menerima field berklasifikasi Ringkasan.
   Permintaan eksplisit terhadap field mentah ditolak server dengan 401/403, bukan
   sekadar disembunyikan di sisi tampilan.
3. Tunggu access token Tim Lapangan kedaluwarsa secara alami sesuai TTL (default 60 detik)
   di tengah sesi aktif. Tunjukkan alur refresh menghasilkan token baru tanpa login
   ulang manual, dan tunjukkan token lama ditolak setelah refresh.
4. Tunjukkan tidak ada kredensial yang ter-hardcode di kode maupun ter-commit, dan
   .env.example tersedia sesuai Ketentuan Implementasi.
"""

import base64
import http.client
import json
import os
from pathlib import Path
import re
import time
import unittest
from urllib.parse import urlencode
from typing import Dict, Any

from problem_set_support import (
    AUTH_HOST,
    CONFIG,
    FULL_ONLY,
    ROOT,
    SECRETS_DIR,
    USERS,
    api,
    mock_request,
    require_stack,
    token,
)

HAZARDS_ENDPOINT = "/api/v1/hazards"
MEDIA = USERS["MEDIA"]
FIELD = USERS["FIELD_TEAM"]
BNPB = USERS["BNPB_INTERNAL"]


def setUpModule():
    require_stack()


def request_raw_token(user: str) -> Dict[str, Any]:
    """Request a fresh client-credentials token payload directly from Zitadel OIDC endpoint."""
    secret_path = SECRETS_DIR / f"{user}.secret"
    project_id_path = SECRETS_DIR / "project-id"
    if not secret_path.is_file() or not project_id_path.is_file():
        raise unittest.SkipTest("Zitadel secrets not found in .cache/auth")

    secret = secret_path.read_text(encoding="utf-8").strip()
    proj_id = project_id_path.read_text(encoding="utf-8").strip()

    basic = base64.b64encode(f"{user}:{secret}".encode()).decode()
    scope = f"openid urn:zitadel:iam:org:project:id:{proj_id}:aud"

    conn = http.client.HTTPConnection("127.0.0.1", 8080, timeout=15)
    try:
        body = urlencode({"grant_type": "client_credentials", "scope": scope})
        conn.request(
            "POST",
            "/oauth/v2/token",
            body=body,
            headers={
                "Host": AUTH_HOST,
                "Authorization": f"Basic {basic}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )
        resp = conn.getresponse()
        payload = json.loads(resp.read())
        return payload
    finally:
        conn.close()


class TestProblem3AuthAndTrust(unittest.TestCase):

    def test_01_cross_service_credentials_rejected(self):
        """Criterion 1: BMKG credential rejected by PVMBG, PVMBG credential rejected by BMKG."""
        bmkg_key = CONFIG.get("BMKG_API_KEY", "test-bmkg-key")
        pvmbg_token = CONFIG.get("PVMBG_BEARER_TOKEN", "test-pvmbg-token")

        self.assertNotEqual(bmkg_key, pvmbg_token, "BMKG and PVMBG credentials must be distinct")

        # 1. Send BMKG API Key to PVMBG
        status, _, _ = mock_request(
            "pvmbg",
            "/volcanic-reports",
            custom_auth_header={"X-BMKG-Key": bmkg_key},
        )
        self.assertIn(status, [401, 403], f"PVMBG must reject BMKG header with 401/403, got {status}")

        # 2. Send PVMBG Bearer Token to BMKG
        status, _, _ = mock_request(
            "bmkg",
            "/seismic-events",
            custom_auth_header={"Authorization": f"Bearer {pvmbg_token}"},
        )
        self.assertIn(status, [401, 403], f"BMKG must reject PVMBG Bearer token with 401/403, got {status}")

        # 3. Unauthenticated requests to both must be rejected
        status_bmkg_anon, _, _ = mock_request("bmkg", "/seismic-events", auth=False)
        self.assertIn(status_bmkg_anon, [401, 403], "BMKG must reject unauthenticated request")

        status_pvmbg_anon, _, _ = mock_request("pvmbg", "/volcanic-reports", auth=False)
        self.assertIn(status_pvmbg_anon, [401, 403], "PVMBG must reject unauthenticated request")

    def test_02_scope_enforcement_and_raw_fields_rejection(self):
        """Criterion 2: Media role receives only Summary; explicit request for Raw fields is 403."""
        # 1. Media GET /api/v1/hazards returns only Summary fields
        status, _, body = api("GET", HAZARDS_ENDPOINT, user=MEDIA)
        self.assertEqual(status, 200)
        self.assertTrue(len(body.get("items", [])) > 0)
        for item in body["items"]:
            # Summary classification check
            leaked = FULL_ONLY & item.keys()
            self.assertFalse(
                leaked,
                f"Media summary view leaked raw/full fields: {leaked} in hazard {item.get('hazard_id')}",
            )

        # 2. Explicit request for raw view (view=full) by Media must return 403 Forbidden
        status, _, _ = api("GET", HAZARDS_ENDPOINT, user=MEDIA, query={"view": "full"})
        self.assertEqual(status, 403, f"Media requesting view=full must be rejected with 403, got {status}")

        # 3. Single hazard detail view=full by Media must also return 403 Forbidden
        first_id = body["items"][0]["hazard_id"]
        status, _, _ = api("GET", f"{HAZARDS_ENDPOINT}/{first_id}", user=MEDIA, query={"view": "full"})
        self.assertEqual(status, 403, f"Media requesting single hazard view=full must be rejected with 403, got {status}")

        # 4. Field Team (FIELD_TEAM) and BNPB Internal (BNPB_INTERNAL) have full access
        for privileged_user in (FIELD, BNPB):
            with self.subTest(user=privileged_user):
                st, _, full_body = api("GET", HAZARDS_ENDPOINT, user=privileged_user, query={"view": "full"})
                self.assertEqual(st, 200)
                self.assertTrue(len(full_body.get("items", [])) > 0)
                first_full = full_body["items"][0]
                self.assertIn("source_ref_id", first_full)
                self.assertIn("latitude", first_full)
                self.assertIn("longitude", first_full)

    def test_03_token_ttl_expiry_and_refresh_flow(self):
        """Criterion 3: Token expires after TTL and refresh issues new token without manual login."""
        # Request a fresh access token for Field Team
        token_payload = request_raw_token(FIELD)
        self.assertIn("access_token", token_payload)
        expires_in = token_payload.get("expires_in", 60)
        raw_access_token = token_payload["access_token"]
        print(f"\n[Problem 3 - Criterion 3] Acquired test access token (TTL: {expires_in}s).")

        # 1. Verify token works initially
        status, _, _ = api("GET", f"{HAZARDS_ENDPOINT}/sources/statuses", headers={"Authorization": f"Bearer {raw_access_token}"})
        self.assertEqual(status, 200, "Fresh access token must succeed with 200")

        # 2. Wait for natural token expiration or test token boundary
        print(f"  Waiting for token expiration window (expires_in = {expires_in}s)...")
        # In testing environments, if TTL is 60s, wait until expiry plus 2s grace
        wait_seconds = min(expires_in + 2, 65)
        time.sleep(wait_seconds)

        # 3. Test that expired token is rejected by the gateway / Envoy / Zitadel
        status_expired, _, _ = api("GET", f"{HAZARDS_ENDPOINT}/sources/statuses", headers={"Authorization": f"Bearer {raw_access_token}"})
        self.assertEqual(
            status_expired,
            401,
            f"Expired access token must be rejected with 401 Unauthorized, got {status_expired}",
        )
        print("  SUCCESS: Expired access token was correctly rejected with HTTP 401.")

        # 4. Request a new token via refresh/client-credentials flow without manual user credentials
        new_token_payload = request_raw_token(FIELD)
        new_access_token = new_token_payload["access_token"]
        self.assertNotEqual(raw_access_token, new_access_token, "New access token must differ from expired token")

        # 5. Verify new token works immediately
        status_new, _, _ = api("GET", f"{HAZARDS_ENDPOINT}/sources/statuses", headers={"Authorization": f"Bearer {new_access_token}"})
        self.assertEqual(status_new, 200, "Refreshed access token must succeed with 200 OK")
        print("  SUCCESS: Refreshed token authenticated successfully without manual re-login.")

    def test_04_no_hardcoded_credentials_and_env_example_complete(self):
        """Criterion 4: Audit repository against hardcoded secrets and verify .env.example."""
        env_example_path = ROOT / ".env.example"
        self.assertTrue(env_example_path.is_file(), ".env.example must exist in repository root")

        env_example_text = env_example_path.read_text(encoding="utf-8")
        required_vars = [
            "BMKG_API_KEY",
            "PVMBG_BEARER_TOKEN",
            "AGGREGATOR_GRPC_PORT",
            "CANONICAL_STORE_DSN",
            "RABBITMQ_DEFAULT_USER",
            "RABBITMQ_DEFAULT_PASS",
            "RABBITMQ_DEFAULT_VHOST",
        ]
        for var in required_vars:
            self.assertIn(var, env_example_text, f".env.example must document {var}")

        # Scan for sensitive patterns across committed source files (excluding git, cache, and .secrets)
        print("\n[Problem 3 - Criterion 4] Scanning codebase for potential hardcoded secrets...")
        suspicious_patterns = [
            re.compile(r"""(?i)(?:api_key|bearer_token|password|client_secret)\s*[:=]\s*["'](?!REPLACE_|\$|guest|postgres|admin@|test)[A-Za-z0-9_\-]{20,}["']"""),
            re.compile(r"-----BEGIN\s+(?:RSA|EC|OPENSSH)?\s*PRIVATE KEY-----"),
        ]

        allowed_extensions = {".go", ".rs", ".ts", ".tsx", ".js", ".mjs", ".cs", ".py", ".yaml", ".yml"}
        flagged_files = []

        for root, dirs, files in os.walk(ROOT):
            # Ignore hidden and cache directories
            dirs[:] = [d for d in dirs if not d.startswith(".") and d not in ("node_modules", "target", "bin", "obj", "data")]
            for file in files:
                if file.startswith(".env") and not file.endswith(".example"):
                    continue
                file_path = Path(root) / file
                if file_path.suffix in allowed_extensions:
                    try:
                        content = file_path.read_text(encoding="utf-8", errors="ignore")
                        for pat in suspicious_patterns:
                            if pat.search(content):
                                # Exclude test fixtures
                                if "test" not in file_path.name.lower() and "fixture" not in file_path.name.lower():
                                    flagged_files.append(str(file_path.relative_to(ROOT)))
                                    break
                    except Exception:
                        pass

        self.assertEqual(
            flagged_files,
            [],
            f"Found potentially hardcoded credentials in files: {flagged_files}",
        )
        print("  SUCCESS: Codebase audit passed. No hardcoded credentials detected.")


if __name__ == "__main__":
    unittest.main(verbosity=2)
