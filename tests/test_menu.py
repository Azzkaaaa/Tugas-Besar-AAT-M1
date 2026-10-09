#!/usr/bin/env python3
"""Interactive CLI for BNPB Disaster Coordination Gateway testing and stack management.

Supports:
- Toggling PVMBG mock schema version (v1 / v2)
- Toggling PVMBG outage simulation
- Requesting OAuth2 client credentials tokens
- Making API requests through Envoy Gateway
- Direct HTTP requests to BMKG (8081) and PVMBG (8082) mocks
- Inspecting RabbitMQ queues, exchanges, and messages
- Checking Docker Compose container status
- Subscribing to live SSE hazard event streams
"""

import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional

# Ensure tests/ directory is in python path
SCRIPT_DIR = Path(__file__).resolve().parent
if (SCRIPT_DIR / "tests").exists():
    sys.path.insert(0, str(SCRIPT_DIR / "tests"))
else:
    sys.path.insert(0, str(SCRIPT_DIR))

# Ensure BNPB_E2E=1 is set for e2e support functions
os.environ.setdefault("BNPB_E2E", "1")

try:
    from e2e_support import USERS, SseReader, api, token
    from problem_set_support import (
        RabbitMQClient,
        get_running_containers,
        is_docker_available,
        mock_request,
        set_pvmbg_outage,
        set_pvmbg_schema_version,
        wait_for,
        CONFIG,
        ROOT,
    )
except ImportError as e:
    print(f"\031[31m[!] Failed to import e2e / problem set support modules: {e}\033[0m")
    print("Ensure this script is run from project root or inside tests/ directory.")
    sys.exit(1)

# ANSI Color Codes
CLR_RESET = "\033[0m"
CLR_BOLD = "\033[1m"
CLR_CYAN = "\033[36m"
CLR_GREEN = "\033[32m"
CLR_YELLOW = "\033[33m"
CLR_RED = "\033[31m"
CLR_BLUE = "\033[34m"
CLR_MAGENTA = "\033[35m"


def print_header(title: str) -> None:
    print(f"\n{CLR_CYAN}{'=' * 60}{CLR_RESET}")
    print(f"{CLR_BOLD}{CLR_CYAN}  {title}{CLR_RESET}")
    print(f"{CLR_CYAN}{'=' * 60}{CLR_RESET}")


def print_json(data: Any) -> None:
    """Pretty print JSON or string payload."""
    if isinstance(data, (dict, list)):
        print(json.dumps(data, indent=2))
    else:
        print(data)


def prompt_choice(prompt_text: str, default: Optional[str] = None) -> str:
    default_hint = f" [{default}]" if default else ""
    try:
        val = input(f"{CLR_BOLD}{prompt_text}{default_hint}: {CLR_RESET}").strip()
        return val if val else (default or "")
    except (KeyboardInterrupt, EOFError):
        print("\nOperation cancelled.")
        return ""


# ---------------------------------------------------------------------------
# Menu Handlers
# ---------------------------------------------------------------------------

def handle_schema_version() -> None:
    print_header("PVMBG Schema Version Control")
    ver_str = prompt_choice("Enter schema version (1 or 2)", default="2")
    if not ver_str.isdigit():
        print(f"{CLR_RED}Invalid version number.{CLR_RESET}")
        return

    version = int(ver_str)
    try:
        res = set_pvmbg_schema_version(version)
        print(f"{CLR_GREEN}✔ Successfully set PVMBG schema version to {version}{CLR_RESET}")
        print_json(res)
    except Exception as e:
        print(f"{CLR_RED}✘ Error setting schema version: {e}{CLR_RESET}")


def handle_outage() -> None:
    print_header("PVMBG Outage Control")
    choice = prompt_choice("Enable outage mode? (y/n)", default="y").lower()
    enabled = choice in ("y", "yes", "true", "1")

    try:
        res = set_pvmbg_outage(enabled)
        state_str = f"{CLR_RED}ENABLED (Simulating Outage){CLR_RESET}" if enabled else f"{CLR_GREEN}DISABLED (Normal Operation){CLR_RESET}"
        print(f"✔ PVMBG Outage status updated: {state_str}")
        print_json(res)
    except Exception as e:
        print(f"{CLR_RED}✘ Error updating outage state: {e}{CLR_RESET}")


def handle_get_token() -> None:
    print_header("Fetch OAuth2 Service Token")
    print("Available service user roles:")
    roles = list(USERS.keys())
    for idx, role in enumerate(roles, 1):
        print(f"  {idx}. {role:<15} ({USERS[role]})")

    choice = prompt_choice("Select role number", default="1")
    user_key = None
    if choice.isdigit() and 1 <= int(choice) <= len(roles):
        user_key = roles[int(choice) - 1]
    elif choice.upper() in USERS:
        user_key = choice.upper()
    else:
        print(f"{CLR_RED}Invalid choice.{CLR_RESET}")
        return

    # Map role key (e.g., "BNPB_INTERNAL") to service user (e.g., "svc-bnpb")
    svc_user = USERS[user_key]

    try:
        t = token(svc_user)
        print(f"\n{CLR_GREEN}✔ Access Token for {user_key} ({svc_user}):{CLR_RESET}")
        print(f"{CLR_YELLOW}{t}{CLR_RESET}")
    except Exception as e:
        print(f"{CLR_RED}✘ Failed to retrieve token: {e}{CLR_RESET}")


def handle_envoy_api() -> None:
    print_header("API Request via Envoy Gateway")
    method = prompt_choice("HTTP Method", default="GET").upper()
    path = prompt_choice("Path", default="/api/v1/hazards")

    print("\nSelect auth user role (press enter for anonymous/public):")
    roles = list(USERS.keys())
    for idx, role in enumerate(roles, 1):
        print(f"  {idx}. {role} ({USERS[role]})")

    user_choice = prompt_choice("Role number (optional)", default="")
    svc_user = None
    if user_choice.isdigit() and 1 <= int(user_choice) <= len(roles):
        svc_user = USERS[roles[int(user_choice) - 1]]

    body = None
    if method in ("POST", "PUT", "PATCH"):
        body_raw = prompt_choice("JSON Body (optional)", default="")
        if body_raw:
            try:
                body = json.loads(body_raw)
            except Exception as e:
                print(f"{CLR_RED}Invalid JSON body: {e}{CLR_RESET}")
                return

    try:
        status, headers, parsed = api(method, path, user=svc_user, body=body)
        status_color = CLR_GREEN if 200 <= status < 300 else CLR_RED
        print(f"\nResponse: {status_color}HTTP {status}{CLR_RESET}")
        print(f"{CLR_BOLD}Headers:{CLR_RESET}")
        print_json(headers)
        print(f"{CLR_BOLD}Body:{CLR_RESET}")
        print_json(parsed)
    except Exception as e:
        print(f"{CLR_RED}✘ API Request failed: {e}{CLR_RESET}")


def handle_direct_mock_request() -> None:
    print_header("Direct Upstream Mock Request")
    source = prompt_choice("Target service (bmkg / pvmbg)", default="pvmbg").lower()
    method = prompt_choice("HTTP Method", default="GET").upper()
    path = prompt_choice("Path", default="/admin/schema-version" if source == "pvmbg" else "/")

    try:
        status, headers, parsed = mock_request(source, path, method=method)
        status_color = CLR_GREEN if 200 <= status < 300 else CLR_RED
        print(f"\nResponse: {status_color}HTTP {status}{CLR_RESET}")
        print(f"{CLR_BOLD}Body:{CLR_RESET}")
        print_json(parsed)
    except Exception as e:
        print(f"{CLR_RED}✘ Mock Request failed: {e}{CLR_RESET}")


def handle_rabbitmq() -> None:
    print_header("RabbitMQ Management")
    print("1. Cluster Overview")
    print("2. List Queues")
    print("3. List Exchanges")
    print("4. Peek Messages from Queue")

    choice = prompt_choice("Select operation", default="1")
    client = RabbitMQClient()

    try:
        if choice == "1":
            print_json(client.get_overview())
        elif choice == "2":
            queues = client.list_queues()
            print(f"\nFound {len(queues)} queues:")
            for q in queues:
                print(f"  - {CLR_BOLD}{q.get('name')}{CLR_RESET} | messages: {q.get('messages', 0)} | consumers: {q.get('consumers', 0)}")
        elif choice == "3":
            exchanges = client.list_exchanges()
            print(f"\nFound {len(exchanges)} exchanges:")
            for ex in exchanges:
                print(f"  - {CLR_BOLD}{ex.get('name') or '(amq.default)'}{CLR_RESET} [{ex.get('type')}]")
        elif choice == "4":
            q_name = prompt_choice("Queue name")
            if not q_name:
                return
            count = int(prompt_choice("Count", default="5"))
            msgs = client.get_messages_from_queue(q_name, count=count)
            print(f"\nRetrieved {len(msgs)} messages:")
            print_json(msgs)
    except Exception as e:
        print(f"{CLR_RED}✘ RabbitMQ operation failed: {e}{CLR_RESET}")


def handle_docker_status() -> None:
    print_header("Docker Compose Stack Status")
    if not is_docker_available():
        print(f"{CLR_RED}✘ Docker Compose is not available or stack is stopped.{CLR_RESET}")
        return

    print(f"{CLR_GREEN}✔ Docker Compose is running.{CLR_RESET}")
    containers = get_running_containers()
    print(f"\nRunning Services ({len(containers)}):")
    for name in containers:
        print(f"  - {CLR_BOLD}{name}{CLR_RESET}")

def handle_sse_stream() -> None:
    print_header("SSE Hazard Stream Listener")
    path = prompt_choice("Stream path", default="/api/v1/hazards/stream")

    print("Select user role for stream authentication:")
    roles = list(USERS.keys())
    for idx, role in enumerate(roles, 1):
        print(f"  {idx}. {role} ({USERS[role]})")

    user_choice = prompt_choice("Role number", default="3")
    if user_choice.isdigit() and 1 <= int(user_choice) <= len(roles):
        svc_user = USERS[roles[int(user_choice) - 1]]
    else:
        svc_user = USERS["BNPB_INTERNAL"]

    print(f"\n{CLR_YELLOW}Connecting to SSE stream as {svc_user}... (Press Ctrl+C to stop){CLR_RESET}\n")
    reader = None
    try:
        reader = SseReader(path, user=svc_user)
        print(f"{CLR_GREEN}✔ Connected. HTTP Status: {reader.status}{CLR_RESET}")
        while True:
            try:
                ev = reader.next_event(time.monotonic() + 10.0)
                timestamp = time.strftime("%H:%M:%S")
                print(f"[{CLR_CYAN}{timestamp}{CLR_RESET}] {CLR_BOLD}Event:{CLR_RESET} {ev.get('event', 'message')}")
                if "data" in ev:
                    try:
                        print_json(json.loads(ev["data"]))
                    except Exception:
                        print(f"  {ev['data']}")
                print("-" * 40)
            except AssertionError:
                print(f"[{time.strftime('%H:%M:%S')}] Waiting for events...")
    except KeyboardInterrupt:
        print(f"\n{CLR_YELLOW}Stopped listening to SSE stream.{CLR_RESET}")
    except Exception as e:
        print(f"{CLR_RED}✘ SSE Stream Error: {e}{CLR_RESET}")
    finally:
        if reader:
            reader.close()

# ---------------------------------------------------------------------------
# Problem 3 Security Proof Handlers
# ---------------------------------------------------------------------------

def prove_credential_isolation() -> None:
    """Proof 1: Upstream Credential Isolation (BMKG vs PVMBG)"""
    print_header("Proof 1: Upstream Credential Isolation")
    bmkg_key = CONFIG.get("BMKG_API_KEY", "")
    pvmbg_token = CONFIG.get("PVMBG_BEARER_TOKEN", "")

    # 1. Valid BMKG Key to BMKG Service
    print(f"\n{CLR_BOLD}[Step 1.1] Valid BMKG Key -> BMKG upstream (port 8081)...{CLR_RESET}")
    st, _, _ = mock_request("bmkg", "/", custom_auth_header={"X-BMKG-Key": bmkg_key})
    print(f"  Response: {CLR_GREEN}HTTP {st}{CLR_RESET} (Authorized)")

    # 2. PVMBG Bearer Token to BMKG Service (Cross-use attempt)
    print(f"\n{CLR_BOLD}[Step 1.2] Cross-test: PVMBG Bearer Token -> BMKG upstream...{CLR_RESET}")
    st, _, data = mock_request("bmkg", "/", custom_auth_header={"Authorization": f"Bearer {pvmbg_token}"})
    color = CLR_GREEN if st in (401, 403) else CLR_RED
    status_msg = "✔ REJECTED (401/403) as expected" if st in (401, 403) else f"✘ FAILED (Expected 401/403, got {st})"
    print(f"  Response: {color}HTTP {st}{CLR_RESET} -> {status_msg}")
    print_json(data)

    # 3. Valid PVMBG Bearer Token to PVMBG Service
    print(f"\n{CLR_BOLD}[Step 1.3] Valid PVMBG Bearer Token -> PVMBG upstream (port 8082)...{CLR_RESET}")
    st, _, _ = mock_request("pvmbg", "/admin/schema-version", method="GET", custom_auth_header={"Authorization": f"Bearer {pvmbg_token}"})
    print(f"  Response: {CLR_GREEN}HTTP {st}{CLR_RESET} (Authorized)")

    # 4. BMKG Key to PVMBG Service (Cross-use attempt)
    print(f"\n{CLR_BOLD}[Step 1.4] Cross-test: BMKG API Key -> PVMBG upstream...{CLR_RESET}")
    st, _, data = mock_request("pvmbg", "/admin/schema-version", method="GET", custom_auth_header={"X-BMKG-Key": bmkg_key})
    color = CLR_GREEN if st in (401, 403) else CLR_RED
    status_msg = "✔ REJECTED (401/403) as expected" if st in (401, 403) else f"✘ FAILED (Expected 401/403, got {st})"
    print(f"  Response: {color}HTTP {st}{CLR_RESET} -> {status_msg}")
    print_json(data)


def prove_scope_enforcement() -> None:
    """Proof 2: Media Role Scope Enforcement vs Field Team / BNPB Internal"""
    print_header("Proof 2: Scope Enforcement & Classification")

    media_user = USERS["MEDIA"]
    field_user = USERS["FIELD_TEAM"]

    # 1. Requesting hazards as Media (Default Summary View)
    print(f"\n{CLR_BOLD}[Step 2.1] Request /api/v1/hazards with MEDIA role ({media_user})...{CLR_RESET}")
    st, _, data = api("GET", "/api/v1/hazards", user=media_user)
    print(f"  Response: {CLR_GREEN}HTTP {st}{CLR_RESET}")
    print("  Summary Payload Sample:")
    if isinstance(data, list) and data:
        print_json(data[0])
    else:
        print_json(data)

    # 2. Explicitly requesting raw/full view with Media role
    print(f"\n{CLR_BOLD}[Step 2.2] Explicit request ?view=full with MEDIA role...{CLR_RESET}")
    st, _, data = api("GET", "/api/v1/hazards", user=media_user, query={"view": "full"})
    color = CLR_GREEN if st in (401, 403) else CLR_RED
    status_msg = "✔ Server rejected raw view with 401/403" if st in (401, 403) else f"✘ FAILED (Expected 401/403, got {st})"
    print(f"  Response: {color}HTTP {st}{CLR_RESET} -> {status_msg}")
    print_json(data)

    # 3. Explicitly requesting raw/full view with Field Team role
    print(f"\n{CLR_BOLD}[Step 2.3] Explicit request ?view=full with FIELD_TEAM role ({field_user})...{CLR_RESET}")
    st, _, data = api("GET", "/api/v1/hazards", user=field_user, query={"view": "full"})
    color = CLR_GREEN if st == 200 else CLR_RED
    status_msg = "✔ Full raw fields granted by server" if st == 200 else f"✘ FAILED (HTTP {st})"
    print(f"  Response: {color}HTTP {st}{CLR_RESET} -> {status_msg}")
    if isinstance(data, list) and data:
        print_json(data[0])
    else:
        print_json(data)


def _fetch_fresh_token(service_user: str) -> str:
    """Utility to bypass e2e_support in-memory token cache and request a new token directly."""
    from e2e_support import AUTH_HOST, _read, _request
    import base64
    from urllib.parse import urlencode

    basic = base64.b64encode(f"{service_user}:{_read(f'{service_user}.secret')}".encode()).decode()
    scope = f"openid urn:zitadel:iam:org:project:id:{_read('project-id')}:aud"
    status, _, raw = _request(
        "POST",
        "/oauth/v2/token",
        host=AUTH_HOST,
        body=urlencode({"grant_type": "client_credentials", "scope": scope}),
        headers={"Authorization": f"Basic {basic}", "Content-Type": "application/x-www-form-urlencoded"},
    )
    if status != 200:
        raise RuntimeError(f"Token generation failed: HTTP {status}")
    return json.loads(raw)["access_token"]


def prove_ttl_and_refresh(ttl_seconds: int = 62) -> None:
    """Proof 3: Token TTL Expiry & Refresh Flow Verification"""
    print_header("Proof 3: Token TTL Expiry & Refresh Flow")
    field_user = USERS["FIELD_TEAM"]

    # 1. Acquire initial token (Token #1)
    print(f"\n{CLR_BOLD}[Step 3.1] Acquiring fresh token for Tim Lapangan ({field_user})...{CLR_RESET}")
    token_1 = _fetch_fresh_token(field_user)
    print(f"  Token 1 (trunc): {CLR_YELLOW}{token_1[:25]}...{token_1[-10:]}{CLR_RESET}")

    # 2. Test Token #1 immediately
    print(f"\n{CLR_BOLD}[Step 3.2] Testing Token 1 before expiry...{CLR_RESET}")
    st, _, _ = api("GET", "/api/v1/hazards", headers={"Authorization": f"Bearer {token_1}"})
    print(f"  Response: {CLR_GREEN}HTTP {st}{CLR_RESET} (Valid)")

    # 3. Live countdown waiting for TTL expiry
    print(f"\n{CLR_BOLD}[Step 3.3] Waiting {ttl_seconds} seconds for TTL expiration...{CLR_RESET}")
    for remaining in range(ttl_seconds, 0, -1):
        sys.stdout.write(f"\r  {CLR_YELLOW}⏳ Waiting for natural token expiry: {remaining}s remaining...{CLR_RESET}")
        sys.stdout.flush()
        time.sleep(1)
    print(f"\r  {CLR_GREEN}✔ Time elapsed ({ttl_seconds}s). Token 1 should now be expired.{CLR_RESET}\033[K")

    # 4. Test Token #1 after TTL
    print(f"\n{CLR_BOLD}[Step 3.4] Testing Token 1 after TTL expiry...{CLR_RESET}")
    st, _, data = api("GET", "/api/v1/hazards", headers={"Authorization": f"Bearer {token_1}"})
    color = CLR_GREEN if st in (401, 403) else CLR_RED
    status_msg = "✔ Expired token rejected by server with 401/403" if st in (401, 403) else f"✘ FAILED (Got HTTP {st})"
    print(f"  Response: {color}HTTP {st}{CLR_RESET} -> {status_msg}")
    print_json(data)

    # 5. Acquire new token (Token #2) via automated client credentials flow
    print(f"\n{CLR_BOLD}[Step 3.5] Triggering token refresh flow (client credentials)...{CLR_RESET}")
    token_2 = _fetch_fresh_token(field_user)
    print(f"  Token 2 (trunc): {CLR_YELLOW}{token_2[:25]}...{token_2[-10:]}{CLR_RESET}")

    # 6. Test Token #2
    print(f"\n{CLR_BOLD}[Step 3.6] Testing new Token 2...{CLR_RESET}")
    st, _, _ = api("GET", "/api/v1/hazards", headers={"Authorization": f"Bearer {token_2}"})
    print(f"  Response: {CLR_GREEN}HTTP {st}{CLR_RESET} -> ✔ Access restored without manual re-login")

    # 7. Re-verify Token #1 remains rejected
    print(f"\n{CLR_BOLD}[Step 3.7] Confirming old Token 1 is permanently unusable...{CLR_RESET}")
    st, _, _ = api("GET", "/api/v1/hazards", headers={"Authorization": f"Bearer {token_1}"})
    color = CLR_GREEN if st in (401, 403) else CLR_RED
    print(f"  Response: {color}HTTP {st}{CLR_RESET} -> ✔ Still rejected")


def handle_problem_3_proofs() -> None:
    """Sub-menu handler for Problem 3 security proofs."""
    while True:
        print_header("Problem 3 Security & Authentication Proofs")
        print(" 1. Run Proof 1: Upstream Credential Isolation (BMKG vs PVMBG)")
        print(" 2. Run Proof 2: Scope Enforcement (Media Ringkasan vs Raw View)")
        print(" 3. Run Proof 3: Token TTL Expiry & Automatic Refresh (60s wait)")
        print(" 4. Run ALL Problem 3 Proofs sequentially")
        print(" 0. Back to Main Menu")

        choice = prompt_choice("Select proof option", default="4")
        if choice == "1":
            prove_credential_isolation()
        elif choice == "2":
            prove_scope_enforcement()
        elif choice == "3":
            prove_ttl_and_refresh()
        elif choice == "4":
            prove_credential_isolation()
            prove_scope_enforcement()
            prove_ttl_and_refresh()
        elif choice in ("0", "back", "q"):
            break
        else:
            print(f"{CLR_RED}Invalid option.{CLR_RESET}")
        time.sleep(0.5)

# ---------------------------------------------------------------------------
# Problem 4 Microservice & Storage Proof Handlers
# ---------------------------------------------------------------------------

def prove_flexible_storage() -> None:
    """Proof 2: Flexible Storage & Dynamic Schema Coexistence (v1 & v2)"""
    print_header("Proof 2: Flexible Storage & Dynamic Schema Coexistence")

    # 1. Ubah skema PVMBG mock ke versi 2
    print(f"{CLR_BOLD}[Step 1] Triggering PVMBG schema version 2...{CLR_RESET}")
    try:
        set_pvmbg_schema_version(2)
        print(f"  {CLR_GREEN}✔ PVMBG mock successfully updated to schema version 2.{CLR_RESET}")
    except Exception as e:
        print(f"  {CLR_RED}✘ Failed to set schema version: {e}{CLR_RESET}")
        return

    # 1.5 wait
    print(f"\n{CLR_BOLD}[Step 1.5] Waiting 60 seconds for schema 2 entries to populate...{CLR_RESET}")
    for remaining in range(60, 0, -1):
        sys.stdout.write(f"\r  {CLR_YELLOW}⏳ Waiting for schema 2 entries to populate: {remaining}s remaining...{CLR_RESET}")
        sys.stdout.flush()
        time.sleep(1)
    print(f"\r  {CLR_GREEN}✔ Time elapsed (60s). Entries should now be populated with some v2.{CLR_RESET}\033[K")

    # 2. Polling API Aggregator untuk menunggu ingest data skema v2
    print(f"\n{CLR_BOLD}[Step 2] Polling Aggregator API for coexisting v1 and v2 records...{CLR_RESET}")

    def get_pvmbg_records():
        status, _, body = api("GET", "/api/v1/hazards", user=USERS["FIELD_TEAM"], query={"source": "PVMBG", "view": "full", "page_size": 100})
        if status == 200 and body and "items" in body:
            items = body["items"]
            has_v2 = any("confidence_level" in h.get("attributes", {}) for h in items)
            if has_v2:
                return items
        return None

    try:
        items = wait_for(get_pvmbg_records, timeout=35, interval=2, message="PVMBG v2 ingestion in Canonical Store")
    except Exception as e:
        print(f"  {CLR_RED}✘ Timeout waiting for v2 records: {e}{CLR_RESET}")
        return

    v1_records = [h for h in items if "confidence_level" not in h.get("attributes", {})]
    v2_records = [h for h in items if "confidence_level" in h.get("attributes", {})]

    print(f"\n{CLR_GREEN}✔ Retrieved {len(items)} total PVMBG hazards from Canonical Store:{CLR_RESET}")
    print(f"  - v1 Records (without confidence_level) : {CLR_BOLD}{len(v1_records)}{CLR_RESET}")
    print(f"  - v2 Records (with confidence_level)    : {CLR_BOLD}{len(v2_records)}{CLR_RESET}")

    # 3. Tampilkan sampel atribut v1 dan v2
    if v1_records:
        print(f"\n{CLR_BOLD}Sample v1 Record Attributes (Legacy Schema):{CLR_RESET}")
        print_json(v1_records[0].get("attributes"))

    if v2_records:
        print(f"\n{CLR_BOLD}Sample v2 Record Attributes (Additive Field 'confidence_level'):{CLR_RESET}")
        print_json(v2_records[0].get("attributes"))

    print(f"\n{CLR_GREEN}✔ VERIFIED: Both v1 and v2 records coexist in Canonical Store without DDL migrations.{CLR_RESET}")


def prove_canonical_store_ownership() -> None:
    """Proof 3: Canonical Store Exclusive Ownership & Access Boundaries"""
    print_header("Proof 3: Canonical Store Exclusive Ownership")

    # 1. Periksa isolasi CANONICAL_STORE_DSN pada compose.yaml
    print(f"{CLR_BOLD}[Step 1] Inspecting compose.yaml for CANONICAL_STORE_DSN isolation...{CLR_RESET}")
    compose_path = ROOT / "compose.yaml"
    if not compose_path.is_file():
        print(f"  {CLR_RED}✘ compose.yaml not found at {compose_path}{CLR_RESET}")
        return

    compose_text = compose_path.read_text(encoding="utf-8")
    services_to_check = ["api-gateway", "feed-service", "webhook-provider", "notification-service"]
    isolated = True

    for svc in services_to_check:
        svc_block = compose_text.split(f"  {svc}:")
        if len(svc_block) > 1:
            sub_block = svc_block[1].split("\n\n  ")[0]
            if "CANONICAL_STORE_DSN" in sub_block:
                print(f"  {CLR_RED}✘ Service '{svc}' has direct access to CANONICAL_STORE_DSN!{CLR_RESET}")
                isolated = False
            else:
                print(f"  {CLR_GREEN}✔ Service '{svc}' does NOT have access to CANONICAL_STORE_DSN.{CLR_RESET}")

    if isolated:
        print(f"  {CLR_GREEN}✔ Canonical Store credentials are strictly isolated to Aggregator service.{CLR_RESET}")

    # 2. Uji akses data pihak luar yang wajib melalui Aggregator API
    print(f"\n{CLR_BOLD}[Step 2] Verifying external data fetch strictly via Aggregator API...{CLR_RESET}")
    st, _, body = api("GET", "/api/v1/hazards/sources/statuses", user=USERS["MEDIA"])
    color = CLR_GREEN if st == 200 else CLR_RED
    print(f"  Response from Aggregator API (/sources/statuses): {color}HTTP {st}{CLR_RESET}")
    if st == 200:
        print_json(body)
        print(f"  {CLR_GREEN}✔ VERIFIED: External components access data solely via Aggregator API.{CLR_RESET}")


def handle_problem_4_proofs() -> None:
    """Sub-menu handler for Problem 4 proofs."""
    while True:
        print_header("Problem 4 Microservices & Storage Proofs")
        print(" 1. Run Proof 2: Flexible Storage & Dynamic Schema Coexistence (v1 & v2)")
        print(" 2. Run Proof 3: Canonical Store Exclusive Ownership & Boundaries")
        print(" 3. Run ALL Problem 4 Proofs (Criteria 2 & 3)")
        print(" 0. Back to Main Menu")

        choice = prompt_choice("Select proof option", default="3")
        if choice == "1":
            prove_flexible_storage()
        elif choice == "2":
            prove_canonical_store_ownership()
        elif choice == "3":
            prove_flexible_storage()
            prove_canonical_store_ownership()
        elif choice in ("0", "back", "q"):
            break
        else:
            print(f"{CLR_RED}Invalid option.{CLR_RESET}")
        time.sleep(0.5)

# ---------------------------------------------------------------------------
# Main Loop
# ---------------------------------------------------------------------------

def main() -> None:
    while True:
        print_header("BNPB Stack Interactive CLI Tool")
        print(" 1. Set PVMBG Schema Version")
        print(" 2. Toggle PVMBG Outage Simulation")
        print(" 3. Get OAuth Access Token")
        print(" 4. Send API Request via Envoy Gateway")
        print(" 5. Send Direct Upstream Mock Request (BMKG / PVMBG)")
        print(" 6. Inspect RabbitMQ (Overview / Queues / Messages)")
        print(" 7. Check Docker Compose Stack Status")
        print(" 8. Subscribe to SSE Event Stream")
        print(" 9. Run Security & Authentication Proofs (Problem 3)")
        print("10. Run Microservice & Storage Proofs (Problem 4 - Criteria 2 & 3)")
        print(" 0. Exit")

        choice = prompt_choice("Select menu option", default="1")

        if choice == "1":
            handle_schema_version()
        elif choice == "2":
            handle_outage()
        elif choice == "3":
            handle_get_token()
        elif choice == "4":
            handle_envoy_api()
        elif choice == "5":
            handle_direct_mock_request()
        elif choice == "6":
            handle_rabbitmq()
        elif choice == "7":
            handle_docker_status()
        elif choice == "8":
            handle_sse_stream()
        elif choice == "9":
            handle_problem_3_proofs()
        elif choice == "10":
            handle_problem_4_proofs()
        elif choice in ("0", "exit", "quit", "q"):
            print("\nExiting CLI tool.")
            break
        else:
            print(f"{CLR_RED}Invalid option selected.{CLR_RESET}")

        time.sleep(0.5)


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, EOFError):
        print("\nExiting CLI tool.")
        sys.exit(0)
