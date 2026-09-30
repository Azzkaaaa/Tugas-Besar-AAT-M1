"""Run one independent service with only its own .env settings."""

import argparse
import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
COMMON = {"MOCK_SEED_COUNT", "MOCK_EVENT_INTERVAL_MS"}
OWNED = {
    "bmkg": {"BMKG_API_KEY", "BMKG_HTTP_PORT", "BMKG_STORE_DSN", "BMKG_RESPONSE_DELAY_MS"},
    "pvmbg": {"PVMBG_BEARER_TOKEN", "PVMBG_HTTP_PORT", "PVMBG_STORE_DSN",
              "PVMBG_RESPONSE_DELAY_MIN_MS", "PVMBG_RESPONSE_DELAY_MAX_MS"},
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("service", choices=OWNED)
    args = parser.parse_args()
    allowed = COMMON | OWNED[args.service]
    env = os.environ.copy()
    for service, names in OWNED.items():
        if service != args.service:
            for name in names:
                env.pop(name, None)
    path = ROOT / ".env"
    if path.exists():
        for line in path.read_text(encoding="utf-8-sig").splitlines():
            if "=" not in line or line.lstrip().startswith("#"):
                continue
            name, value = line.split("=", 1)
            if name.strip() in allowed:
                env.setdefault(name.strip(), value.strip().strip("\"'"))
    process = subprocess.Popen(
        [sys.executable, "-u", str(ROOT / "services" / f"{args.service}-mock" / "app.py")],
        cwd=ROOT, env=env,
    )
    try:
        return process.wait()
    except KeyboardInterrupt:
        # Ctrl+C also reaches the child in this process group.
        try:
            return process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.terminate()
            return process.wait(timeout=5)


if __name__ == "__main__":
    sys.exit(main())
