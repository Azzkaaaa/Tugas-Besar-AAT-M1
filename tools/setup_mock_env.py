"""Provision independent local mock credentials without displaying them."""

from pathlib import Path
import re
import secrets


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    path = ROOT / ".env"
    content = (path if path.exists() else ROOT / ".env.example").read_text(encoding="utf-8-sig")
    for name in ("BMKG_API_KEY", "PVMBG_BEARER_TOKEN"):
        pattern = re.compile(rf"^{name}=(.*)$", re.MULTILINE)
        match = pattern.search(content)
        value = match.group(1).strip().strip("\"'") if match else ""
        if not value or value.startswith("REPLACE_"):
            line = f"{name}={secrets.token_urlsafe(32)}"
            content = pattern.sub(lambda _: line, content) if match else content.rstrip() + "\n" + line + "\n"
    values = dict(re.findall(r"^(BMKG_API_KEY|PVMBG_BEARER_TOKEN)=(.*)$", content, re.MULTILINE))
    if values["BMKG_API_KEY"].strip().strip("\"'") == values["PVMBG_BEARER_TOKEN"].strip().strip("\"'"):
        raise SystemExit("BMKG_API_KEY dan PVMBG_BEARER_TOKEN harus berbeda; perbaiki .env lokal.")
    path.write_text(content, encoding="utf-8")
    print(".env siap; kredensial mock terpisah tersimpan lokal dan tidak ditampilkan.")


if __name__ == "__main__":
    main()
