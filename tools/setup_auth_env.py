"""Create local Auth secrets once; preserve existing values on subsequent runs."""

from pathlib import Path
import re
import secrets


ROOT = Path(__file__).resolve().parents[1]
SECRET_FACTORIES = {
    "ZITADEL_MASTERKEY": lambda: secrets.token_hex(16),
    "AUTH_POSTGRES_PASSWORD": lambda: secrets.token_hex(24),
    "LOGIN_SESSION_COOKIE_SECRET": lambda: secrets.token_hex(32),
    "AUTH_SESSION_KEY": lambda: secrets.token_hex(32),
    "GATEWAY_SERVICE_TOKEN": lambda: secrets.token_urlsafe(32),
    "AUTH_ADMIN_PASSWORD": lambda: "Aa1!" + secrets.token_urlsafe(20),
    "AUTH_MEDIA_PASSWORD": lambda: "Aa1!" + secrets.token_urlsafe(20),
    "AUTH_FIELD_TEAM_PASSWORD": lambda: "Aa1!" + secrets.token_urlsafe(20),
    "AUTH_BNPB_PASSWORD": lambda: "Aa1!" + secrets.token_urlsafe(20),
}


def configure(root: Path) -> Path:
    path = root / ".env.auth"
    template = root / ".env.auth.example"
    content = (path if path.exists() else template).read_text(encoding="utf-8-sig")
    for name, factory in SECRET_FACTORIES.items():
        pattern = re.compile(rf"^{name}=(.*)$", re.MULTILINE)
        match = pattern.search(content)
        value = match.group(1).strip().strip("\"'") if match else ""
        if not value or value.startswith("REPLACE_"):
            line = f"{name}={factory()}"
            content = pattern.sub(lambda _: line, content) if match else content.rstrip() + "\n" + line + "\n"
    values = dict(re.findall(r"^([A-Z_]+)=(.*)$", content, re.MULTILINE))
    if len(values["ZITADEL_MASTERKEY"].strip().strip("\"'")) != 32:
        raise ValueError("ZITADEL_MASTERKEY harus tepat 32 karakter; nilai yang ada tidak diubah.")
    if not re.fullmatch(r"[a-fA-F0-9]{64}", values["AUTH_SESSION_KEY"].strip().strip("\"'")):
        raise ValueError("AUTH_SESSION_KEY harus 64 karakter hexadecimal; nilai yang ada tidak diubah.")
    if path.exists():
        path.write_text(content, encoding="utf-8")
    else:
        # Exclusive creation prevents two setup processes from silently replacing secrets.
        with path.open("x", encoding="utf-8") as stream:
            stream.write(content)
    try:
        path.chmod(0o600)
    except OSError:
        pass  # Windows controls access through the user's directory ACL.
    return path


def main() -> None:
    configure(ROOT)
    print(".env.auth siap. Secret yang sudah ada dipertahankan; nilainya tidak ditampilkan.")
    print("Akun lokal: media-demo, lapangan-demo, bnpb-demo, admin@bnpb.local.")
    print("Password awal ada di AUTH_*_PASSWORD dalam .env.auth; jangan push file tersebut.")


if __name__ == "__main__":
    main()
