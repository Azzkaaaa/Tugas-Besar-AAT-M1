"""PVMBG-owned startup configuration; no dotenv or third-party dependency."""

from dataclasses import dataclass
import os
from pathlib import Path
import re


def _integer(name: str, default: int, minimum: int, maximum: int | None = None) -> int:
    raw = os.environ.get(name, str(default))
    if not re.fullmatch(r"[0-9]+", raw):
        raise ValueError(f"{name} must be an integer")
    value = int(raw)
    if value < minimum or (maximum is not None and value > maximum):
        raise ValueError(f"{name} is outside its supported range")
    return value


@dataclass(frozen=True)
class Config:
    bearer_token: str
    database: Path
    port: int
    seed_count: int
    interval_ms: int
    delay_min_ms: int
    delay_max_ms: int

    @classmethod
    def from_env(cls) -> "Config":
        token = os.environ.get("PVMBG_BEARER_TOKEN", "")
        if not token or token.startswith("REPLACE_") or any(c.isspace() for c in token):
            raise ValueError("PVMBG_BEARER_TOKEN must be a nonblank, provisioned opaque token")
        if any(ord(c) < 32 or ord(c) == 127 for c in token):
            raise ValueError("PVMBG_BEARER_TOKEN cannot contain control characters")
        dsn = os.environ.get("PVMBG_STORE_DSN", "sqlite:///data/pvmbg.sqlite3")
        if not dsn.startswith("sqlite:///"):
            raise ValueError("PVMBG_STORE_DSN must use sqlite:/// with a persistent file")
        filename = dsn[len("sqlite:///"):]
        if (not filename.strip() or filename == ":memory:" or filename.startswith("file:")
                or "?" in filename or "#" in filename):
            raise ValueError("PVMBG_STORE_DSN must name a persistent SQLite file")
        delay_min = _integer("PVMBG_RESPONSE_DELAY_MIN_MS", 500, 0)
        delay_max = _integer("PVMBG_RESPONSE_DELAY_MAX_MS", 3000, 0)
        if delay_min > delay_max:
            raise ValueError("PVMBG_RESPONSE_DELAY_MIN_MS cannot exceed PVMBG_RESPONSE_DELAY_MAX_MS")
        return cls(
            bearer_token=token,
            database=Path(filename).resolve(),
            port=_integer("PVMBG_HTTP_PORT", 8082, 1, 65535),
            seed_count=_integer("MOCK_SEED_COUNT", 20, 20),
            interval_ms=_integer("MOCK_EVENT_INTERVAL_MS", 10000, 1, 10000),
            delay_min_ms=delay_min,
            delay_max_ms=delay_max,
        )
