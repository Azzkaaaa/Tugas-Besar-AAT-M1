"""BMKG-only environment configuration; no dependency on another service."""

from dataclasses import dataclass
import os
from pathlib import Path


class ConfigurationError(ValueError):
    """A safe configuration diagnostic that never contains supplied values."""


def _integer(name: str, default: int, minimum: int, maximum: int | None = None) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be an integer") from exc
    if value < minimum or (maximum is not None and value > maximum):
        raise ConfigurationError(f"{name} is outside the supported range")
    return value


@dataclass(frozen=True)
class Config:
    api_key: str
    database_path: Path
    port: int
    seed_count: int
    event_interval_ms: int
    response_delay_ms: int

    @classmethod
    def from_environment(cls) -> "Config":
        api_key = os.environ.get("BMKG_API_KEY", "")
        if not api_key or api_key.upper().startswith("REPLACE_") or any(ord(character) < 33 or ord(character) > 126 for character in api_key):
            raise ConfigurationError("BMKG_API_KEY must contain a provisioned API key")
        dsn = os.environ.get("BMKG_STORE_DSN", "sqlite:///data/bmkg.sqlite3")
        if not dsn.startswith("sqlite:///"):
            raise ConfigurationError("BMKG_STORE_DSN must use sqlite:///")
        filename = dsn[len("sqlite:///"):]
        if not filename.strip() or ":memory:" in filename.lower() or filename.startswith("file:"):
            raise ConfigurationError("BMKG_STORE_DSN must identify a persistent database file")
        return cls(
            api_key=api_key,
            database_path=Path(filename).expanduser().resolve(),
            port=_integer("BMKG_HTTP_PORT", 8081, 1, 65535),
            seed_count=_integer("MOCK_SEED_COUNT", 20, 20),
            event_interval_ms=_integer("MOCK_EVENT_INTERVAL_MS", 10000, 1, 10000),
            response_delay_ms=_integer("BMKG_RESPONSE_DELAY_MS", 100, 50, 150),
        )
