"""Runtime configuration from DATAOPS_* environment variables.

Connection details are assembled per role so that every component connects with the
least-privileged role it needs. Passwords are kept out of reprs and log output.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


class Role(StrEnum):
    ADMIN = "admin"
    SRC_APP = "src_app"
    SRC_READER = "src_reader"
    WH_LOADER = "wh_loader"
    WH_TRANSFORM = "wh_transform"
    WH_READER = "wh_reader"
    REL_APP = "rel_app"
    REL_READER = "rel_reader"


# role -> (database env var, password env var)
_ROLE_TARGETS: dict[Role, tuple[str, str]] = {
    Role.SRC_APP: ("DATAOPS_SOURCE_DB", "DATAOPS_SRC_APP_PASSWORD"),
    Role.SRC_READER: ("DATAOPS_SOURCE_DB", "DATAOPS_SRC_READER_PASSWORD"),
    Role.WH_LOADER: ("DATAOPS_WAREHOUSE_DB", "DATAOPS_WH_LOADER_PASSWORD"),
    Role.WH_TRANSFORM: ("DATAOPS_WAREHOUSE_DB", "DATAOPS_WH_TRANSFORM_PASSWORD"),
    Role.WH_READER: ("DATAOPS_WAREHOUSE_DB", "DATAOPS_WH_READER_PASSWORD"),
    Role.REL_APP: ("DATAOPS_RELIABILITY_DB", "DATAOPS_REL_APP_PASSWORD"),
    Role.REL_READER: ("DATAOPS_RELIABILITY_DB", "DATAOPS_REL_READER_PASSWORD"),
}

_DB_DEFAULTS = {
    "DATAOPS_SOURCE_DB": "commerce_source",
    "DATAOPS_WAREHOUSE_DB": "warehouse",
    "DATAOPS_RELIABILITY_DB": "reliability",
}


class ConfigError(RuntimeError):
    pass


@dataclass(frozen=True)
class ConnectionSpec:
    host: str
    port: int
    dbname: str
    user: str
    password: str = field(repr=False)
    application_name: str = "dataops"

    def kwargs(self) -> dict[str, object]:
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
            "application_name": self.application_name,
            "connect_timeout": 10,
        }

    def safe_description(self) -> str:
        return f"{self.user}@{self.host}:{self.port}/{self.dbname}"


def load_dotenv(path: Path | None = None) -> None:
    """Populate os.environ from .env without overriding variables already set."""
    path = path or REPO_ROOT / ".env"
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        value = value.split(" #", 1)[0].strip().strip('"').strip("'")
        os.environ.setdefault(key.strip(), value)


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ConfigError(f"Environment variable {name} is not set (see .env.example)")
    if value == "CHANGE_ME":
        raise ConfigError(f"Environment variable {name} still has the CHANGE_ME placeholder")
    return value


def database_name(env_var: str) -> str:
    return os.environ.get(env_var) or _DB_DEFAULTS[env_var]


def connection_spec(role: Role, application_name: str = "dataops") -> ConnectionSpec:
    host = os.environ.get("DATAOPS_PG_HOST", "127.0.0.1")
    port = int(os.environ.get("DATAOPS_PG_PORT", "5432"))
    if role is Role.ADMIN:
        return ConnectionSpec(
            host,
            port,
            "postgres",
            os.environ.get("DATAOPS_PG_ADMIN_USER", "postgres"),
            _require("DATAOPS_PG_ADMIN_PASSWORD"),
            application_name,
        )
    db_var, pw_var = _ROLE_TARGETS[role]
    return ConnectionSpec(
        host, port, database_name(db_var), role.value, _require(pw_var), application_name
    )


_SECRET_PATTERNS = [
    re.compile(r"(password\s*[=:]\s*)([^\s,;'\"]+)", re.IGNORECASE),
    re.compile(r"(postgres(?:ql)?://[^:/\s]+:)([^@\s]+)(@)", re.IGNORECASE),
    re.compile(r"((?:api[_-]?key|token|secret)\s*[=:]\s*)([^\s,;'\"]+)", re.IGNORECASE),
    re.compile(r"(sk-[A-Za-z0-9_\-]{8})[A-Za-z0-9_\-]+"),
]


def redact(text: str) -> str:
    """Remove passwords, connection-string credentials and API keys from free text."""
    out = text
    for pat in _SECRET_PATTERNS:
        if pat.groups >= 3:
            out = pat.sub(lambda m: f"{m.group(1)}***{m.group(3)}", out)
        elif pat.groups == 2:
            out = pat.sub(lambda m: f"{m.group(1)}***", out)
        else:
            out = pat.sub(lambda m: f"{m.group(1)}***", out)
    for name, value in os.environ.items():
        if name.endswith("_PASSWORD") and value and len(value) >= 6:
            out = out.replace(value, "***")
    return out
