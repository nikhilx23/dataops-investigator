"""Load versioned data contracts (contracts/**/*.yaml) and check them against the source."""

from __future__ import annotations

from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import Any

import yaml

from dataops.config import REPO_ROOT

CONTRACT_DIR = REPO_ROOT / "contracts"
TYPES = {"int", "text", "bool", "timestamp", "char2", "char3"}


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    required: bool
    allowed: tuple[str, ...] | None = None
    min: int | None = None


@dataclass(frozen=True)
class TableContract:
    contract: str
    version: int
    table: str
    key: str
    columns: tuple[Column, ...]
    freshness_hours: int
    event_time: str | None = None

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]


def _parse(doc: dict[str, Any]) -> dict[str, TableContract]:
    out = {}
    for table, spec in doc["tables"].items():
        cols = []
        for name, c in spec["columns"].items():
            if c["type"] not in TYPES:
                raise ValueError(f"{table}.{name}: unknown type {c['type']!r}")
            cols.append(
                Column(
                    name,
                    c["type"],
                    bool(c.get("required", False)),
                    tuple(c["allowed"]) if c.get("allowed") else None,
                    c.get("min"),
                )
            )
        if spec["key"] not in {c.name for c in cols}:
            raise ValueError(f"{table}: key {spec['key']!r} is not a column")
        out[table] = TableContract(
            doc["contract"],
            int(doc["version"]),
            table,
            spec["key"],
            tuple(cols),
            int(spec["freshness_hours"]),
            spec.get("event_time"),
        )
    return out


@cache
def load(
    name: str = "shop", version: int = 1, root: Path = CONTRACT_DIR
) -> dict[str, TableContract]:
    path = root / "source" / f"{name}.v{version}.yaml"
    return _parse(yaml.safe_load(path.read_text(encoding="utf-8")))
