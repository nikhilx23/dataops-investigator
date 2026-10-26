"""Volume profiles for the synthetic data generator."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime


@dataclass(frozen=True)
class VolumeProfile:
    name: str
    days: int
    customers: int
    products: int
    warehouses: int
    orders_per_day: int


PROFILES: dict[str, VolumeProfile] = {
    # Unit-test fixtures: a few hundred rows, generates in milliseconds.
    "tiny": VolumeProfile(
        "tiny", days=4, customers=40, products=12, warehouses=2, orders_per_day=15
    ),
    # Default demo dataset: comfortably runs on a laptop.
    "small": VolumeProfile(
        "small", days=30, customers=600, products=60, warehouses=3, orders_per_day=60
    ),
    # Optional heavier workloads for benchmarks; never used by default startup.
    "medium": VolumeProfile(
        "medium", days=90, customers=5_000, products=200, warehouses=4, orders_per_day=400
    ),
    "large": VolumeProfile(
        "large", days=180, customers=30_000, products=800, warehouses=6, orders_per_day=2_000
    ),
}

DEFAULT_PROFILE = "small"
DEFAULT_SEED = 42
# Fixed simulation start keeps generated data independent of the wall clock.
DEFAULT_START = datetime(2026, 3, 2, tzinfo=UTC)
