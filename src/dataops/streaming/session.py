"""Spark session and lakehouse layout for the full profile.

Spark 4.2.0 + Delta Lake 4.4.1 (Scala 2.13), Kafka source spark-sql-kafka-0-10 4.2.0.
Runs in local mode inside the developer's Python environment; jars are resolved from Maven
Central on first use and cached under ~/.ivy2 (see docs/full_profile.md).
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any

from dataops.config import REPO_ROOT

SPARK_VERSION = "4.2.0"
KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{SPARK_VERSION}"


def lake_root() -> Path:
    return Path(os.environ.get("DATAOPS_LAKE_DIR", str(REPO_ROOT / "var" / "lake")))


def path(*parts: str) -> str:
    return str(lake_root().joinpath(*parts))


BRONZE = ("bronze", "cdc_events")
QUARANTINE = ("silver", "_quarantine")
BATCH_LOG = ("silver", "_batch_log")
HOURLY = ("silver", "shipment_events_hourly")


def checkpoint(name: str) -> str:
    return path("_checkpoints", name)


def get_spark(app: str = "dataops", *, cores: int = 2, memory: str = "1g") -> Any:
    # Collected timestamps become naive datetimes in the process time zone: pin it to UTC.
    os.environ["TZ"] = "UTC"
    time.tzset()
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master(f"local[{cores}]")
        .appName(app)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.driver.memory", memory)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")  # demo volumes: avoid 200 tiny files
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        )
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
    )
    spark = configure_spark_with_delta_pip(builder, extra_packages=[KAFKA_PACKAGE]).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
