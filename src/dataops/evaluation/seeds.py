"""Seed templates: each writes the *observable evidence* of one failure situation into the
isolated reliability_eval database, the same tables the real collectors write. Ground truth
(expected cause, actions, abstention) lives only in evaluations/suite_v1.yaml and is never
written to the database or the runbook corpus.

All timestamps are relative to the evaluation's `now`; ids are deterministic per case.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any

NS = uuid.UUID("6f1c0f5e-4c1b-4b8e-9f5a-0d1a7e0c2b11")
RAW_TABLES = [
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "refunds",
    "inventory_movements",
    "shipments",
    "shipment_events",
    "warehouses",
    "product_versions",
]


class Seeder:
    def __init__(self, conn: Any, case_id: str, now: datetime) -> None:
        self.c, self.case, self.now, self.n = conn, case_id, now, 0

    def uid(self) -> str:
        self.n += 1
        return str(uuid.uuid5(NS, f"{self.case}:{self.n}"))

    def ago(self, minutes: float) -> datetime:
        return self.now - timedelta(minutes=minutes)

    def run(
        self,
        pipeline: str,
        status: str,
        minutes_ago: float,
        tasks: list[dict[str, Any]],
        run_id: str | None = None,
        duration_min: float = 4,
    ) -> str:
        rid = run_id or f"{pipeline}:{self.uid()}"
        ended = self.ago(minutes_ago)
        self.c.execute(
            "insert into reliability.pipeline_runs (run_id, pipeline_id, external_run_id, status,"
            " started_at, ended_at, duration_s, trigger, correlation_id) values"
            " (%s,%s,%s,%s,%s,%s,%s,'scheduled',%s)",
            (
                rid,
                pipeline,
                rid.split(":", 1)[1],
                status,
                ended - timedelta(minutes=duration_min),
                ended,
                duration_min * 60,
                rid,
            ),
        )
        for t in tasks:
            self.c.execute(
                "insert into reliability.task_runs (task_run_id, run_id, task_id, dataset_id,"
                " status,"
                " try_number, started_at, ended_at, rows_affected, message) values"
                " (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    f"{rid}/{t['task_id']}",
                    rid,
                    t["task_id"],
                    t.get("dataset_id"),
                    t["status"],
                    t.get("try", 1),
                    ended - timedelta(minutes=duration_min),
                    ended,
                    t.get("rows"),
                    t.get("message"),
                ),
            )
        return rid

    def quality(
        self,
        run_id: str,
        check_id: str,
        ctype: str,
        ds: str,
        status: str,
        failures: int | None,
        message: str | None,
        minutes_ago: float,
        column: str | None = None,
    ) -> None:
        self.c.execute(
            "insert into reliability.quality_check_results (result_id, check_id, check_type,"
            " dataset_id, column_name, run_id, status, failures, message, observed_at)"
            " values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                f"{run_id}/{check_id}",
                check_id,
                ctype,
                ds,
                column,
                run_id,
                status,
                failures,
                message,
                self.ago(minutes_ago),
            ),
        )

    def fresh(
        self, ds: str, measure: str, lag_h: float, thr_h: float, minutes_ago: float = 5
    ) -> None:
        st = "stale" if lag_h > thr_h else "fresh"
        self.c.execute(
            "insert into reliability.freshness_observations (observation_id, dataset_id, measure,"
            " lag_seconds, threshold_seconds, status, source_high_water, target_high_water,"
            " query_fingerprint, observed_at) values (%s,%s,%s,%s,%s,%s,%s,%s,'eval',%s)",
            (
                self.uid(),
                ds,
                measure,
                round(lag_h * 3600),
                round(thr_h * 3600),
                st,
                self.ago(minutes_ago + 10) if measure == "data_lag" else None,
                self.ago(minutes_ago + 10 + lag_h * 60) if measure == "data_lag" else None,
                self.ago(minutes_ago),
            ),
        )

    def schema(self, ds: str, compat: str, diffs: list[dict[str, Any]], minutes_ago: float) -> None:
        self.c.execute(
            "insert into reliability.schema_observations (observation_id, dataset_id, columns,"
            " schema_checksum, previous_checksum, changed, contract_id, contract_version,"
            " compatibility, differences, observed_at)"
            " values (%s,%s,'[]',%s,%s,true,'shop',1,%s,%s,%s)",
            (
                self.uid(),
                ds,
                f"sha-{self.n}",
                f"sha-{self.n - 1}",
                compat,
                json.dumps(diffs),
                self.ago(minutes_ago),
            ),
        )

    def recon(
        self,
        check_id: str,
        ds: str,
        status: str,
        minutes_ago: float,
        mismatches: list[dict[str, Any]],
        left: dict[str, Any],
        right: dict[str, Any],
        compared: int = 30,
        error: str | None = None,
    ) -> None:
        self.c.execute(
            "insert into reliability.reconciliation_results (reconciliation_id, check_id,"
            " check_version, dataset_id, comparison, scope, status, compared_rows, mismatched_rows,"
            " left_totals, right_totals, mismatches, query_fingerprint, correlation_id,"
            " observed_at,"
            " error_message) values (%s,%s,'1',%s,%s,%s,%s,%s,%s,%s,%s,%s,'eval','eval',%s,%s)",
            (
                self.uid(),
                check_id,
                ds,
                check_id.split(".")[-1],
                json.dumps({"date_basis": "UTC"}),
                status,
                compared,
                len(mismatches),
                json.dumps(left),
                json.dumps(right),
                json.dumps(mismatches),
                self.ago(minutes_ago),
                error,
            ),
        )

    def stream(
        self, kind: str, ds: str | None, metrics: dict[str, Any], minutes_ago: float
    ) -> None:
        self.c.execute(
            "insert into reliability.stream_observations (observation_id, kind, dataset_id,"
            " metrics,"
            " observed_at) values (%s,%s,%s,%s,%s)",
            (f"{kind}:{ds}:{self.uid()}", kind, ds, json.dumps(metrics), self.ago(minutes_ago)),
        )

    def all_raw_fresh(self, except_: set[str] = frozenset()) -> None:  # type: ignore[assignment]
        for t in RAW_TABLES:
            if t not in except_:
                self.fresh(f"warehouse.raw.{t}", "data_lag", 2.0, 26)


def _mm(dates: list[str], metric: str, left: list[int], right: list[int]) -> list[dict[str, Any]]:
    return [
        {
            "key": d,
            "metrics": {metric: {"left": a, "right": b, "delta": b - a}},
            "missing_left": False,
            "missing_right": False,
        }
        for d, a, b in zip(dates, left, right, strict=True)
    ]


# --------------------------------------------------------------------------- templates
def schema_break(s: Seeder, p: dict[str, Any]) -> None:
    t = p["table"]
    s.schema(f"commerce_source.shop.{t}", "breaking", p["diffs"], 140)
    rid = s.run(
        "dbt",
        "failed",
        120,
        [
            {"task_id": f"model.dataops.{m}", "dataset_id": ds, "status": "skipped"}
            for m, ds in ((x.rsplit(".", 1)[1], x) for x in p["skipped"])
        ],
    )
    for check, col in p["errored_tests"]:
        s.quality(
            rid,
            f"test.dataops.{check}",
            "not_null" if col else "singular",
            f"warehouse.staging.stg_{t}",
            "error",
            None,
            p.get(
                "test_message",
                f"Database Error in test {check}\n  invalid input syntax for"
                f' type bigint: "{p.get("bad_value", "12.99")}"',
            ),
            120,
            col,
        )
    if p.get("recon"):
        dates = ["2026-03-19", "2026-03-20", "2026-03-21"]
        s.recon(
            "revenue.daily.source_vs_mart",
            "warehouse.marts.mart_daily_revenue",
            "fail",
            100,
            _mm(dates, p["recon"], [17604, 14635, 18927], [1760497, 1463578, 1892737]),
            {"captured_cents": 555677},
            {"captured_cents": 55569210},
        )
    s.all_raw_fresh()


def batch_stall(s: Seeder, p: dict[str, Any]) -> None:
    s.run(
        "batch_ingestion",
        "failed",
        50,
        [
            {
                "task_id": f"ingest_{t}",
                "dataset_id": f"warehouse.raw.{t}",
                "status": "failed",
                "try": 3,
                "message": p["message"],
            }
            for t in p["tables"]
        ]
        + [
            {
                "task_id": f"ingest_{t}",
                "dataset_id": f"warehouse.raw.{t}",
                "status": "success",
                "rows": 120,
            }
            for t in p.get("ok_tables", [])
        ],
    )
    for t in p["tables"]:
        s.fresh(f"warehouse.raw.{t}", "data_lag", p.get("lag_h", 31.5), 26)
    s.all_raw_fresh(set(p["tables"]))
    if p.get("recon"):
        s.recon(
            "revenue.daily.source_vs_mart",
            "warehouse.marts.mart_daily_revenue",
            "fail",
            20,
            _mm(["2026-03-21"], "captured_cents", [0], [1892737]),
            {"captured_cents": 53676473},
            {"captured_cents": 55569210},
        )


def consumer_stopped(s: Seeder, p: dict[str, Any]) -> None:
    for t, lag in zip(p["tables"], p["lags"], strict=True):
        s.stream(
            "kafka_lag",
            f"kafka.cdc.shop.{t}",
            {
                "topic": f"dataops.shop.{t}",
                "lag_messages": lag,
                "oldest_unprocessed_age_s": p["age_s"],
                "consumer": "bronze",
            },
            3,
        )
        s.fresh(f"warehouse.cdc_pub.{t}", "load_age", p["age_s"] / 3600 + 0.2, 1)
    s.stream(
        "spark_progress",
        "lakehouse.bronze.orders",
        {
            "query": "bronze",
            "batchId": 412,
            "numInputRows": 0,
            "last_progress_at": s.ago(p["age_s"] / 60).isoformat(),
        },
        p["age_s"] / 60,
    )
    s.stream(
        "connector_status",
        None,
        {"connector": "RUNNING", "tasks": [{"id": 0, "state": "RUNNING"}]},
        3,
    )


def connector_failed(s: Seeder, p: dict[str, Any]) -> None:
    s.stream(
        "connector_status",
        None,
        {"connector": "RUNNING", "tasks": [{"id": 0, "state": "FAILED", "trace": p["trace"]}]},
        4,
    )
    for t in p["tables"]:
        s.stream(
            "kafka_lag",
            f"kafka.cdc.shop.{t}",
            {
                "topic": f"dataops.shop.{t}",
                "lag_messages": 0,
                "oldest_unprocessed_age_s": None,
                "consumer": "bronze",
            },
            4,
        )
        s.fresh(f"warehouse.cdc_pub.{t}", "load_age", p.get("age_h", 2.5), 1)
    s.run(
        "silver_publication",
        "success",
        25,
        [{"task_id": "publish_silver", "dataset_id": None, "status": "success", "rows": 0}],
    )


def marts_behind(s: Seeder, p: dict[str, Any]) -> None:
    s.run(
        "dbt",
        "success",
        p.get("dbt_minutes_ago", 360),
        [
            {
                "task_id": "model.dataops.fct_payment_transaction",
                "dataset_id": "warehouse.marts.fct_payment_transaction",
                "status": "success",
                "rows": 412,
            },
            {
                "task_id": "model.dataops.mart_daily_revenue",
                "dataset_id": "warehouse.marts.mart_daily_revenue",
                "status": "success",
                "rows": 30,
            },
        ],
    )
    s.run(
        "batch_ingestion",
        "success",
        30,
        [
            {
                "task_id": "ingest_refunds",
                "dataset_id": "warehouse.raw.refunds",
                "status": "success",
                "rows": 57,
            }
        ],
    )
    s.recon(
        p["check"],
        "warehouse.marts.mart_daily_revenue",
        "fail",
        15,
        _mm(p["dates"], "refunded_cents", p["left"], p["right"]),
        {"refunded_cents": sum(p["left"])},
        {"refunded_cents": sum(p["right"])},
    )
    s.all_raw_fresh()


def duplicates(s: Seeder, p: dict[str, Any]) -> None:
    t = p["table"]
    rid = s.run(
        "dbt",
        "failed",
        90,
        [
            {
                "task_id": f"model.dataops.{m.rsplit('.', 1)[1]}",
                "dataset_id": m,
                "status": "skipped",
            }
            for m in p["skipped"]
        ],
    )
    s.quality(
        rid,
        f"test.dataops.unique_stg_{t}_{p['key']}",
        "unique",
        f"warehouse.staging.stg_{t}",
        "fail",
        p["failures"],
        p.get("message", f"Got {p['failures']} results, configured to fail if != 0"),
        90,
        p["key"],
    )
    if p.get("recon"):
        s.recon(
            "revenue.daily.source_vs_mart",
            "warehouse.marts.mart_daily_revenue",
            "fail",
            70,
            _mm(
                ["2026-03-20", "2026-03-21"],
                "booked_revenue_cents",
                [1902210, 2011345],
                [1902210, 2048790],
            ),
            {"booked_revenue_cents": 3913555},
            {"booked_revenue_cents": 3951000},
        )
    s.all_raw_fresh()


def orphans(s: Seeder, p: dict[str, Any]) -> None:
    rid = s.run(
        "dbt",
        "failed",
        80,
        [
            {
                "task_id": f"model.dataops.{m.rsplit('.', 1)[1]}",
                "dataset_id": m,
                "status": "skipped",
            }
            for m in p["skipped"]
        ],
    )
    s.quality(
        rid,
        f"test.dataops.relationships_stg_{p['child']}_{p['column']}",
        "relationships",
        f"warehouse.staging.stg_{p['child']}",
        "fail",
        p["failures"],
        f"Got {p['failures']} results, configured to fail if != 0",
        80,
        p["column"],
    )
    s.all_raw_fresh()


def quarantine(s: Seeder, p: dict[str, Any]) -> None:
    s.run(
        "spark:silver",
        "success",
        30,
        [
            {
                "task_id": "silver_payments",
                "dataset_id": "lakehouse.silver.payments",
                "status": "success",
                "rows": 412,
            }
        ],
    )
    s.quality(
        "spark:silver:batch-881",
        "contract:shop.v1:payments",
        "quarantine",
        "lakehouse.silver.payments",
        "fail",
        p["failures"],
        p["message"],
        30,
        "amount_cents",
    )
    if p["source_still_breaking"]:
        s.schema(
            "commerce_source.shop.payments",
            "breaking",
            [
                {
                    "change": "type_changed",
                    "column": "amount_cents",
                    "breaking": True,
                    "expected": "int",
                    "observed": "numeric",
                }
            ],
            45,
        )
    else:
        s.schema(
            "commerce_source.shop.payments",
            "breaking",
            [
                {
                    "change": "type_changed",
                    "column": "amount_cents",
                    "breaking": True,
                    "expected": "int",
                    "observed": "numeric",
                }
            ],
            60 * 30,
        )
        s.schema("commerce_source.shop.payments", "matches_contract", [], 20)


def lone_stale(s: Seeder, p: dict[str, Any]) -> None:
    t = p["table"]
    s.run(
        "batch_ingestion",
        "success",
        40,
        [
            {
                "task_id": f"ingest_{x}",
                "dataset_id": f"warehouse.raw.{x}",
                "status": "success",
                "rows": 0 if x == t else 150,
            }
            for x in RAW_TABLES
        ],
    )
    s.fresh(f"warehouse.raw.{t}", "data_lag", p["lag_h"], 26)
    s.all_raw_fresh({t})


def publication_failed(s: Seeder, p: dict[str, Any]) -> None:
    s.run(
        "silver_publication",
        "failed",
        35,
        [
            {
                "task_id": "publish_silver",
                "dataset_id": f"warehouse.cdc_pub.{p['tables'][0]}",
                "status": "failed",
                "try": 3,
                "message": p["message"],
            }
        ],
    )
    for t in p["tables"]:
        s.fresh(f"warehouse.cdc_pub.{t}", "load_age", 2.2, 1)
        s.stream(
            "kafka_lag",
            f"kafka.cdc.shop.{t}",
            {
                "topic": f"dataops.shop.{t}",
                "lag_messages": 0,
                "oldest_unprocessed_age_s": None,
                "consumer": "bronze",
            },
            4,
        )
    s.stream(
        "connector_status",
        None,
        {"connector": "RUNNING", "tasks": [{"id": 0, "state": "RUNNING"}]},
        4,
    )


def permission_denied(s: Seeder, p: dict[str, Any]) -> None:
    s.run(
        "dbt",
        "failed",
        60,
        [
            {
                "task_id": "model.dataops.stg_payments",
                "dataset_id": "warehouse.staging.stg_payments",
                "status": "failed",
                "message": p["message"],
            }
        ]
        + [
            {
                "task_id": f"model.dataops.{m.rsplit('.', 1)[1]}",
                "dataset_id": m,
                "status": "skipped",
            }
            for m in (
                "warehouse.marts.fct_payment_transaction",
                "warehouse.marts.mart_daily_revenue",
            )
        ],
    )
    s.all_raw_fresh()


def stale_evidence(s: Seeder, p: dict[str, Any]) -> None:
    s.schema(
        "commerce_source.shop.payments",
        "breaking",
        [
            {
                "change": "type_changed",
                "column": "amount_cents",
                "breaking": True,
                "expected": "int",
                "observed": "numeric",
            }
        ],
        20 * 60,
    )
    s.schema("commerce_source.shop.payments", "matches_contract", [], 3 * 60)
    s.run(
        "dbt",
        "success",
        60,
        [
            {
                "task_id": "model.dataops.mart_daily_revenue",
                "dataset_id": "warehouse.marts.mart_daily_revenue",
                "status": "success",
                "rows": 30,
            }
        ],
    )
    s.recon(
        "revenue.daily.source_vs_mart",
        "warehouse.marts.mart_daily_revenue",
        "fail",
        18 * 60,
        _mm(["2026-03-20", "2026-03-21"], "captured_cents", [14635, 18927], [1463578, 1892737]),
        {"captured_cents": 33562},
        {"captured_cents": 3356315},
    )
    s.all_raw_fresh()


TEMPLATES: dict[str, Callable[[Seeder, dict[str, Any]], None]] = {
    f.__name__: f
    for f in (
        schema_break,
        batch_stall,
        consumer_stopped,
        connector_failed,
        marts_behind,
        duplicates,
        orphans,
        quarantine,
        lone_stale,
        publication_failed,
        permission_denied,
        stale_evidence,
    )
}
