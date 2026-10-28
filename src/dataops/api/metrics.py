"""Prometheus metrics computed at scrape time from the reliability views (read-only role).

Nothing is cached in the process: every scrape reflects the database, so the API can run
as several workers and restarts lose nothing.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from prometheus_client.core import GaugeMetricFamily
from prometheus_client.registry import Collector

from dataops.config import Role
from dataops.db import connect


def _rows(conn: Any, sql: str) -> list[tuple[Any, ...]]:
    return conn.execute(sql).fetchall()


class ReliabilityCollector(Collector):
    def collect(self) -> Iterator[GaugeMetricFamily]:
        up = GaugeMetricFamily("dataops_metadata_up", "1 if the reliability database answered")
        try:
            conn = connect(
                Role.REL_READER,
                application_name="dataops-metrics",
                statement_timeout_s=10,
                attempts=1,
            )
        except Exception:  # noqa: BLE001 - reported as a metric
            up.add_metric([], 0)
            yield up
            return
        with conn:
            up.add_metric([], 1)
            yield up
            g = GaugeMetricFamily(
                "dataops_active_incidents",
                "Open/investigating/mitigating incidents",
                labels=["severity"],
            )
            counts = dict(
                _rows(
                    conn, "select severity, count(*) from reliability.v_active_incidents group by 1"
                )
            )
            for sev in ("sev1", "sev2", "sev3"):
                g.add_metric([sev], counts.get(sev, 0))
            yield g
            g = GaugeMetricFamily(
                "dataops_incidents_by_status", "Incidents by status", labels=["status"]
            )
            for st, n in _rows(
                conn, "select status, count(*) from reliability.incidents group by 1"
            ):
                g.add_metric([st], n)
            yield g
            lag = GaugeMetricFamily(
                "dataops_freshness_lag_seconds",
                "Latest freshness lag",
                labels=["dataset", "measure"],
            )
            stale = GaugeMetricFamily(
                "dataops_dataset_stale",
                "1 if the latest observation is stale",
                labels=["dataset", "measure"],
            )
            for ds, measure, value, status in _rows(
                conn,
                "select dataset_id, measure, lag_seconds, status from"
                " reliability.v_freshness_current",
            ):
                if value is not None:
                    lag.add_metric([ds, measure], float(value))
                stale.add_metric([ds, measure], 1 if status == "stale" else 0)
            yield lag
            yield stale
            rec = GaugeMetricFamily(
                "dataops_reconciliation_passing", "1 if the latest result passed", labels=["check"]
            )
            mism = GaugeMetricFamily(
                "dataops_reconciliation_mismatched_keys", "Mismatched keys", labels=["check"]
            )
            for check, status, mm in _rows(
                conn,
                "select check_id, status, mismatched_rows"
                " from reliability.v_reconciliation_current",
            ):
                rec.add_metric([check], 1 if status == "pass" else 0)
                mism.add_metric([check], mm)
            yield rec
            yield mism
            last = GaugeMetricFamily(
                "dataops_pipeline_last_success_timestamp_seconds",
                "Unix time of the last successful run",
                labels=["pipeline"],
            )
            failed = GaugeMetricFamily(
                "dataops_pipeline_failed_runs_24h", "Failed runs in 24 h", labels=["pipeline"]
            )
            for pid, last_ok, f24 in _rows(
                conn,
                "select pipeline_id, extract(epoch from last_success_at), failed_24h"
                " from reliability.v_pipeline_health",
            ):
                if last_ok is not None:
                    last.add_metric([pid], float(last_ok))
                failed.add_metric([pid], f24)
            yield last
            yield failed
            cl = GaugeMetricFamily(
                "dataops_consumer_lag_messages", "Unprocessed CDC messages", labels=["topic"]
            )
            for topic, n in _rows(
                conn, "select topic, lag_messages from reliability.v_consumer_lag_current"
            ):
                if topic and n is not None:
                    cl.add_metric([topic], n)
            yield cl
            q = GaugeMetricFamily(
                "dataops_quality_failures_7d",
                "Failing or erroring checks in 7 days",
                labels=["dataset"],
            )
            for ds, n in _rows(
                conn,
                "select coalesce(dataset_id,'unknown'), count(*) from"
                " reliability.v_quality_failures_recent group by 1",
            ):
                q.add_metric([ds], n)
            yield q
            inv = GaugeMetricFamily(
                "dataops_investigations_total",
                "Investigations by mode and status",
                labels=["mode", "status"],
            )
            for mode, st, n in _rows(
                conn,
                "select mode, status, count(*) from reliability.investigation_runs group by 1, 2",
            ):
                inv.add_metric([mode, st], n)
            yield inv
            tok = GaugeMetricFamily(
                "dataops_investigation_tokens_total",
                "LLM tokens (model mode)",
                labels=["direction"],
            )
            tin, tout = _rows(
                conn,
                "select coalesce(sum(input_tokens),0), coalesce(sum(output_tokens),0)"
                " from reliability.investigation_runs where mode='model'",
            )[0]
            tok.add_metric(["input"], tin)
            tok.add_metric(["output"], tout)
            yield tok
            ra = GaugeMetricFamily(
                "dataops_recovery_actions_total",
                "Recovery actions by type and status",
                labels=["action", "status"],
            )
            for a, st, n in _rows(
                conn,
                "select action_type, status, count(*) from"
                " reliability.recovery_actions group by 1, 2",
            ):
                ra.add_metric([a, st], n)
            yield ra
            ttv = GaugeMetricFamily(
                "dataops_incident_seconds_to_verified",
                "Detection to verified recovery, per resolved incident (last 50)",
                labels=["incident"],
            )
            for iid, secs in _rows(
                conn,
                "select incident_id::text, extract(epoch from verified_at - detected_at)"
                " from reliability.incidents where verified_at is not null"
                " order by verified_at desc limit 50",
            ):
                ttv.add_metric([iid], float(secs))
            yield ttv
