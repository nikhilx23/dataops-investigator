"""Reliability collectors: normalize execution and validation evidence into the reliability
metadata model.

Rules every collector follows:
- Supported interfaces only: dbt artifacts (manifest.json, run_results.json), the loader's
  own manifest tables, Airflow's stable REST API v2, Spark's StreamingQueryProgress (written
  by the jobs), Kafka's admin/consumer API, Kafka Connect's REST API, PostgreSQL catalogs.
- Idempotent: records are keyed by natural ids (external run ids, artifact checksums,
  offsets) and upserted, so collecting twice changes nothing.
- Resilient: a collector that cannot reach its system records 'unavailable' in
  collector_runs and returns; it never stops the others.
- No secrets: messages are redacted and truncated before storage.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dataops.config import REPO_ROOT, Role, redact
from dataops.contracts import load as load_contracts
from dataops.db import connect

log = logging.getLogger(__name__)

PIPELINES = {
    "batch_ingestion": ("loader", "critical", "Incremental batch extraction into warehouse raw"),
    "dbt": ("dbt", "critical", "dbt build of staging, dimensions, facts and marts"),
    "silver_publication": ("loader", "critical", "Silver snapshot publication into cdc_pub"),
    "spark:bronze": ("spark", "standard", "Kafka CDC topics -> Bronze Delta"),
    "spark:silver": ("spark", "critical", "Bronze -> validated Silver Delta tables"),
    "spark:events_hourly": ("spark", "standard", "Event-time hourly shipment counts"),
    "debezium": ("debezium", "critical", "PostgreSQL change data capture"),
}
DBT_STATUS = {
    "success": "success",
    "error": "failed",
    "skipped": "skipped",
    "pass": "success",
    "fail": "failed",
    "warn": "success",
    "runtime error": "failed",
}
TEST_STATUS = {
    "pass": "pass",
    "fail": "fail",
    "warn": "warn",
    "error": "error",
    "skipped": "skipped",
}
AIRFLOW_RUN = {"queued": "queued", "running": "running", "success": "success", "failed": "failed"}
AIRFLOW_TASK = {
    "success": "success",
    "failed": "failed",
    "skipped": "skipped",
    "upstream_failed": "upstream_failed",
    "running": "running",
    "queued": "queued",
    "scheduled": "queued",
    "up_for_retry": "running",
    "deferred": "running",
}


def _msg(text: str | None, limit: int = 1500) -> str | None:
    return redact(text)[:limit] if text else None


def _sha_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _record_collector(
    name: str, status: str, records: int, message: str | None, started: datetime
) -> None:
    with connect(Role.REL_APP, application_name="dataops-collector") as conn, conn.transaction():
        conn.execute(
            "insert into reliability.collector_runs (collector_run_id, collector, status,"
            " records, message, started_at) values (%s,%s,%s,%s,%s,%s)",
            (str(uuid.uuid4()), name, status, records, _msg(message, 500), started),
        )


def ensure_pipelines(extra: dict[str, tuple[str, str, str]] | None = None) -> None:
    with connect(Role.REL_APP, application_name="dataops-collector") as conn, conn.transaction():
        for pid, (system, crit, desc) in {**PIPELINES, **(extra or {})}.items():
            conn.execute(
                "insert into reliability.pipeline_definitions (pipeline_id, system, criticality,"
                " description) values (%s,%s,%s,%s) on conflict (pipeline_id) do update set"
                " system=excluded.system, criticality=excluded.criticality,"
                " description=excluded.description",
                (pid, system, crit, desc),
            )


def _upsert_run(conn: Any, run: dict[str, Any]) -> None:
    conn.execute(
        "insert into reliability.pipeline_runs (run_id, pipeline_id, external_run_id, status,"
        " started_at, ended_at, duration_s, trigger, correlation_id, artifact_checksum, details)"
        " values (%(run_id)s,%(pipeline_id)s,%(external_run_id)s,%(status)s,%(started_at)s,"
        " %(ended_at)s,%(duration_s)s,%(trigger)s,%(correlation_id)s,%(artifact_checksum)s,"
        " %(details)s) on conflict (run_id) do update set status=excluded.status,"
        " started_at=excluded.started_at, ended_at=excluded.ended_at,"
        " duration_s=excluded.duration_s, details=excluded.details,"
        " artifact_checksum=excluded.artifact_checksum, collected_at=clock_timestamp()",
        {
            "trigger": None,
            "correlation_id": None,
            "artifact_checksum": None,
            **run,
            "details": json.dumps(run.get("details", {}), default=str),
        },
    )


def _upsert_task(conn: Any, t: dict[str, Any]) -> None:
    conn.execute(
        "insert into reliability.task_runs (task_run_id, run_id, task_id, dataset_id, status,"
        " try_number, started_at, ended_at, duration_s, rows_affected, message, details)"
        " values (%(task_run_id)s,%(run_id)s,%(task_id)s,%(dataset_id)s,%(status)s,"
        " %(try_number)s,%(started_at)s,%(ended_at)s,%(duration_s)s,%(rows_affected)s,"
        " %(message)s,%(details)s) on conflict (task_run_id) do update set"
        " status=excluded.status, ended_at=excluded.ended_at, duration_s=excluded.duration_s,"
        " rows_affected=excluded.rows_affected, message=excluded.message,"
        " details=excluded.details",
        {
            "dataset_id": None,
            "try_number": 1,
            "started_at": None,
            "ended_at": None,
            "duration_s": None,
            "rows_affected": None,
            "message": None,
            **t,
            "details": json.dumps(t.get("details", {}), default=str),
        },
    )


def _upsert_quality(conn: Any, q: dict[str, Any]) -> None:
    conn.execute(
        "insert into reliability.quality_check_results (result_id, check_id, check_type,"
        " dataset_id, column_name, run_id, status, failures, message, query_fingerprint,"
        " observed_at, details) values (%(result_id)s,%(check_id)s,%(check_type)s,"
        " %(dataset_id)s,%(column_name)s,%(run_id)s,%(status)s,%(failures)s,%(message)s,"
        " %(query_fingerprint)s,%(observed_at)s,%(details)s) on conflict (result_id) do update"
        " set status=excluded.status, failures=excluded.failures, message=excluded.message",
        {
            "column_name": None,
            "run_id": None,
            "failures": None,
            "message": None,
            "query_fingerprint": None,
            **q,
            "details": json.dumps(q.get("details", {})),
        },
    )


def _scalar(conn: Any, sql: str, params: tuple[Any, ...] = ()) -> Any:
    row = conn.execute(sql, params).fetchone()
    return row[0] if row else None


def _ts(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


# ---------------------------------------------------------------------------- dbt
def dbt_target_dirs() -> list[Path]:
    root = Path(os.environ.get("DATAOPS_DBT_TARGET_ROOT", str(REPO_ROOT / "dbt" / "target")))
    return sorted({p.parent for p in root.glob("**/run_results.json")})


def collect_dbt(target_dirs: list[Path] | None = None) -> int:
    """dbt run_results.json -> pipeline/task runs and quality results; manifest -> lineage."""
    from dataops.reliability import lineage

    count = 0
    latest_manifest: Path | None = None
    for target in target_dirs or dbt_target_dirs():
        rr_path, mf_path = target / "run_results.json", target / "manifest.json"
        if not (rr_path.exists() and mf_path.exists()):
            continue
        rr = json.loads(rr_path.read_text(encoding="utf-8"))
        manifest = json.loads(mf_path.read_text(encoding="utf-8"))
        nodes = {**manifest["nodes"], **manifest.get("sources", {})}
        invocation = rr["metadata"]["invocation_id"]
        run_id = f"dbt:{invocation}"
        checksum = _sha_file(rr_path)
        results = rr["results"]
        failed = [r for r in results if r["status"] in ("error", "fail", "runtime error")]
        started = min(
            (
                _ts(t["started_at"]) or datetime.max.replace(tzinfo=UTC)
                for r in results
                for t in r.get("timing", [])
                if t.get("started_at")
            ),
            default=_ts(rr["metadata"]["generated_at"]),
        )
        ended = _ts(rr["metadata"]["generated_at"])
        args = rr.get("args", {})
        with (
            connect(Role.REL_APP, application_name="dataops-collector") as conn,
            conn.transaction(),
        ):
            _upsert_run(
                conn,
                {
                    "run_id": run_id,
                    "pipeline_id": "dbt",
                    "external_run_id": invocation,
                    "status": "failed" if failed else "success",
                    "started_at": started,
                    "ended_at": ended,
                    "duration_s": rr.get("elapsed_time"),
                    "trigger": args.get("which"),
                    "artifact_checksum": checksum,
                    "details": {
                        "dbt_version": rr["metadata"]["dbt_version"],
                        "target_dir": str(target.relative_to(REPO_ROOT))
                        if target.is_relative_to(REPO_ROOT)
                        else str(target),
                        "vars": args.get("vars"),
                        "results": len(results),
                        "failed": len(failed),
                    },
                },
            )
            for r in results:
                uid = r["unique_id"]
                node = nodes.get(uid, {})
                timing = {t["name"]: t for t in r.get("timing", [])}
                exe = timing.get("execute", {})
                if node.get("resource_type") == "test":
                    attached = node.get("attached_node") or next(
                        (
                            n
                            for n in node.get("depends_on", {}).get("nodes", [])
                            if n.startswith(("model.", "seed.", "source."))
                        ),
                        None,
                    )
                    target_node = nodes.get(attached or "", {})
                    meta = node.get("test_metadata") or {}
                    _upsert_quality(
                        conn,
                        {
                            "result_id": f"{run_id}/{uid}",
                            "check_id": uid,
                            "check_type": meta.get("name", "singular"),
                            "dataset_id": lineage.dataset_id_for_node(target_node)
                            if target_node
                            else None,
                            "column_name": node.get("column_name"),
                            "run_id": run_id,
                            "status": TEST_STATUS.get(r["status"], "error"),
                            "failures": r.get("failures"),
                            "message": _msg(r.get("message")),
                            "query_fingerprint": hashlib.sha256(
                                (node.get("compiled_code") or uid).encode()
                            ).hexdigest(),
                            "observed_at": _ts(exe.get("completed_at")) or ended,
                            "details": {"artifact_checksum": checksum},
                        },
                    )
                else:
                    _upsert_task(
                        conn,
                        {
                            "task_run_id": f"{run_id}/{uid}",
                            "run_id": run_id,
                            "task_id": uid,
                            "dataset_id": lineage.dataset_id_for_node(node) if node else None,
                            "status": DBT_STATUS.get(r["status"], "unknown"),
                            "started_at": _ts(exe.get("started_at")),
                            "ended_at": _ts(exe.get("completed_at")),
                            "duration_s": r.get("execution_time"),
                            "rows_affected": (r.get("adapter_response") or {}).get("rows_affected"),
                            "message": _msg(r.get("message")),
                            "details": {
                                "artifact_checksum": checksum,
                                "materialized": node.get("config", {}).get("materialized"),
                            },
                        },
                    )
                count += 1
        if latest_manifest is None or mf_path.stat().st_mtime > latest_manifest.stat().st_mtime:
            latest_manifest = mf_path
    if latest_manifest is not None:
        lineage.persist(lineage.build_graph(latest_manifest))
    return count


# ---------------------------------------------------------------- batch loader
def collect_ingestion() -> int:
    """raw.ingestion_batches (loader manifests) -> runs per correlation id, tasks per batch;
    cdc_pub.publications -> silver_publication runs."""
    with connect(Role.WH_READER, application_name="dataops-collector") as wh:
        batches = wh.execute(
            "select batch_id, source_table, window_start, window_end, status, extracted_rows,"
            " inserted_rows, duplicate_rows, started_at, finished_at, error_message, run_id,"
            " correlation_id, content_checksum from raw.ingestion_batches"
        ).fetchall()
        try:
            pubs = wh.execute(
                "select publication_id, silver_versions, row_counts, content_checksum,"
                " correlation_id, started_at, committed_at from cdc_pub.publications"
            ).fetchall()
        except Exception:  # cdc_pub not present on this warehouse
            pubs = []
    by_run: dict[str, list[tuple[Any, ...]]] = {}
    for b in batches:
        by_run.setdefault(b[11] or b[12], []).append(b)
    with connect(Role.REL_APP, application_name="dataops-collector") as conn, conn.transaction():
        for ext, rows in by_run.items():
            run_id = f"batch_ingestion:{ext}"
            last_by_table: dict[tuple[str, Any, Any], tuple[Any, ...]] = {}
            for b in sorted(rows, key=lambda b: b[8]):
                last_by_table[(b[1], b[2], b[3])] = b
            final = [b[4] for b in last_by_table.values()]
            status = (
                "failed" if "failed" in final else "running" if "running" in final else "success"
            )
            starts = [b[8] for b in rows]
            ends = [b[9] for b in rows if b[9]]
            _upsert_run(
                conn,
                {
                    "run_id": run_id,
                    "pipeline_id": "batch_ingestion",
                    "external_run_id": ext,
                    "status": status,
                    "started_at": min(starts),
                    "ended_at": max(ends) if ends else None,
                    "duration_s": (max(ends) - min(starts)).total_seconds() if ends else None,
                    "trigger": "airflow" if rows[0][11] else "cli",
                    "correlation_id": rows[0][12],
                    "details": {
                        "batches": len(rows),
                        "inserted_rows": sum(b[6] or 0 for b in rows),
                        "failed_attempts": sum(1 for b in rows if b[4] == "failed"),
                    },
                },
            )
            for b in rows:
                _upsert_task(
                    conn,
                    {
                        "task_run_id": f"{run_id}/{b[0]}",
                        "run_id": run_id,
                        "task_id": f"ingest:{b[1]}",
                        "dataset_id": f"warehouse.raw.{b[1]}",
                        "status": {
                            "committed": "success",
                            "failed": "failed",
                            "abandoned": "failed",
                            "running": "running",
                        }[b[4]],
                        "started_at": b[8],
                        "ended_at": b[9],
                        "duration_s": (b[9] - b[8]).total_seconds() if b[9] else None,
                        "rows_affected": b[6],
                        "message": _msg(b[10]),
                        "details": {
                            "batch_id": str(b[0]),
                            "window_start": b[2],
                            "window_end": b[3],
                            "extracted_rows": b[5],
                            "duplicate_rows": b[7],
                            "content_checksum": b[13],
                        },
                    },
                )
        for p in pubs:
            _upsert_run(
                conn,
                {
                    "run_id": f"silver_publication:{p[0]}",
                    "pipeline_id": "silver_publication",
                    "external_run_id": str(p[0]),
                    "status": "success",
                    "started_at": p[5],
                    "ended_at": p[6],
                    "duration_s": (p[6] - p[5]).total_seconds(),
                    "correlation_id": p[4],
                    "artifact_checksum": p[3],
                    "details": {"silver_versions": p[1], "row_counts": p[2]},
                },
            )
    return len(batches) + len(pubs)


# ---------------------------------------------------------------------- airflow
def collect_airflow(base_url: str | None = None, limit: int = 100) -> int:
    """Airflow 3 REST API v2 (JWT from /auth/token) -> runs and task instances."""
    import httpx

    base = base_url or os.environ.get("DATAOPS_AIRFLOW_URL", "http://127.0.0.1:8080")
    user = os.environ.get("DATAOPS_AIRFLOW_API_USER")
    password = os.environ.get("DATAOPS_AIRFLOW_API_PASSWORD")
    if not (user and password):
        raise ConnectionError("DATAOPS_AIRFLOW_API_USER / _PASSWORD not set")
    with httpx.Client(base_url=base, timeout=10.0) as http:
        tok = http.post("/auth/token", json={"username": user, "password": password})
        tok.raise_for_status()
        headers = {"Authorization": f"Bearer {tok.json()['access_token']}"}
        runs = http.get(
            "/api/v2/dags/~/dagRuns",
            headers=headers,
            params={"limit": limit, "order_by": "-start_date"},
        ).json()["dag_runs"]
        tis = http.get(
            "/api/v2/dags/~/dagRuns/~/taskInstances", headers=headers, params={"limit": 1000}
        ).json()["task_instances"]
    dag_ids = {r["dag_id"] for r in runs}
    ensure_pipelines({f"airflow:{d}": ("airflow", "standard", f"Airflow DAG {d}") for d in dag_ids})
    known = {(r["dag_id"], r["dag_run_id"]) for r in runs}
    with connect(Role.REL_APP, application_name="dataops-collector") as conn, conn.transaction():
        for r in runs:
            run_id = f"airflow:{r['dag_id']}:{r['dag_run_id']}"
            s, e = _ts(r.get("start_date")), _ts(r.get("end_date"))
            _upsert_run(
                conn,
                {
                    "run_id": run_id,
                    "pipeline_id": f"airflow:{r['dag_id']}",
                    "external_run_id": r["dag_run_id"],
                    "status": AIRFLOW_RUN.get(r.get("state") or "", "unknown"),
                    "started_at": s,
                    "ended_at": e,
                    "duration_s": (e - s).total_seconds() if s and e else None,
                    "trigger": r.get("run_type"),
                    "details": {
                        "logical_date": r.get("logical_date"),
                        "data_interval_start": r.get("data_interval_start"),
                        "data_interval_end": r.get("data_interval_end"),
                        "conf": r.get("conf"),
                    },
                },
            )
        for t in tis:
            if (t["dag_id"], t["dag_run_id"]) not in known:
                continue
            run_id = f"airflow:{t['dag_id']}:{t['dag_run_id']}"
            _upsert_task(
                conn,
                {
                    "task_run_id": f"{run_id}/{t['task_id']}/{t.get('try_number', 1)}",
                    "run_id": run_id,
                    "task_id": t["task_id"],
                    "status": AIRFLOW_TASK.get(t.get("state") or "", "unknown"),
                    "try_number": t.get("try_number") or 1,
                    "started_at": _ts(t.get("start_date")),
                    "ended_at": _ts(t.get("end_date")),
                    "duration_s": t.get("duration"),
                    "details": {
                        "operator": t.get("operator"),
                        "pool": t.get("pool"),
                        "hostname": t.get("hostname"),
                    },
                },
            )
    return len(runs) + len(tis)


# ------------------------------------------------------------------------ spark
def collect_spark(lake_dir: Path | None = None) -> int:
    from dataops.streaming.session import lake_root

    root = (lake_dir or lake_root()) / "_metrics"
    progress, batches = root / "progress.jsonl", root / "silver_batches.jsonl"
    if not progress.exists():
        raise FileNotFoundError(f"no Spark metrics at {progress} (streaming jobs never ran)")
    n = 0
    with connect(Role.REL_APP, application_name="dataops-collector") as conn, conn.transaction():
        for line in progress.read_text(encoding="utf-8").splitlines():
            p = json.loads(line)
            obs_id = f"spark_progress:{p['run_id']}:{p['batch_id']}"
            conn.execute(
                "insert into reliability.stream_observations (observation_id, kind, dataset_id,"
                " metrics, observed_at) values (%s,'spark_progress',%s,%s,%s)"
                " on conflict (observation_id) do nothing",
                (
                    obs_id,
                    {
                        "bronze": "lakehouse.bronze.cdc_events",
                        "silver": "lakehouse.silver",
                        "events": "lakehouse.silver.shipment_events_hourly",
                    }.get(p["job"]),
                    json.dumps(p),
                    _ts(p["timestamp"]),
                ),
            )
            n += 1
        if batches.exists():
            for line in batches.read_text(encoding="utf-8").splitlines():
                b = json.loads(line)
                ds = f"lakehouse.silver.{b['source_table']}"
                rid = f"spark:silver:{b['run_id']}/batch{b['silver_batch_id']}/{b['source_table']}"
                _upsert_quality(
                    conn,
                    {
                        "result_id": f"{rid}/quarantine",
                        "check_id": f"contract:quarantine:{ds}",
                        "check_type": "quarantine",
                        "dataset_id": ds,
                        "status": "fail" if b["quarantined"] else "pass",
                        "failures": b["quarantined"],
                        "message": f"{b['quarantined']} of {b['input_rows']} records quarantined"
                        f" by the shop v1 contract",
                        "observed_at": _ts(b["logged_at"]),
                        "details": {
                            "silver_batch_id": b["silver_batch_id"],
                            "transport_duplicates": b["transport_duplicates"],
                        },
                    },
                )
                n += 1
    return n


# ------------------------------------------------------------------------ kafka
def collect_kafka() -> int:
    """Lag of the Bronze stream behind the CDC topics, from its checkpoint (Structured
    Streaming does not commit consumer-group offsets), plus Debezium connector status."""
    from confluent_kafka import Consumer, TopicPartition

    from dataops.cdc import connector
    from dataops.cdc.stream import bootstrap_servers
    from dataops.streaming.session import checkpoint

    offsets_dir = Path(checkpoint("bronze")) / "offsets"
    committed: dict[str, int] = {}
    files = sorted(
        (p for p in offsets_dir.glob("*") if p.name.isdigit()), key=lambda p: int(p.name)
    )
    commits_dir = Path(checkpoint("bronze")) / "commits"
    done = {p.name for p in commits_dir.glob("*") if p.name.isdigit()}
    for f in reversed(files):  # latest batch whose commit file exists
        if f.name in done:
            for topic, parts in json.loads(f.read_text().splitlines()[-1]).items():
                committed[topic] = int(parts.get("0", 0))
            break
    now = datetime.now(UTC)
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers(),
            "group.id": "dataops-lag-probe",
            "enable.auto.commit": False,
        }
    )
    n = 0
    try:
        with (
            connect(Role.REL_APP, application_name="dataops-collector") as conn,
            conn.transaction(),
        ):
            for table in connector.CDC_TABLES:
                topic = connector.topic_for(table)
                _lo, hi = consumer.get_watermark_offsets(TopicPartition(topic, 0), timeout=10)
                pos = committed.get(topic, 0)
                lag = max(hi - pos, 0)
                oldest_age = None
                if lag:
                    consumer.assign([TopicPartition(topic, 0, pos)])
                    msg = consumer.poll(5)
                    if msg is not None and not msg.error():
                        ts_type, ts_ms = msg.timestamp()
                        oldest_age = (
                            now - datetime.fromtimestamp(ts_ms / 1000, UTC)
                        ).total_seconds()
                conn.execute(
                    "insert into reliability.stream_observations (observation_id, kind,"
                    " dataset_id, metrics, observed_at) values (%s,'kafka_lag',%s,%s,%s)",
                    (
                        f"kafka_lag:{topic}:{now.isoformat()}",
                        f"kafka.cdc.shop.{table}",
                        json.dumps(
                            {
                                "topic": topic,
                                "end_offset": hi,
                                "processed_offset": pos,
                                "lag_messages": lag,
                                "oldest_unprocessed_age_s": oldest_age,
                                "consumer": "spark:bronze (checkpoint)",
                            }
                        ),
                        now,
                    ),
                )
                n += 1
            st = connector.status()
            conn.execute(
                "insert into reliability.stream_observations (observation_id, kind, dataset_id,"
                " metrics, observed_at) values (%s,'connector_status','debezium',%s,%s)",
                (
                    f"connector_status:{connector.CONNECTOR_NAME}:{now.isoformat()}",
                    json.dumps(
                        {
                            "connector": st.get("connector", {}).get("state"),
                            "tasks": [
                                {
                                    "id": t.get("id"),
                                    "state": t.get("state"),
                                    "trace": (t.get("trace") or "")[:500],
                                }
                                for t in st.get("tasks", [])
                            ],
                        }
                    ),
                    now,
                ),
            )
            n += 1
    finally:
        consumer.close()
    return n


# -------------------------------------------------------------------- freshness
def collect_freshness() -> int:
    """data_lag: source high-water mark minus the warehouse input's high-water mark (data
    time, meaningful under the time simulation); load_age: wall clock since the last
    successful load. Paths without any committed data are skipped (not 'stale')."""
    contracts = load_contracts()
    now = datetime.now(UTC)
    # Only the input path dbt actually reads is judged (DATAOPS_SOURCE_PATH: batch | cdc);
    # the other path may exist with partial data and is not a freshness obligation.
    source_path = os.environ.get("DATAOPS_SOURCE_PATH", "batch")
    rows: list[tuple[Any, ...]] = []
    with connect(Role.SRC_READER, application_name="dataops-collector") as src:
        source_hw = {
            t: _scalar(src, f"select max(updated_at) from shop.{t}")  # noqa: S608
            for t in contracts
        }
    with connect(Role.WH_READER, application_name="dataops-collector") as wh:
        raw_hw: dict[str, datetime] = dict(
            wh.execute(
                "select r.source_table, max(r.source_updated_at) from raw.source_records r"
                " join raw.ingestion_batches b on b.batch_id = r.batch_id and b.status='committed'"
                " group by 1"
            ).fetchall()
        )
        last_batch = _scalar(
            wh, "select max(finished_at) from raw.ingestion_batches where status='committed'"
        )
        try:
            pub_hw: dict[str, datetime] = dict(
                wh.execute(
                    "select source_table, max(source_updated_at) from cdc_pub.records group by 1"
                ).fetchall()
            )
            last_pub = _scalar(wh, "select max(committed_at) from cdc_pub.publications")
        except Exception:
            pub_hw, last_pub = {}, None
    active_paths = (
        [("warehouse.raw", raw_hw)] if source_path == "batch" else [("warehouse.cdc_pub", pub_hw)]
    )
    for t, c in contracts.items():
        threshold = c.freshness_hours * 3600
        for prefix, hw in active_paths:
            if not hw or source_hw[t] is None:
                continue
            target = hw.get(t)
            lag = (source_hw[t] - target).total_seconds() if target else None
            status = "unknown" if lag is None else ("stale" if lag > threshold else "fresh")
            rows.append(
                (
                    f"{prefix}.{t}",
                    "data_lag",
                    lag,
                    threshold,
                    status,
                    source_hw[t],
                    target,
                    f"freshness:data_lag:{prefix}",
                )
            )
    load_checks = {
        "batch": ("warehouse.raw.source_records", last_batch, 26 * 3600),
        "cdc": ("warehouse.cdc_pub.records", last_pub, 2 * 3600),
    }
    for ds, last, thr in [load_checks[source_path]]:
        if last is not None:
            age = (now - last).total_seconds()
            rows.append(
                (
                    ds,
                    "load_age",
                    age,
                    thr,
                    "stale" if age > thr else "fresh",
                    None,
                    last,
                    f"freshness:load_age:{ds}",
                )
            )
    with connect(Role.REL_APP, application_name="dataops-collector") as conn, conn.transaction():
        for r in rows:
            conn.execute(
                "insert into reliability.freshness_observations (observation_id, dataset_id,"
                " measure, lag_seconds, threshold_seconds, status, source_high_water,"
                " target_high_water, query_fingerprint) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (str(uuid.uuid4()), *r[:7], hashlib.sha256(r[7].encode()).hexdigest()),
            )
    return len(rows)


# ----------------------------------------------------------------------- schema
PG_TO_CONTRACT = {
    "bigint": "int",
    "integer": "int",
    "smallint": "int",
    "text": "text",
    "character varying": "text",
    "boolean": "bool",
    "timestamp with time zone": "timestamp",
}


def _contract_type(data_type: str, char_len: int | None) -> str:
    if data_type == "character":
        return f"char{char_len}"
    return PG_TO_CONTRACT.get(data_type, data_type)


def compare_to_contract(
    observed: list[dict[str, Any]], contract: Any
) -> tuple[str, list[dict[str, Any]]]:
    """Classify an observed table schema against its contract (rules in shop.v1.yaml)."""
    diffs: list[dict[str, Any]] = []
    obs = {c["name"]: c for c in observed}
    for col in contract.columns:
        o = obs.get(col.name)
        if o is None:
            diffs.append({"column": col.name, "change": "removed", "breaking": True})
            continue
        if o["type"] != col.type:
            diffs.append(
                {
                    "column": col.name,
                    "change": "type_changed",
                    "expected": col.type,
                    "observed": o["type"],
                    "breaking": True,
                }
            )
        if col.required and o["nullable"]:
            diffs.append({"column": col.name, "change": "became_nullable", "breaking": True})
    for name, o in obs.items():
        if name not in contract.column_names:
            diffs.append(
                {
                    "column": name,
                    "change": "added",
                    "observed": o["type"],
                    "nullable": o["nullable"],
                    "breaking": False,
                }
            )
    if not diffs:
        return "matches_contract", diffs
    return ("breaking" if any(d["breaking"] for d in diffs) else "compatible"), diffs


def collect_schema() -> int:
    contracts = load_contracts()
    with connect(Role.SRC_READER, application_name="dataops-collector") as src:
        cols = src.execute(
            "select table_name, column_name, data_type, character_maximum_length, is_nullable"
            " from information_schema.columns where table_schema = 'shop'"
            " order by table_name, ordinal_position"
        ).fetchall()
        # information_schema only lists columns the role may read: a table whose SELECT was
        # revoked would look like "all columns removed". Such tables are not observed (the
        # failing extraction reports the access problem instead).
        unreadable = {
            r[0]
            for r in src.execute(
                "select c.relname from pg_class c join pg_namespace n on n.oid = c.relnamespace"
                " where n.nspname = 'shop' and c.relkind = 'r'"
                " and not has_table_privilege(c.oid, 'SELECT')"
            ).fetchall()
        }
    if unreadable:
        log.warning("schema collector cannot read %s; not observed", sorted(unreadable))
    by_table: dict[str, list[dict[str, Any]]] = {}
    for t, name, dtype, clen, nullable in cols:
        by_table.setdefault(t, []).append(
            {"name": name, "type": _contract_type(dtype, clen), "nullable": nullable == "YES"}
        )
    n = 0
    with connect(Role.REL_APP, application_name="dataops-collector") as conn, conn.transaction():
        for t, contract in contracts.items():
            if t in unreadable:
                continue
            observed = by_table.get(t, [])
            checksum = hashlib.sha256(json.dumps(observed, sort_keys=True).encode()).hexdigest()
            ds = f"commerce_source.shop.{t}"
            prev = conn.execute(
                "select schema_checksum from reliability.schema_observations where dataset_id=%s"
                " order by observed_at desc limit 1",
                (ds,),
            ).fetchone()
            if prev and prev[0] == checksum:
                continue  # unchanged since the last observation
            compat, diffs = compare_to_contract(observed, contract)
            conn.execute(
                "insert into reliability.schema_observations (observation_id, dataset_id,"
                " columns, schema_checksum, previous_checksum, changed, contract_id,"
                " contract_version, compatibility, differences) values"
                " (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    str(uuid.uuid4()),
                    ds,
                    json.dumps(observed),
                    checksum,
                    prev[0] if prev else None,
                    prev is not None,
                    contract.contract,
                    contract.version,
                    compat,
                    json.dumps(diffs),
                ),
            )
            n += 1
    return n


def register_contracts() -> int:
    contracts = load_contracts()
    with connect(Role.REL_APP, application_name="dataops-collector") as conn, conn.transaction():
        for t, c in contracts.items():
            spec = {
                "key": c.key,
                "freshness_hours": c.freshness_hours,
                "columns": [
                    {
                        "name": x.name,
                        "type": x.type,
                        "required": x.required,
                        "allowed": list(x.allowed) if x.allowed else None,
                        "min": x.min,
                    }
                    for x in c.columns
                ],
            }
            # The lineage build registers full dataset metadata; make sure the source table
            # exists even before dbt artifacts have been collected.
            conn.execute(
                "insert into reliability.datasets (dataset_id, system, kind, display_name,"
                " freshness_hours, contract_id, registered_by) values"
                " (%s, 'commerce_source', 'source_table', %s, %s, %s, 'contract')"
                " on conflict (dataset_id) do nothing",
                (
                    f"commerce_source.shop.{t}",
                    f"shop.{t}",
                    c.freshness_hours,
                    f"{c.contract}.v{c.version}",
                ),
            )
            conn.execute(
                "insert into reliability.dataset_contract_versions (contract_id, version,"
                " dataset_id, key_column, spec, spec_checksum, source_path) values"
                " (%s,%s,%s,%s,%s,%s,%s) on conflict do nothing",
                (
                    c.contract,
                    c.version,
                    f"commerce_source.shop.{t}",
                    c.key,
                    json.dumps(spec),
                    hashlib.sha256(json.dumps(spec, sort_keys=True).encode()).hexdigest(),
                    f"contracts/source/{c.contract}.v{c.version}.yaml",
                ),
            )
    return len(contracts)


COLLECTORS: dict[str, Callable[[], int]] = {
    "dbt": collect_dbt,
    "ingestion": collect_ingestion,
    "schema": collect_schema,
    "freshness": collect_freshness,
    "airflow": collect_airflow,
    "spark": collect_spark,
    "kafka": collect_kafka,
}


def _unavailable_errors() -> tuple[type[BaseException], ...]:
    """Exceptions meaning 'the observed system is not reachable', not a collector bug."""
    errs: list[type[BaseException]] = [ConnectionError, FileNotFoundError, OSError]
    try:
        import httpx

        errs.append(httpx.TransportError)
    except ImportError:  # pragma: no cover
        pass
    try:
        from confluent_kafka import KafkaException

        errs.append(KafkaException)
    except ImportError:  # pragma: no cover
        pass
    return tuple(errs)


UNAVAILABLE = _unavailable_errors()


def collect(names: list[str] | None = None) -> dict[str, dict[str, Any]]:
    """Run collectors; each failure is isolated and recorded."""
    ensure_pipelines()
    register_contracts()
    out: dict[str, dict[str, Any]] = {}
    for name in names or list(COLLECTORS):
        started = datetime.now(UTC)
        try:
            n = COLLECTORS[name]()
            out[name] = {"status": "success", "records": n}
        except UNAVAILABLE as exc:
            out[name] = {
                "status": "unavailable",
                "records": 0,
                "message": _msg(f"{type(exc).__name__}: {exc}", 300),
            }
        except Exception as exc:  # recorded, never propagated to other collectors
            log.exception("collector %s failed", name)
            out[name] = {
                "status": "failed",
                "records": 0,
                "message": _msg(f"{type(exc).__name__}: {exc}", 300),
            }
        try:
            _record_collector(
                name, out[name]["status"], out[name]["records"], out[name].get("message"), started
            )
        except Exception:
            log.exception("could not record collector run for %s", name)
    return out
