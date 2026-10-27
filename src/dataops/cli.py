"""Command-line entry point: `python -m dataops <command>` (or `dataops <command>`).

Every command prints one JSON document to stdout; logs go to stderr as JSON lines.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from dataops.config import REPO_ROOT, load_dotenv, redact

DEFAULT_HISTORY_START = "2020-01-01T00:00:00+00:00"


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": redact(record.getMessage()),
        }
        if record.exc_info:
            entry["exc"] = redact(self.formatException(record.exc_info))
        return json.dumps(entry)


def _setup_logging(level: str) -> None:
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=level.upper(), handlers=[handler], force=True)


def _ts(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError(f"timestamp must include a UTC offset: {value}")
    return parsed.astimezone(UTC)


def _duration(value: str) -> timedelta:
    units = {"m": "minutes", "h": "hours", "d": "days"}
    if not value or value[-1] not in units or not value[:-1].isdigit():
        raise argparse.ArgumentTypeError("duration like 15m, 6h or 1d")
    return timedelta(**{units[value[-1]]: int(value[:-1])})


def _emit(payload: Any) -> None:
    print(json.dumps(payload, indent=2, default=str))


def cmd_migrate(args: argparse.Namespace) -> int:
    from dataops.migrate import migrate

    _emit({"applied": migrate(args.targets or None)})
    return 0


def _ledger_path(profile: str, seed: int) -> Path:
    return REPO_ROOT / "var" / "ledger" / f"{profile}-seed{seed}.json"


def cmd_generate(args: argparse.Namespace) -> int:
    from dataops.generator.generate import generate
    from dataops.generator.ledger import build_ledger, write_ledger

    ds = generate(args.seed, args.profile)
    ledger = build_ledger(ds)
    out = write_ledger(
        ledger, Path(args.ledger_out) if args.ledger_out else _ledger_path(args.profile, args.seed)
    )
    result: dict[str, Any] = {
        "synthetic": True,
        "seed": args.seed,
        "profile": args.profile,
        "simulation_start": ds.start.isoformat(),
        "simulation_end": ds.end.isoformat(),
        "dataset_checksum": ds.checksum(),
        "row_counts": ds.row_counts(),
        "ledger_path": str(out),
    }
    if args.load:
        from dataops.generator.loader import load_source

        result["loaded"] = load_source(ds, replace=args.replace)
    _emit(result)
    return 0


def cmd_simulate(args: argparse.Namespace) -> int:
    from dataops.generator.generate import generate
    from dataops.generator.ledger import build_ledger, write_ledger
    from dataops.generator.simulator import simulate

    ds = generate(args.seed, args.profile)
    until = args.until or ds.end
    if until > ds.end:
        raise SystemExit(f"--until is after the simulation end {ds.end.isoformat()}")
    write_ledger(build_ledger(ds), _ledger_path(args.profile, args.seed))
    result = simulate(ds, until, reset=args.reset)
    _emit(
        {
            "synthetic": True,
            "seed": args.seed,
            "profile": args.profile,
            "simulation_start": ds.start.isoformat(),
            "simulation_end": ds.end.isoformat(),
            **result,
        }
    )
    return 0


def cmd_ingest(args: argparse.Namespace) -> int:
    from dataops.generator.generate import TABLE_ORDER
    from dataops.ingestion.batch import ingest_range, ingest_table, watermark

    tables = TABLE_ORDER if args.tables == "all" else [t.strip() for t in args.tables.split(",")]
    common = {
        "overlap": args.overlap,
        "run_id": args.run_id,
        "correlation_id": args.correlation_id or str(uuid.uuid4()),
    }
    results = []
    if args.until:
        history = _ts(args.history_start)
        for table in [t for t in TABLE_ORDER if t in tables]:
            start = watermark(table) or history
            if start >= args.until:
                results.append(
                    {"source_table": table, "status": "up_to_date", "watermark": start.isoformat()}
                )
                continue
            results.append(ingest_table(table, start, args.until, **common).as_dict())
    else:
        if not (args.start and args.end):
            raise SystemExit("provide --until, or both --start and --end")
        results = [
            r.as_dict()
            for r in ingest_range(tables, args.start, args.end, step=args.step, **common)
        ]
    failed = [r for r in results if r.get("status") == "failed"]
    _emit(
        {
            "batches": sum(1 for r in results if r.get("status") != "up_to_date"),
            "up_to_date_tables": [
                r["source_table"] for r in results if r.get("status") == "up_to_date"
            ],
            "failed": len(failed),
            "inserted_rows": sum(r.get("inserted_rows", 0) for r in results),
            "duplicate_rows": sum(r.get("duplicate_rows", 0) for r in results),
            "results": results if args.verbose else failed,
        }
    )
    return 1 if failed else 0


def cmd_reconcile(args: argparse.Namespace) -> int:
    from dataops.reconciliation import revenue

    correlation_id = args.correlation_id or str(uuid.uuid4())
    persist = not args.no_persist
    results = [
        revenue.reconcile_row_counts(correlation_id, persist, args.source_path),
        revenue.reconcile_source_vs_mart(correlation_id, persist),
    ]
    if args.ledger:
        ledger = json.loads(Path(args.ledger).read_text(encoding="utf-8"))
        results.append(revenue.reconcile_ledger_vs_mart(ledger, correlation_id, persist))
        results.append(revenue.reconcile_inventory_ledger_vs_mart(ledger, correlation_id, persist))
    _emit({"correlation_id": correlation_id, "results": [r.as_dict() for r in results]})
    return 0 if all(r.status == "pass" for r in results) else 1


def cmd_fingerprint(args: argparse.Namespace) -> int:
    from dataops.reconciliation.fingerprint import fingerprint_all

    prints = fingerprint_all(args.models or None)
    _emit({m: {"rows": f.rows, "md5": f.md5} for m, f in prints.items()})
    return 0


def cmd_stream(args: argparse.Namespace) -> int:
    from dataops.streaming import jobs

    if args.stream_cmd == "replay":
        if not args.confirm_rebuild_from_bronze:
            raise SystemExit(
                "replay deletes Silver and its checkpoints and rebuilds them from "
                "Bronze; re-run with --confirm-rebuild-from-bronze"
            )
        _emit(jobs.replay_silver())
        return 0
    if args.stream_cmd == "reconcile":
        from dataops.streaming import reconcile
        from dataops.streaming.session import get_spark

        r = reconcile.reconcile(get_spark(), args.correlation_id or str(uuid.uuid4()))
        _emit(r.as_dict())
        return 0 if r.status == "pass" else 1
    report = jobs.run(
        args.jobs,
        available_now=not args.continuous,
        processing_time=args.processing_time,
        run_seconds=args.run_seconds,
    )
    for v in report.values():
        v.pop("last_progress", None)
        v.pop("batch_ids", None)
    _emit(report)
    return 0


def cmd_publish(args: argparse.Namespace) -> int:
    from dataops.streaming.publish import publish
    from dataops.streaming.session import get_spark

    _emit(publish(get_spark("dataops-publisher"), args.correlation_id or str(uuid.uuid4())))
    return 0


def cmd_cdc(args: argparse.Namespace) -> int:
    from dataops.cdc import connector

    if args.cdc_cmd == "register":
        connector.register()
        _emit(connector.wait_running())
    elif args.cdc_cmd == "status":
        _emit(connector.status())
    elif args.cdc_cmd == "reconcile":
        from dataops.cdc import reconcile

        r = reconcile.reconcile(args.correlation_id or str(uuid.uuid4()))
        _emit(r.as_dict())
        return 0 if r.status == "pass" else 1
    return 0


def cmd_collect(args: argparse.Namespace) -> int:
    from dataops.reliability.collectors import collect

    out = collect(args.collectors or None)
    _emit(out)
    return 0 if all(v["status"] != "failed" for v in out.values()) else 1


def cmd_lineage(args: argparse.Namespace) -> int:
    from dataops.reliability import lineage

    g, version = lineage.load_latest()
    if version is None:
        raise SystemExit("no lineage collected yet: run `dataops collect --collectors dbt`")
    if args.lineage_cmd == "impact":
        _emit({"lineage_version": version, **lineage.impact(g, args.dataset)})
    elif args.lineage_cmd == "upstream":
        _emit(
            {
                "lineage_version": version,
                "dataset_id": args.dataset,
                "upstream": g.upstream(args.dataset),
            }
        )
    elif args.lineage_cmd == "path":
        p = g.path(args.dataset, args.to)
        _emit({"lineage_version": version, "path": p, "hops": g.explain(p) if p else None})
    else:
        _emit(
            {
                "lineage_version": version,
                "datasets": len(g.nodes),
                "edges": len(g.edges),
                "cycles": g.find_cycles(),
            }
        )
    return 0


def cmd_incidents(args: argparse.Namespace) -> int:
    from dataops.reliability import incidents

    if args.incident_cmd == "evaluate":
        _emit(incidents.evaluate())
    elif args.incident_cmd == "list":
        _emit(incidents.list_incidents(active_only=not args.all))
    elif args.incident_cmd == "close":
        if not (args.incident_id and args.reason):
            raise SystemExit("close needs an incident id and --reason")
        _emit(
            incidents.close(
                args.incident_id, args.reason, args.actor or os.environ.get("USER", "cli")
            )
        )
    else:
        if not args.incident_id:
            raise SystemExit("show needs an incident id")
        _emit(incidents.show(args.incident_id))
    return 0


def cmd_runbooks(args: argparse.Namespace) -> int:
    from dataops.investigator import runbooks

    if args.runbook_cmd == "index":
        _emit(runbooks.index())
    else:
        if not args.query:
            raise SystemExit("search needs a query")
        hits = runbooks.search(" ".join(args.query), args.k, args.incident_type)
        _emit([{k: v for k, v in h.items() if k != "content"} for h in hits])
    return 0


def cmd_investigate(args: argparse.Namespace) -> int:
    from dataops.investigator import engine

    if args.show:
        _emit(engine.show(args.target))
        return 0
    result = engine.investigate(args.target, mode=args.mode, question=args.question)
    if result["mode"] == "fixture":
        print(
            "NOTE: fixture mode is a deterministic test harness, not AI inference.", file=sys.stderr
        )
    _emit(
        result
        if args.verbose
        else {
            k: result[k]
            for k in (
                "investigation_id",
                "mode",
                "model",
                "status",
                "output",
                "error",
                "input_tokens",
                "output_tokens",
                "latency_ms",
            )
        }
        | {
            "tool_calls": len(result["tool_calls"]),
            "invalid_citations": result["validation"].get("invalid_citations"),
        }
    )
    return 0 if result["status"] != "failed" else 1


def cmd_eval(args: argparse.Namespace) -> int:
    from dataops.evaluation import harness

    report = harness.run(args.mode, args.case or None, rebuild=args.rebuild)
    _emit(
        {k: report[k] for k in ("mode", "label", "model", "metrics", "files")}
        | {"retrieval": {k: v for k, v in report["retrieval"].items() if k != "rows"}}
    )
    return 0


def cmd_reset(args: argparse.Namespace) -> int:
    if not args.confirm_synthetic_reset:
        raise SystemExit(
            "refusing to reset: this deletes the synthetic source, warehouse raw zone and dbt "
            "schemas. Re-run with --confirm-synthetic-reset."
        )
    from dataops.admin import reset_synthetic_environment

    _emit({"reset": reset_synthetic_environment(include_reliability=args.include_reliability)})
    return 0


def cmd_health(args: argparse.Namespace) -> int:
    """Readiness: every role can connect to its database and the expected schemas exist."""
    from dataops.config import Role
    from dataops.db import connect

    probes = {
        Role.SRC_READER: "select count(*) from shop.schema_migrations",
        Role.WH_LOADER: "select count(*) from raw.schema_migrations",
        Role.WH_READER: "select count(*) from raw.ingestion_batches",
        Role.REL_APP: "select count(*) from reliability.schema_migrations",
        Role.REL_READER: "select count(*) from reliability.reconciliation_results",
    }
    checks = []
    for role, sql in probes.items():
        try:
            with connect(role, application_name="dataops-health", attempts=1) as conn:
                conn.execute(sql).fetchone()
            checks.append({"role": role.value, "status": "ok"})
        except Exception as exc:  # report every failure, do not stop at the first
            checks.append({"role": role.value, "status": "error", "error": redact(str(exc))})
    ok = all(c["status"] == "ok" for c in checks)
    _emit({"ready": ok, "checks": checks})
    return 0 if ok else 1


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="dataops", description=__doc__)
    p.add_argument("--log-level", default="INFO")
    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("health", help="check database readiness for every role").set_defaults(
        func=cmd_health
    )

    db = sub.add_parser("db", help="database administration").add_subparsers(
        dest="db_cmd", required=True
    )
    mig = db.add_parser("migrate", help="apply schema migrations")
    mig.add_argument("--targets", nargs="*", choices=["source", "warehouse", "reliability"])
    mig.set_defaults(func=cmd_migrate)

    gen = sub.add_parser("generate", help="generate synthetic data and its expected-results ledger")
    gen.add_argument("--seed", type=int, default=42)
    gen.add_argument("--profile", default="small", choices=["tiny", "small", "medium", "large"])
    gen.add_argument("--load", action="store_true", help="load into the source database")
    gen.add_argument(
        "--replace", action="store_true", help="truncate and reload the synthetic source tables"
    )
    gen.add_argument("--ledger-out")
    gen.set_defaults(func=cmd_generate)

    sim = sub.add_parser("simulate", help="advance the synthetic source through simulated time")
    sim.add_argument("--seed", type=int, default=42)
    sim.add_argument("--profile", default="small", choices=["tiny", "small", "medium", "large"])
    sim.add_argument("--until", type=_ts, help="simulated time to advance to (default: end)")
    sim.add_argument(
        "--reset", action="store_true", help="truncate the synthetic source and restart the clock"
    )
    sim.set_defaults(func=cmd_simulate)

    ing = sub.add_parser("ingest", help="incremental batch extraction into warehouse raw")
    ing.add_argument("--tables", default="all")
    ing.add_argument("--until", type=_ts, help="catch up each table from its watermark to here")
    ing.add_argument(
        "--history-start",
        default=DEFAULT_HISTORY_START,
        help="start for tables without a watermark (with --until)",
    )
    ing.add_argument("--start", type=_ts)
    ing.add_argument("--end", type=_ts)
    ing.add_argument("--step", type=_duration, default=timedelta(days=1))
    ing.add_argument("--overlap", type=_duration, default=timedelta(minutes=15))
    ing.add_argument("--run-id")
    ing.add_argument("--correlation-id")
    ing.add_argument("--verbose", action="store_true")
    ing.set_defaults(func=cmd_ingest)

    rec = sub.add_parser("reconcile", help="reconcile source, ledger and revenue mart")
    rec.add_argument("--ledger", help="path to an expected-results ledger JSON")
    rec.add_argument("--no-persist", action="store_true")
    rec.add_argument(
        "--source-path",
        choices=["batch", "cdc"],
        default=os.environ.get("DATAOPS_SOURCE_PATH", "batch"),
        help="which warehouse input to count against the source",
    )
    rec.add_argument("--correlation-id")
    rec.set_defaults(func=cmd_reconcile)

    cdc = sub.add_parser("cdc", help="Debezium change data capture (full profile)")
    cdc.add_argument("cdc_cmd", choices=["register", "status", "reconcile"])
    cdc.add_argument("--correlation-id")
    cdc.set_defaults(func=cmd_cdc)

    stm = sub.add_parser("stream", help="Spark Bronze/Silver/event jobs (full profile)")
    stm.add_argument("stream_cmd", choices=["run", "replay", "reconcile"])
    stm.add_argument(
        "--jobs",
        nargs="+",
        default=["bronze", "silver", "events"],
        choices=["bronze", "silver", "events"],
    )
    stm.add_argument(
        "--continuous",
        action="store_true",
        help="processing-time triggers instead of process-available-and-stop",
    )
    stm.add_argument("--processing-time", default="10 seconds")
    stm.add_argument("--run-seconds", type=float)
    stm.add_argument("--confirm-rebuild-from-bronze", action="store_true")
    stm.add_argument("--correlation-id")
    stm.set_defaults(func=cmd_stream)

    pub = sub.add_parser("publish", help="publish curated Silver into the warehouse (cdc_pub)")
    pub.add_argument("--correlation-id")
    pub.set_defaults(func=cmd_publish)

    col = sub.add_parser("collect", help="collect reliability metadata")
    col.add_argument(
        "--collectors",
        nargs="*",
        choices=["dbt", "ingestion", "schema", "freshness", "airflow", "spark", "kafka"],
    )
    col.set_defaults(func=cmd_collect)

    lin = sub.add_parser("lineage", help="lineage graph and impact analysis")
    lin.add_argument("lineage_cmd", choices=["impact", "upstream", "path", "summary"])
    lin.add_argument("dataset", nargs="?", default="")
    lin.add_argument("--to", help="target dataset for `path`")
    lin.set_defaults(func=cmd_lineage)

    inc = sub.add_parser("incidents", help="deterministic incident detection")
    inc.add_argument("incident_cmd", choices=["evaluate", "list", "show", "close"])
    inc.add_argument("incident_id", nargs="?")
    inc.add_argument("--all", action="store_true", help="include resolved/closed incidents")
    inc.add_argument("--reason", help="why the incident is closed (required for close)")
    inc.add_argument("--actor", help="who closes it (default: $USER)")
    inc.set_defaults(func=cmd_incidents)

    rbk = sub.add_parser("runbooks", help="runbook retrieval index")
    rbk.add_argument("runbook_cmd", choices=["index", "search"])
    rbk.add_argument("query", nargs="*")
    rbk.add_argument("--k", type=int, default=4)
    rbk.add_argument("--incident-type")
    rbk.set_defaults(func=cmd_runbooks)

    inv = sub.add_parser("investigate", help="read-only AI investigation of an incident")
    inv.add_argument("target", help="incident id (or investigation id with --show)")
    inv.add_argument(
        "--mode",
        choices=["model", "fixture"],
        help="model = real LLM (default); fixture = labelled deterministic test harness",
    )
    inv.add_argument("--question")
    inv.add_argument("--show", action="store_true", help="show a recorded investigation")
    inv.add_argument("--verbose", action="store_true")
    inv.set_defaults(func=cmd_investigate)

    evl = sub.add_parser("eval", help="run the investigator evaluation suite on *_eval databases")
    evl.add_argument("--mode", choices=["model", "fixture"], required=True)
    evl.add_argument("--case", action="append", help="run only these case ids")
    evl.add_argument("--rebuild", action="store_true", help="recreate the *_eval databases")
    evl.set_defaults(func=cmd_eval)

    rst = sub.add_parser("reset", help="DESTRUCTIVE: empty the synthetic demo environment")
    rst.add_argument("--confirm-synthetic-reset", action="store_true")
    rst.add_argument(
        "--include-reliability",
        action="store_true",
        help="also delete reliability metadata (kept by default)",
    )
    rst.set_defaults(func=cmd_reset)

    fp = sub.add_parser("fingerprint", help="content fingerprints of warehouse models")
    fp.add_argument("--models", nargs="*", help="e.g. marts.fct_order_item (default: all)")
    fp.set_defaults(func=cmd_fingerprint)
    return p


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    args = build_parser().parse_args(argv)
    _setup_logging(args.log_level)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
