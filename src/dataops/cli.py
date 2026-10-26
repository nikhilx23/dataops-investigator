"""Command-line entry point: `python -m dataops <command>` (or `dataops <command>`).

Every command prints one JSON document to stdout; logs go to stderr as JSON lines.
"""

from __future__ import annotations

import argparse
import json
import logging
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
        revenue.reconcile_row_counts(correlation_id, persist),
        revenue.reconcile_source_vs_mart(correlation_id, persist),
    ]
    if args.ledger:
        ledger = json.loads(Path(args.ledger).read_text(encoding="utf-8"))
        results.append(revenue.reconcile_ledger_vs_mart(ledger, correlation_id, persist))
    _emit({"correlation_id": correlation_id, "results": [r.as_dict() for r in results]})
    return 0 if all(r.status == "pass" for r in results) else 1


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
    rec.add_argument("--correlation-id")
    rec.set_defaults(func=cmd_reconcile)
    return p


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    args = build_parser().parse_args(argv)
    _setup_logging(args.log_level)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
