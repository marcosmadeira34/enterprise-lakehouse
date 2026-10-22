"""Command line interface. Prints a JSON summary on stdout; logs (JSON) go to stderr.

Exit codes: 0 success, 2 a BLOCKING quality rule failed, 3 a lock is held by another process,
1 any other error. Airflow tasks rely on these codes.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from collections.abc import Callable
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from lakehouse.config import Settings
from lakehouse.contracts import ENTITY_ORDER
from lakehouse.observability.logging import configure_logging, log_event

log = logging.getLogger("lakehouse.cli")

EXIT_OK, EXIT_ERROR, EXIT_QUALITY, EXIT_LOCKED = 0, 1, 2, 3


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return str(value)


def _entities(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--entity", action="append", choices=ENTITY_ORDER, help="repeatable; default: all")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="lakehouse", description="Enterprise Lakehouse Platform")
    parser.add_argument("--config", help="YAML config (default: $LAKEHOUSE_CONFIG or config/lakehouse.yaml)")
    parser.add_argument("--correlation-id", help="shared id of a pipeline execution (e.g. Airflow run_id)")
    parser.add_argument("--trigger", default="cli", help="who started the run (cli, airflow, benchmark)")
    parser.add_argument("--attempt", type=int, default=1, help="attempt number (Airflow try_number)")
    parser.add_argument("--log-level", default="INFO")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init", help="create metadata schema and storage containers")

    gen = sub.add_parser("generate", help="write synthetic retail files to the landing zone")
    gen.add_argument("--drop", type=int, default=1, choices=(1, 2), help="1 = initial load, 2 = incremental")
    gen.add_argument("--seed", type=int, default=42)
    gen.add_argument("--scale", default="small", choices=("tiny", "small", "medium", "large"))
    gen.add_argument("--files-per-entity", type=int, default=2)
    gen.add_argument("--error-rate", type=float, default=0.01, help="share of anomalous records per scenario")
    gen.add_argument("--scenarios", help="comma-separated subset (default: all); 'none' for clean data")
    gen.add_argument("--store-skew", type=float, default=0.0, help="Zipf exponent for sales per store")
    gen.add_argument("--landing", help="override landing URI")

    for name, text in (("ingest", "discover landing files and plan batches"),
                       ("bronze", "load planned batches into Bronze"), ("silver", "conform Bronze into Silver")):  # fmt: skip
        p = sub.add_parser(name, help=text)
        _entities(p)
        if name != "ingest":
            p.add_argument("--batch-id", action="append", help="restrict to these batches (repeatable)")

    sub.add_parser("gold", help="rebuild, audit and publish Gold")
    q = sub.add_parser("quality", help="run Silver/Gold quality suites")
    q.add_argument("--suite", action="append", choices=("silver", "gold"))
    rec = sub.add_parser("reconcile", help="cross-layer reconciliation")
    _entities(rec)
    run_all = sub.add_parser("run-all", help="ingest -> bronze -> silver -> gold -> quality -> reconcile")
    _entities(run_all)

    rp = sub.add_parser("reprocess", help="historical reprocessing of Silver (and Gold) from Bronze")
    rp.add_argument("--entity", action="append", choices=ENTITY_ORDER, required=True)
    rp.add_argument("--batch-id", action="append")
    rp.add_argument("--since", help="ingestion date lower bound (yyyy-mm-dd)")
    rp.add_argument("--until", help="ingestion date upper bound (yyyy-mm-dd)")
    rp.add_argument("--full-refresh", action="store_true", help="rebuild the entity from all Bronze batches")
    rp.add_argument("--skip-gold", action="store_true")

    sub.add_parser("recover", help="close abandoned runs and make interrupted batches retryable")
    sub.add_parser("status", help="layer totals, file states, last runs, current publication")
    runs = sub.add_parser("runs", help="run history")
    runs.add_argument("--limit", type=int, default=20)
    runs.add_argument("--run-id")
    rj = sub.add_parser("rejects", help="inspect rejected records")
    rj.add_argument("--entity", required=True, choices=ENTITY_ORDER)
    rj.add_argument("--reason")
    rj.add_argument("--limit", type=int, default=20)
    return parser


def _generate(settings: Settings, args: argparse.Namespace) -> dict[str, Any]:
    from lakehouse.common.storage import get_store
    from lakehouse.generator.retail import ALL_SCENARIOS, GeneratorConfig, generate

    landing = args.landing or settings.storage.landing_uri
    if args.scenarios is None:
        scenarios: tuple[str, ...] = ALL_SCENARIOS
    elif args.scenarios.strip().lower() == "none":
        scenarios = ()
    else:
        scenarios = tuple(s.strip() for s in args.scenarios.split(",") if s.strip())
    config = GeneratorConfig(seed=args.seed, scale=args.scale, files_per_entity=args.files_per_entity,
                             error_rate=args.error_rate, scenarios=scenarios, store_skew=args.store_skew)  # fmt: skip
    store = get_store(landing, settings.storage)
    store.ensure_container(landing)
    manifest = generate(store, landing, config, drop=args.drop)
    return {"drop": manifest["drop"], "files": len(manifest["files"]),
            "rows": sum(max(f["rows"], 0) for f in manifest["files"]),
            "expected_rejections": len(manifest["expected"]["rejections"]),
            "manifest": f"{landing}/_manifests/drop_{args.drop:03d}.json"}  # fmt: skip


def dispatch(args: argparse.Namespace) -> Any:
    from lakehouse.pipelines.runner import Platform

    settings = Settings.load(args.config)
    if args.command == "generate":
        return _generate(settings, args)
    platform = Platform(
        settings, correlation_id=args.correlation_id, trigger=args.trigger, attempt=args.attempt
    )
    entities = getattr(args, "entity", None)
    batch_ids = getattr(args, "batch_id", None)
    commands: dict[str, Callable[[], Any]] = {
        "init": platform.init,
        "ingest": lambda: platform.ingest(entities),
        "bronze": lambda: platform.bronze(entities, batch_ids),
        "silver": lambda: platform.silver(entities, batch_ids),
        "gold": platform.gold,
        "quality": lambda: platform.quality(tuple(args.suite or ("silver", "gold"))),
        "reconcile": lambda: platform.reconcile(entities),
        "run-all": lambda: platform.run_all(entities),
        "reprocess": lambda: platform.reprocess(
            entities or [], batch_ids, args.since, args.until, args.full_refresh, not args.skip_gold
        ),
        "recover": platform.recover,
        "status": platform.status,
        "runs": lambda: (
            platform.repo.get_run(args.run_id) if args.run_id else platform.repo.list_runs(args.limit)[0]
        ),
        "rejects": lambda: platform.rejects(args.entity, args.reason, args.limit),
    }
    try:
        if args.command != "init":
            platform.repo.init_schema()  # idempotent; keeps one-off commands usable on a fresh database
        return commands[args.command]()
    finally:
        platform.stop()


def main(argv: list[str] | None = None) -> int:
    from lakehouse.metadata.repository import LockNotAcquiredError
    from lakehouse.quality.framework import QualityGateError

    args = build_parser().parse_args(argv)
    # All timestamps are UTC; PySpark would otherwise convert collected values to the host's zone.
    os.environ["TZ"] = "UTC"
    time.tzset()
    configure_logging(args.log_level)
    try:
        result = dispatch(args)
    except QualityGateError as exc:
        log_event(log, "quality_gate_failed", logging.ERROR, failures=[f.rule_id for f in exc.failures])
        print(json.dumps({"status": "QUALITY_GATE_FAILED", "failures": [f.rule_id for f in exc.failures]}))
        return EXIT_QUALITY
    except LockNotAcquiredError as exc:
        log_event(log, "lock_busy", logging.ERROR, error=str(exc))
        return EXIT_LOCKED
    except Exception as exc:
        log.exception("command failed: %s", exc)
        return EXIT_ERROR
    print(json.dumps(result, default=_json_default, indent=2))
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
