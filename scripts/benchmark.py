"""Reproducible end-to-end benchmark: generate -> ingest -> bronze -> silver -> gold -> quality -> reconcile.

Every run starts from an empty temporary lake, uses a fixed seed and records the environment, the
parameters, per-stage wall time, throughput, rejects, published rows, reconciliation outcome and the
peak resident memory of the process tree (Python driver + JVM). Results go to
``benchmarks/results/<timestamp>-<scale>.json``; nothing is extrapolated.

Examples::

    uv run python scripts/benchmark.py --scale small
    uv run python scripts/benchmark.py --scale medium --files-per-entity 8 --shuffle-partitions 16 --master 'local[4]'
    uv run python scripts/benchmark.py --scale small --error-rate 0 --store-skew 1.2
"""

from __future__ import annotations

import argparse
import json
import os
import platform as host
import shutil
import sys
import tempfile
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psutil

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from lakehouse.common.storage import LocalStore  # noqa: E402
from lakehouse.config import Settings  # noqa: E402
from lakehouse.generator.retail import GeneratorConfig, generate  # noqa: E402
from lakehouse.metadata.repository import MetadataRepository  # noqa: E402
from lakehouse.observability.logging import configure_logging  # noqa: E402
from lakehouse.pipelines.runner import Platform  # noqa: E402
from lakehouse.quality.framework import QualityGateError  # noqa: E402


class PeakMemory:
    """Samples RSS of this process and its children (the JVM) every 200 ms."""

    def __init__(self) -> None:
        self.peak = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        me = psutil.Process()
        while not self._stop.is_set():
            try:
                total = me.memory_info().rss + sum(c.memory_info().rss for c in me.children(recursive=True))
                self.peak = max(self.peak, total)
            except psutil.Error:
                pass
            time.sleep(0.2)

    def __enter__(self) -> PeakMemory:
        self._thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self._stop.set()
        self._thread.join()


def timed(stages: dict[str, float], name: str, fn: Callable[[], Any]) -> Any:
    start = time.perf_counter()
    try:
        return fn()
    finally:
        stages[name] = round(time.perf_counter() - start, 3)


def environment() -> dict[str, Any]:
    import pyspark

    cpu = "unknown"
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                break
    except OSError:
        pass
    return {
        "host": host.platform(),
        "cpu_model": cpu,
        "logical_cpus": os.cpu_count(),
        "ram_gib": round(psutil.virtual_memory().total / 2**30, 1),
        "python": host.python_version(),
        "pyspark": pyspark.__version__,
        "storage": "local filesystem (Delta) + SQLite metadata",
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    workdir = Path(tempfile.mkdtemp(prefix="lakehouse-bench-"))
    settings = Settings.model_validate(
        {
            "storage": {"landing_uri": str(workdir / "landing"), "lake_uri": str(workdir / "lake")},
            "metadata": {"url": f"sqlite:///{workdir / 'metadata.db'}"},
            "spark": {
                "master": args.master,
                "shuffle_partitions": args.shuffle_partitions,
                "driver_memory": args.driver_memory,
            },
        }
    )
    repo = MetadataRepository.from_url(settings.metadata.url)
    platform = Platform(settings, repo, trigger="benchmark")
    stages: dict[str, float] = {}
    outcome: dict[str, Any] = {}
    try:
        with PeakMemory() as memory:
            platform.init()
            config = GeneratorConfig(seed=args.seed, scale=args.scale, files_per_entity=args.files_per_entity,
                                     error_rate=args.error_rate, store_skew=args.store_skew)  # fmt: skip
            manifest = timed(
                stages, "generate", lambda: generate(LocalStore(), settings.storage.landing_uri, config)
            )
            timed(stages, "spark_startup", lambda: platform.spark)
            timed(stages, "ingest", platform.ingest)
            bronze = timed(stages, "bronze", platform.bronze)
            silver = timed(stages, "silver", platform.silver)
            gold = timed(stages, "gold", platform.gold)
            try:
                quality = timed(stages, "quality", platform.quality)
                outcome["quality"] = quality["results"]["by_status"]
            except QualityGateError as exc:
                outcome["quality"] = {"blocking_failures": [f.rule_id for f in exc.failures]}
            try:
                reconcile = timed(stages, "reconcile", platform.reconcile)
                outcome["reconciliation"] = reconcile["results"]["by_status"]
            except QualityGateError as exc:
                outcome["reconciliation"] = {"blocking_failures": [f.rule_id for f in exc.failures]}
        bronze_rows = sum(b["records"] for b in bronze["batches"])
        silver_read = sum(b["records_read"] for b in silver["batches"])
        rejected = sum(b["rejected"] for b in silver["batches"])
        lake_bytes = sum(p.stat().st_size for p in (workdir / "lake").rglob("*") if p.is_file())
        landing_bytes = sum(p.stat().st_size for p in (workdir / "landing").rglob("*") if p.is_file())
        pipeline_seconds = sum(stages[s] for s in ("ingest", "bronze", "silver", "gold"))
        return {
            "timestamp": datetime.now(UTC).isoformat(timespec="seconds"),
            "parameters": vars(args),
            "environment": environment(),
            "volume": {
                "source_files": len(manifest["files"]),
                "source_records": sum(f["rows"] for f in manifest["files"]),
                "landing_bytes": landing_bytes,
                "lake_bytes": lake_bytes,
                "sizes": manifest["sizes"],
            },
            "stages_seconds": stages,
            "throughput_records_per_second": {
                "bronze": round(bronze_rows / stages["bronze"], 1),
                "silver": round(silver_read / stages["silver"], 1),
                "ingest_to_gold": round(bronze_rows / pipeline_seconds, 1),
            },
            "records": {
                "bronze": bronze_rows,
                "silver_read": silver_read,
                "silver_rejected": rejected,
                "expected_rejected": len(manifest["expected"]["rejections"]),
                "gold_rows": gold["row_counts"],
            },
            "gold_matches_oracle": {
                "fact_rows": gold["row_counts"]["gold.fact_sales_item"]
                == manifest["expected"]["gold"]["fact_rows"],
            },
            "outcome": outcome,
            "peak_rss_mib": round(memory.peak / 2**20, 1),
        }
    finally:
        platform.stop()
        if not args.keep:
            shutil.rmtree(workdir, ignore_errors=True)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--scale", default="small", choices=("tiny", "small", "medium", "large"))
    parser.add_argument("--files-per-entity", type=int, default=2)
    parser.add_argument("--shuffle-partitions", type=int, default=8)
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--driver-memory", default="2g")
    parser.add_argument("--error-rate", type=float, default=0.01)
    parser.add_argument("--store-skew", type=float, default=0.0)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--keep", action="store_true", help="keep the temporary lake for inspection")
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()
    os.environ["TZ"] = "UTC"
    time.tzset()
    configure_logging("WARNING")
    result = run(args)
    output = (
        args.output
        or ROOT / "benchmarks" / "results" / f"{result['timestamp'].replace(':', '')}-{args.scale}.json"
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, default=str) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(output), "stages_seconds": result["stages_seconds"],
                      "throughput": result["throughput_records_per_second"], "peak_rss_mib": result["peak_rss_mib"],
                      "outcome": result["outcome"]}, indent=2))  # fmt: skip
    return 0


if __name__ == "__main__":
    sys.exit(main())
