# Benchmark

## Methodology

`scripts/benchmark.py` runs the complete pipeline on a fresh temporary lake for every execution:

1. generate one drop with a fixed seed (`--scale`, `--files-per-entity`, `--error-rate`, `--store-skew`);
2. start Spark (`--master`, `--shuffle-partitions`, `--driver-memory`);
3. `ingest → bronze → silver → gold → quality → reconcile` through the same `Platform` facade the CLI
   and Airflow use;
4. record wall time per stage, throughput, records per layer, rejects (actual vs. the manifest),
   Gold row counts, quality and reconciliation outcome, and the peak RSS of the process tree
   (Python driver + JVM, sampled every 200 ms with psutil).

Each result is a JSON file in `benchmarks/results/`. Numbers below are copied from those files; nothing
is extrapolated.

Variables you can change:

| Parameter | Flag |
|---|---|
| Data volume | `--scale tiny|small|medium|large` |
| Number of files | `--files-per-entity N` |
| Shuffle partitions | `--shuffle-partitions N` |
| Spark execution | `--master 'local[N]' --driver-memory 4g` |
| Data distribution | `--store-skew S` (Zipf exponent of sales per store) |
| Invalid records | `--error-rate R` (0 = clean) |

```bash
uv run python scripts/benchmark.py --scale small
uv run python scripts/benchmark.py --scale medium --files-per-entity 8 --shuffle-partitions 16 --master 'local[4]'
```

## Results

<!-- filled from benchmarks/results/*.json -->
