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

Measured on 2026-10-23. Environment (from the result files): WSL2 Linux 6.6 on Windows 11,
Intel Core i7-13620H, 8 logical CPUs, 23.5 GiB RAM, Python 3.12.3, PySpark 3.5.7, Delta 3.3.2, Java 17;
local filesystem for Delta and SQLite for metadata. The project's Docker stack was stopped, but 13
containers of other projects kept running on the host, so absolute numbers include that noise.
One run per configuration (no averaging): treat differences under ~10% as noise.

### Configurations

| Run | Scale | Spark | Shuffle partitions | Files / entity | Error rate | Store skew | Source records | Source files | Landing MiB | Lake MiB |
|---|---|---|---|---|---|---|---|---|---|---|
| A | small | local[2], 2g | 8 | 2 | 0.01 | 0 | 10 508 | 16 | 0.7 | 2.6 |
| B | medium | local[2], 4g | 8 | 2 | 0.01 | 0 | 305 714 | 16 | 22.0 | 57.4 |
| C | medium | local[4], 4g | 16 | 8 | 0.01 | 0 | 305 714 | 63 | 22.3 | 60.1 |
| D | medium | local[4], 4g | 16 | 8 | 0 (clean) | 0 | 294 273 | 63 | 20.1 | 56.9 |
| E | medium | local[4], 4g | 16 | 8 | 0.05 | 1.2 | 355 439 | 63 | 31.5 | 70.0 |
| F | large | local[6], 8g | 24 | 8 | 0.01 | 0 | 2 775 365 | 64 | 221.2 | 539.1 |

### Wall time per stage (seconds)

| Run | generate | ingest | bronze | silver | gold | quality | reconcile | ingest→gold |
|---|---|---|---|---|---|---|---|---|
| A | 0.1 | 0.1 | 26.8 | 39.0 | 26.0 | 14.5 | 14.1 | 91.9 |
| B | 2.7 | 0.2 | 30.4 | 46.3 | 27.2 | 15.9 | 17.8 | 104.1 |
| C | 2.6 | 0.6 | 34.5 | 38.7 | 23.0 | 13.3 | 14.0 | 96.7 |
| D | 2.7 | 0.7 | 35.7 | 37.3 | 23.0 | 12.7 | 13.2 | 96.6 |
| E | 3.4 | 0.3 | 33.6 | 41.4 | 21.9 | 13.8 | 14.1 | 97.2 |
| F | 31.0 | 0.4 | 53.0 | 81.6 | 29.0 | 17.3 | 27.1 | 164.0 |

`spark_startup` was 2.4–2.9 s in every run (jars already in the local Ivy cache).

### Throughput, correctness and memory

| Run | Bronze rec/s | Silver rec/s | ingest→gold rec/s | Rejected (actual / expected) | Gold fact rows (= oracle) | Quality | Reconciliation | Peak RSS MiB |
|---|---|---|---|---|---|---|---|---|
| A | 392 | 269 | 114 | 329 / 329 | 3 698 (yes) | 32 passed, 5 warned | 20 passed | 3 026 |
| B | 10 066 | 6 599 | 2 937 | 11 383 / 11 383 | 124 483 (yes) | 32 passed, 5 warned | 20 passed | 4 026 |
| C | 8 853 | 7 902 | 3 160 | 11 383 / 11 383 | 124 483 (yes) | 32 passed, 5 warned | 20 passed | 4 904 |
| D | 8 249 | 7 892 | 3 047 | 0 / 0 | 124 481 (yes) | 37 passed | 20 passed | 4 448 |
| E | 10 566 | 8 593 | 3 656 | 60 951 / 60 951 | 124 409 (yes) | 32 passed, 5 warned | 20 passed | 4 857 |
| F | 52 380 | 34 000 | 16 923 | 114 694 / 114 694 | 1 245 239 (yes) | 32 passed, 5 warned | 20 passed | 6 752 |

Throughput is records written to Bronze (or read by Silver) divided by the stage wall time.

### Findings

* **Fixed overhead dominates up to ~300 k records.** A → B is 29× more data for +13% ingest→gold time.
  Each stage runs dozens of small Spark jobs and Delta commits per entity (8 entities; Silver alone does
  conform + counts + reasons + MERGE + rejects + history per entity), each costing on the order of a second
  locally regardless of size. Throughput numbers at small scale mostly measure that overhead.
* **Volume becomes visible at ~2.8 M records** (F): 9× the data of B for 1.6× the time; Silver (MERGE plus
  two passes over the persisted conformed data) is the most volume-sensitive stage, Gold the least.
* **More cores and partitions** (B → C) changed little at medium scale — consistent with the overhead
  finding; it is not evidence that parallelism is useless at larger volumes.
* **Invalid records** (C → D → E) do not change runtime materially; rejects are produced by the same pass.
* **Skew** (E, Zipf 1.2 on stores): no measurable effect at this volume; the only store-keyed shuffle is
  the daily aggregate, which is small.
* **Correctness held in every configuration**: rejected counts equal the generator's expectation exactly,
  Gold fact rows equal the oracle, and all reconciliation rules passed.

### Limitations of these measurements

* Single machine, Spark local mode, local filesystem: no network or object-storage latency. Against MinIO
  the same pipeline (small scale) took 151 s end to end inside Docker for drop 1 (`make demo`), including
  JVM start per CLI call — not directly comparable with the in-process runs above.
* One run per configuration; the host was shared with other containers.
* CPU utilisation was not sampled; peak RSS is the Python driver + JVM process tree.
* Generation time (31 s for large) is single-threaded Python and not part of the pipeline.

### Reproduce

```bash
uv run python scripts/benchmark.py --scale small                                                          # A
uv run python scripts/benchmark.py --scale medium --master 'local[2]' --shuffle-partitions 8 --files-per-entity 2 --driver-memory 4g   # B
uv run python scripts/benchmark.py --scale medium --master 'local[4]' --shuffle-partitions 16 --files-per-entity 8 --driver-memory 4g  # C
uv run python scripts/benchmark.py --scale medium --master 'local[4]' --shuffle-partitions 16 --files-per-entity 8 --driver-memory 4g --error-rate 0   # D
uv run python scripts/benchmark.py --scale medium --master 'local[4]' --shuffle-partitions 16 --files-per-entity 8 --driver-memory 4g --store-skew 1.2 --error-rate 0.05   # E
uv run python scripts/benchmark.py --scale large --master 'local[6]' --shuffle-partitions 24 --files-per-entity 8 --driver-memory 8g   # F
```

Raw results: `benchmarks/results/2026-10-23T17*.json`.
