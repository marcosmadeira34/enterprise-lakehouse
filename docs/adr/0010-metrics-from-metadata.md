# 0010 — Metrics derived from metadata (pull), no Pushgateway

## Context
Pipeline steps are short-lived processes; Prometheus scrapes long-lived targets. The usual answer is a
Pushgateway, which adds a component and loses metrics when a job dies before pushing.

## Decision
The API exposes `/metrics`, computed on each scrape from the operational metadata (runs, batch layers,
files, latest quality result per rule, publication age). The metadata database is already the record of
truth and is written transactionally by every step, including failed ones.

## Consequences
* Metrics are as fresh as the metadata (no lag beyond the scrape interval).
* Only aggregated, non-sensitive values are exposed (no record contents).
* Per-scrape cost is a handful of grouped queries; fine at this scale, cacheable if needed.
