# Remediation Plan

This checklist converts repo-review feedback into concrete fixes. Use it before sharing the
project with recruiters or hiring managers.

## Priority 1: Reviewability

Status: in progress

- Keep Python files formatted with Black and import-sorted with isort.
- Run Ruff before every push.
- Keep formatter and linter config in `pyproject.toml` so the repo looks intentional to reviewers.
- Prefer small commits with clear names. A separate formatting commit is useful because it keeps
  future behavioral changes easy to review.

Commands:

```powershell
pip install -e ".[dev]"
black src tests spark_jobs
isort src tests spark_jobs
ruff check . --fix
pytest
```

## Priority 2: Honest Streaming Story

Status: documented

The Spark job currently uses `foreachBatch` plus driver-side `collect()` before writing to Postgres.
That is acceptable for this local WMATA demo because train-position volume is small, but it should be
described as a demo-limited sink pattern.

Production remediation:

- Replace `collect()` with partition-wise writes or a proper JDBC/sink connector.
- Add dead-letter tables or topics for malformed Kafka messages.
- Track batch IDs and sink-write status for idempotent recovery.
- Move long-retention history to analytical storage if volume grows.

## Priority 3: Reliability Metrics

Status: partially implemented

Current dashboard coverage:

- Active trains by line.
- Stale trains by line.
- Location-unavailable train counts.
- Normal and no-passenger train counts.
- Feed health by source.
- Historical line activity.

Recommended next metrics:

- Feed lag in seconds.
- Invalid-record rate.
- Duplicate-event rate.
- Per-line service freshness trend.
- Train-count variance by line over time.
- Station coverage and headway variance once station arrival or ETA feeds are added.

## Priority 4: Demo Benchmark

Status: helper added

Run the V2 stack for a fixed duration and capture measured throughput:

```powershell
.\scripts\benchmark_v2.ps1 -DurationMinutes 30
```

For rehearsal:

```powershell
.\scripts\benchmark_v2.ps1 -DurationSeconds 120
```

Use the result in the README, LinkedIn project section, or resume only after you have actually run
the benchmark locally.

Example wording:

```text
In a 30-minute local demo run, the pipeline ingested X bronze train-position messages, produced Y
typed silver records, and maintained dashboard freshness of Z seconds.
```

## Priority 5: Scope Positioning

Status: documented

Position this project primarily for Data Engineer, Analytics Engineer, Data Platform Engineer, and
Streaming Data Engineer roles. It can support AI/Product roles later if you add forecasting,
anomaly detection, or natural-language reliability summaries, but the current strength is streaming
data infrastructure.
