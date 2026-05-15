# Real-Time WMATA Metro Reliability Pipeline

[![CI](https://github.com/atulk1000/real-time-transit-reliability/actions/workflows/ci.yml/badge.svg)](https://github.com/atulk1000/real-time-transit-reliability/actions/workflows/ci.yml)

Production-style data engineering project that turns WMATA Metrorail API feeds into a live train board and historical reliability dashboard.

The project has two runnable paths:

- **Stage 1 MVP:** WMATA APIs -> Postgres bronze/silver/gold tables -> Streamlit.
- **V2 Streaming:** WMATA APIs -> Redpanda/Kafka topics -> Spark Structured Streaming -> Postgres -> Streamlit.

## What It Shows

- Active trains grouped by WMATA line color.
- Current station-relative train location.
- Next station, origin, destination, direction, car count, service status, and freshness.
- Feed health and record counts.
- Historical line activity metrics in a clickable dashboard tab.

The project intentionally skips ETA and incident feeds for now so the core streaming/data-modeling path stays focused.

## Architecture

### Stage 1 MVP

```text
WMATA Rail APIs
  -> Python poller
  -> Postgres bronze JSONB tables
  -> Silver typed tables
  -> Gold current train board
  -> Streamlit / FastAPI
```

### V2 Streaming

```text
WMATA Rail APIs
  -> Python Kafka producer
  -> Redpanda topics
  -> Spark Structured Streaming
  -> Postgres bronze/silver/gold tables
  -> Streamlit dashboard
```

## Production-Style vs. Demo-Limited

This repo is designed as a portfolio-grade local streaming system, not a claim that WMATA-scale
data needs a large cluster.

Production-style pieces:

- Source-specific Kafka topics for train positions, lines, stations, and route circuits.
- Bronze JSONB retention, typed silver tables, and dashboard-ready gold tables.
- Postgres partitioning on the highest-growth bronze table.
- Spark Structured Streaming checkpoints for replayable local streams.
- Feed-health and historical activity tables for operational monitoring.
- Unit and integration tests for transforms, API behavior, Kafka envelopes, topics, and sinks.

Demo-limited pieces:

- Spark `foreachBatch` currently collects each micro-batch to the driver before writing to Postgres.
  That is acceptable for a small WMATA local demo, but it is not the scalable sink pattern to use for
  high-volume streaming data.
- The dashboard focuses on train positions and reference data. ETA and incident feeds are excluded
  intentionally to keep the first public version focused and reviewable.
- The default Docker Compose setup runs one local Spark worker and one local Redpanda broker.

Production upgrade path:

- Replace driver-side `collect()` writes with partition-wise writes, a JDBC sink strategy, or a
  dedicated sink connector.
- Add idempotent batch metadata and dead-letter handling for malformed records.
- Move long-term history to partitioned analytical storage such as Delta/Iceberg or warehouse tables.
- Add alerting on feed lag, invalid records, duplicate rates, and stale-train thresholds.
- Scale Kafka partitions and Spark executors based on measured throughput, not guesswork.

## Data Sources

Required WMATA API products:

- **Train Positions**
- **Rail Station Information**

Endpoints used:

```text
/TrainPositions/TrainPositions?contentType=json
/TrainPositions/StandardRoutes?contentType=json
/Rail.svc/json/jStations
/Rail.svc/json/jLines
```

WMATA developer links:

- [WMATA Developer Portal](https://developer.wmata.com/)
- [WMATA Developer Resources](https://www.wmata.com/about/developers/)
- [Train Positions FAQ](https://developer.wmata.com/trainpositionsfaq)

## Setup

Prerequisites:

- Docker Desktop
- Python 3.10+
- WMATA API keys for Train Positions and Rail Station Information

Create the local environment:

```powershell
git clone https://github.com/<your-username>/real-time-transit-reliability.git
cd real-time-transit-reliability
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
copy .env.example .env
```

Edit `.env`:

```text
WMATA_TRAIN_POSITIONS_API_KEY=your_train_positions_key
WMATA_RAIL_STATION_API_KEY=your_rail_station_information_key
WMATA_VERIFY_SSL=true
DATABASE_URL=postgresql://transit:transit@localhost:55432/transit_reliability
KAFKA_BOOTSTRAP_SERVERS=localhost:19092
```

If Python raises a local certificate verification error on Windows, prefer fixing the Python certificate store. For quick local-only testing, set:

```text
WMATA_VERIFY_SSL=false
```

Do not commit `.env`.

## Demo Option 1: Mostly Dockerized V2

Best for showing the full streaming architecture.

```powershell
docker compose up -d --build postgres redpanda redpanda-init redpanda-console spark-reference-stream spark-train-stream dashboard
python -m transit_reliability.db_migrate
docker compose up producer
```

Open:

```text
Streamlit dashboard: http://127.0.0.1:8501
Redpanda Console:   http://127.0.0.1:8080
```

The producer runs continuously until stopped with `CTRL+C`.

Stop the stack:

```powershell
docker compose down
```

## Demo Option 2: Manual / Easier To Debug

Best while developing because producer and dashboard errors show directly in PowerShell.

Start infrastructure and Spark streams:

```powershell
.\scripts\demo_v2_infra.ps1
```

Start the producer in another terminal:

```powershell
.\scripts\demo_v2_producer.ps1
```

Start Streamlit in another terminal:

```powershell
streamlit run src\transit_reliability\dashboard\app.py
```

Check row counts:

```powershell
.\scripts\check_counts.ps1
```

Run a timed benchmark:

```powershell
.\scripts\benchmark_v2.ps1 -DurationMinutes 30
```

For a quick demo rehearsal, use a shorter run:

```powershell
.\scripts\benchmark_v2.ps1 -DurationSeconds 120
```

## Stage 1 Quick Demo

Fastest product demo without Kafka/Spark:

```powershell
.\scripts\demo_stage1.ps1
```

This runs one WMATA ingestion cycle and opens the dashboard.

## Kafka Topics

```text
wmata.train_positions.raw       3 partitions, key=train_id
wmata.lines.raw                 1 partition
wmata.stations.raw              1 partition
wmata.standard_routes.raw       1 partition
```

Train positions are published as one message per train so per-train ordering is preserved by the `train_id` key.

## Database Design

Bronze tables store raw API responses as JSONB:

```text
bronze_wmata_train_positions
bronze_wmata_standard_routes
bronze_wmata_stations
bronze_wmata_lines
```

The high-growth train-position bronze table is monthly partitioned by `fetched_at`. A default partition is included so local demos do not fail when a future month has not been created.

Silver tables hold typed, queryable records:

```text
silver_train_position_events
silver_station_reference
silver_line_reference
silver_route_circuits
```

Gold tables power the dashboard:

```text
gold_current_train_board
gold_feed_health
gold_line_activity_history
gold_feed_health_history
```

Apply schema updates to an existing database:

```powershell
python -m transit_reliability.db_migrate
```

## Dashboard

Streamlit tabs:

- **Current Train Board**
- **Historical Metrics**
- **Feed Health**

The historical tab is populated by the V2 Spark train-position stream.

See [docs/demo_run.md](docs/demo_run.md) for a captured local run with row counts, benchmark
output, dashboard screenshots, Redpanda topic evidence, and Postgres gold-table query evidence.

## Reliability Metrics

Current metrics:

- Active trains by line.
- Stale trains by line.
- Location-unavailable train count.
- Normal vs. no-passenger train count.
- Feed health by source.
- Historical line activity windows.

Next metrics to add:

- Feed lag in seconds.
- Invalid-record rate.
- Duplicate-event rate.
- Per-line service freshness trend.
- Train count variance by line over time.
- Station coverage and headway variance when the project adds station arrival or ETA feeds.

## Tests

Run unit tests:

```powershell
pytest
```

Run formatting and lint checks:

```powershell
black --check src tests spark_jobs
isort --check-only src tests spark_jobs
ruff check .
```

Run full Postgres integration checks against a disposable/resettable local database:

```powershell
$env:TEST_DATABASE_URL="postgresql://transit:transit@localhost:55432/transit_reliability"
pytest
```

Warning: integration tests truncate project tables before running.

## Repo Layout

```text
configs/        WMATA and Kafka topic configuration
docker/         Python and Spark Dockerfiles
scripts/        Demo and diagnostics helpers
spark_jobs/     Spark Structured Streaming entrypoints
sql/            Postgres schema
src/            Application, ingestion, streaming, API, dashboard code
tests/          Unit and integration tests
```

