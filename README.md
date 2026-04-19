# USGS Earthquake Pipeline

A Python application that fetches earthquake data from the USGS API for the past 30 days, handles paginated API responses, calculates daily earthquake counts by magnitude bucket, stores both raw events and daily aggregates in SQLite, and includes tests that do not require a live API connection.

## Requirements

- Python 3.11+
- pip

## Setup

Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

## Run the pipeline

From the project root:

```bash
python main.py
```

This will:

- fetch earthquake event data from the USGS API for the last 30 days
- handle pagination across multiple API requests
- store raw event data in `earthquakes.db`
- calculate daily aggregate counts by magnitude bucket
- store aggregate results in SQLite

## Run tests

Run the full test suite:

```bash
python -m pytest
```

Or run individual test files:

```bash
python -m pytest tests/test_transform.py
python -m pytest tests/test_client.py
```

## Database output

The pipeline creates a SQLite database file in the project root:

```text
earthquakes.db
```

It contains two tables:

### `raw_earthquakes`

Stores normalized raw earthquake event records along with the full original source payload in `raw_json`.

### `daily_earthquake_counts`

Stores daily earthquake counts grouped into these magnitude buckets:

- `0-2`
- `2-4`
- `4-6`
- `6+`

## Design decisions

### Raw and aggregate storage

I stored both raw event data and aggregated daily counts in SQLite.

This keeps the raw source data available for debugging and reprocessing, while also producing a clean summary table that directly answers the reporting requirement.

### Field selection

I intentionally flattened only a focused subset of fields from the USGS response into explicit columns used for aggregation and debugging. The full original event payload is still preserved in `raw_json`, which keeps the schema manageable without losing source fidelity.

### Pagination

The client fetches multiple pages of results using `limit` and `offset` until it reaches a final short page. This ensures the pipeline retrieves the full result set within the requested 30-day window.

### Testability

The client supports a configurable page size so pagination behavior can be tested with small mocked responses without changing the production default. The tests use mocked API responses, so they do not depend on live network access or the current state of the USGS API.

### Logging and sanity checks

The pipeline logs key execution steps including:

- pipeline start and completion
- page requests
- records returned per page
- total records fetched
- rows inserted into SQLite
- skipped records during aggregation
- representative daily aggregate highlights
- total runtime

Representative daily aggregate highlights are logged to make the bucketing output visible without dumping the full aggregate table.

In addition to standard logging, the pipeline runs lightweight post-run sanity checks to help catch silent failures or unusual outputs, such as empty fetches, empty aggregates, unusually high missing magnitude rates, or mismatches between valid raw event counts and aggregate totals.

## What I would do differently in production

If this were running as a production scheduled job, I would likely add:

- retry and backoff logic for transient API failures
- stronger data quality checks and warning thresholds
- historical run metrics such as runtime, fetched row counts, skipped row counts, and failure counts
- reconciliation monitoring between valid raw event counts and aggregate totals
- source payload drift checks to detect missing or changed fields in the USGS response
- freshness monitoring to detect stale runs or unexpectedly old source data
- alerting or dashboarding in a tool such as Grafana around unusual drops in fetched volume, missing dates, high null rates, stale data, or reconciliation gaps
- a scheduled execution environment such as cron or a workflow orchestrator

## Project structure

```text
usgs-earthquake-pipeline/
  app/
    __init__.py
    client.py
    config.py
    db.py
    logging_config.py
    pipeline.py
    transform.py
  tests/
    test_client.py
    test_transform.py
  main.py
  requirements.txt
  README.md
```