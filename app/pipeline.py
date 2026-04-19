import logging
from datetime import UTC, datetime

from app.client import fetch_earthquake_events
from app.config import DB_PATH, LOG_LEVEL
from app.db import get_connection, init_db, insert_raw_events, refresh_daily_aggregates
from app.logging_config import configure_logging
from app.transform import build_daily_aggregates

logger = logging.getLogger(__name__)


def run_sanity_checks(events: list[dict], aggregates: list[dict]) -> None:
    """Log lightweight anomaly checks that can help catch silent failures."""
    if not events:
        logger.warning("No earthquake events were fetched for the requested time window.")

    if not aggregates:
        logger.warning("No aggregate rows were produced from the fetched events.")

    missing_magnitude_count = sum(1 for event in events if event.get("magnitude") is None)
    if events and missing_magnitude_count / len(events) > 0.05:
        logger.warning(
            "More than 5%% of fetched events are missing magnitude (%s of %s).",
            missing_magnitude_count,
            len(events),
        )

    valid_raw_event_count = sum(
        1
        for event in events
        if event.get("magnitude") is not None and event.get("event_time_ms") is not None
    )
    aggregate_total = sum(aggregate["event_count"] for aggregate in aggregates)

    if aggregate_total != valid_raw_event_count:
        logger.warning(
            "Aggregate total (%s) does not match valid raw event count (%s).",
            aggregate_total,
            valid_raw_event_count,
        )


def log_aggregate_day_highlights(aggregates: list[dict]) -> None:
    """Log labeled daily aggregate highlights for visibility into bucketing."""
    if not aggregates:
        return

    totals_by_day = {}
    bucket_counts_by_day = {}

    for row in aggregates:
        event_date = row["event_date_utc"]
        bucket = row["magnitude_bucket"]
        count = row["event_count"]

        totals_by_day[event_date] = totals_by_day.get(event_date, 0) + count

        if event_date not in bucket_counts_by_day:
            bucket_counts_by_day[event_date] = {}
        bucket_counts_by_day[event_date][bucket] = count

    sorted_days = sorted(totals_by_day)
    earliest_day = sorted_days[0]
    latest_day = sorted_days[-1]
    busiest_day = max(totals_by_day, key=totals_by_day.get)
    quietest_day = min(totals_by_day, key=totals_by_day.get)

    labels_by_day = {}
    for label, day in (
        ("earliest", earliest_day),
        ("latest", latest_day),
        ("busiest", busiest_day),
        ("quietest", quietest_day),
    ):
        labels_by_day.setdefault(day, []).append(label)

    ordered_days = []
    for day in (earliest_day, latest_day, busiest_day, quietest_day):
        if day not in ordered_days:
            ordered_days.append(day)

    parts = []
    for day in ordered_days:
        labels = "/".join(labels_by_day[day])
        total = totals_by_day[day]
        bucket_counts = bucket_counts_by_day[day]

        bucket_summary = ", ".join(
            f"{bucket}={bucket_counts.get(bucket, 0)}"
            for bucket in ("0-2", "2-4", "4-6", "6+")
        )

        parts.append(f"{labels}={day} (total={total}) [{bucket_summary}]")

    logger.info("Aggregate day highlights: %s", "; ".join(parts))


def run_pipeline() -> None:
    """Run the earthquake ingestion and aggregation pipeline."""
    configure_logging(LOG_LEVEL)
    run_started_at = datetime.now(UTC)
    conn = None
    current_stage = "starting"

    try:
        logger.info("Starting earthquake pipeline")

        current_stage = "fetching earthquake events"
        events = fetch_earthquake_events()
        logger.info("Fetched %s events from API", len(events))

        current_stage = "opening database connection"
        conn = get_connection(DB_PATH)

        current_stage = "initializing database schema"
        init_db(conn)

        current_stage = "inserting raw events"
        insert_raw_events(conn, events)
        logger.info("Inserted %s raw events into SQLite", len(events))

        current_stage = "building daily aggregates"
        aggregates, skipped_event_count = build_daily_aggregates(events)

        current_stage = "refreshing aggregate table"
        refresh_daily_aggregates(conn, aggregates)
        logger.info("Inserted %s daily aggregate rows into SQLite", len(aggregates))
        logger.info(
            "Skipped %s events during aggregation due to missing magnitude or event time",
            skipped_event_count,
        )

        current_stage = "logging aggregate highlights"
        log_aggregate_day_highlights(aggregates)

        current_stage = "running sanity checks"
        run_sanity_checks(events, aggregates)

        runtime_seconds = (datetime.now(UTC) - run_started_at).total_seconds()
        logger.info("Earthquake pipeline completed successfully in %.2f seconds", runtime_seconds)

    except Exception:
        runtime_seconds = (datetime.now(UTC) - run_started_at).total_seconds()
        logger.exception(
            "Earthquake pipeline failed during '%s' after %.2f seconds",
            current_stage,
            runtime_seconds,
        )
        raise

    finally:
        if conn is not None:
            conn.close()