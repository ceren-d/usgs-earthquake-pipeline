import logging

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


def run_pipeline() -> None:
    """Run the earthquake ingestion and aggregation pipeline."""
    configure_logging(LOG_LEVEL)
    logger.info("Starting earthquake pipeline")

    events = fetch_earthquake_events()
    logger.info("Fetched %s events from API", len(events))

    conn = get_connection(DB_PATH)
    init_db(conn)

    insert_raw_events(conn, events)
    logger.info("Inserted %s raw events into SQLite", len(events))

    aggregates = build_daily_aggregates(events)
    refresh_daily_aggregates(conn, aggregates)
    logger.info("Inserted %s daily aggregate rows into SQLite", len(aggregates))

    run_sanity_checks(events, aggregates)

    conn.close()
    logger.info("Earthquake pipeline completed successfully")