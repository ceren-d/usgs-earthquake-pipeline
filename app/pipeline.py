import logging

from app.client import fetch_earthquake_events
from app.config import DB_PATH, LOG_LEVEL
from app.db import get_connection, init_db, insert_raw_events, refresh_daily_aggregates
from app.logging_config import configure_logging
from app.transform import build_daily_aggregates

logger = logging.getLogger(__name__)


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

    conn.close()
    logger.info("Earthquake pipeline completed successfully")