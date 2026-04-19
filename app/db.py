import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path


def get_connection(db_path: Path) -> sqlite3.Connection:
    """Open a SQLite connection for the application database."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    """Create the tables used by the pipeline if they do not already exist."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_earthquakes (
            event_id TEXT PRIMARY KEY,
            event_time_utc TEXT NOT NULL,
            updated_time_utc TEXT,
            latitude REAL,
            longitude REAL,
            depth_km REAL,
            magnitude REAL,
            place TEXT,
            mag_type TEXT,
            tsunami INTEGER,
            event_type TEXT,
            status TEXT,
            raw_json TEXT NOT NULL,
            ingested_at_utc TEXT NOT NULL
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_earthquake_counts (
            event_date_utc TEXT NOT NULL,
            magnitude_bucket TEXT NOT NULL,
            event_count INTEGER NOT NULL,
            computed_at_utc TEXT NOT NULL,
            PRIMARY KEY (event_date_utc, magnitude_bucket)
        )
        """
    )

    conn.commit()


def insert_raw_events(conn: sqlite3.Connection, events: list[dict]) -> None:
    """Insert or replace raw earthquake events in SQLite."""
    ingested_at_utc = datetime.now(UTC).replace(microsecond=0).isoformat()

    rows = []
    for event in events:
        rows.append(
            (
                event.get("event_id"),
                event.get("event_time_utc"),
                event.get("updated_time_utc"),
                event.get("latitude"),
                event.get("longitude"),
                event.get("depth_km"),
                event.get("magnitude"),
                event.get("place"),
                event.get("mag_type"),
                event.get("tsunami"),
                event.get("event_type"),
                event.get("status"),
                json.dumps(event.get("raw_json")),
                ingested_at_utc,
            )
        )

    conn.executemany(
        """
        INSERT OR REPLACE INTO raw_earthquakes (
            event_id,
            event_time_utc,
            updated_time_utc,
            latitude,
            longitude,
            depth_km,
            magnitude,
            place,
            mag_type,
            tsunami,
            event_type,
            status,
            raw_json,
            ingested_at_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )

    conn.commit()


def refresh_daily_aggregates(conn: sqlite3.Connection, aggregates: list[dict]) -> None:
    """Replace aggregate rows with a fresh daily snapshot."""
    # This keeps reruns simple and avoids accumulating stale aggregate rows.
    conn.execute("DELETE FROM daily_earthquake_counts")

    computed_at_utc = datetime.now(UTC).replace(microsecond=0).isoformat()

    rows = []
    for aggregate in aggregates:
        rows.append(
            (
                aggregate["event_date_utc"],
                aggregate["magnitude_bucket"],
                aggregate["event_count"],
                computed_at_utc,
            )
        )

    conn.executemany(
        """
        INSERT INTO daily_earthquake_counts (
            event_date_utc,
            magnitude_bucket,
            event_count,
            computed_at_utc
        )
        VALUES (?, ?, ?, ?)
        """,
        rows,
    )

    conn.commit()