from pathlib import Path

from app.db import (
    get_connection,
    init_db,
    insert_raw_events,
    refresh_daily_aggregates,
)


def test_init_db_creates_tables(tmp_path: Path):
    db_path = tmp_path / "test_earthquakes.db"
    conn = get_connection(db_path)

    init_db(conn)

    tables = {
        row["name"]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }

    assert "raw_earthquakes" in tables
    assert "daily_earthquake_counts" in tables

    conn.close()


def test_insert_raw_events_inserts_rows(tmp_path: Path):
    db_path = tmp_path / "test_earthquakes.db"
    conn = get_connection(db_path)
    init_db(conn)

    events = [
        {
            "event_id": "event-1",
            "event_time_utc": "2026-04-18T00:00:00+00:00",
            "updated_time_utc": "2026-04-18T00:01:00+00:00",
            "latitude": 37.1,
            "longitude": -122.1,
            "depth_km": 10.0,
            "magnitude": 1.5,
            "place": "Place 1",
            "mag_type": "ml",
            "tsunami": 0,
            "event_type": "earthquake",
            "status": "reviewed",
            "raw_json": {"id": "event-1"},
        },
        {
            "event_id": "event-2",
            "event_time_utc": "2026-04-19T00:00:00+00:00",
            "updated_time_utc": "2026-04-19T00:01:00+00:00",
            "latitude": 38.2,
            "longitude": -123.2,
            "depth_km": 12.0,
            "magnitude": 2.8,
            "place": "Place 2",
            "mag_type": "mb",
            "tsunami": 1,
            "event_type": "earthquake",
            "status": "automatic",
            "raw_json": {"id": "event-2"},
        },
    ]

    insert_raw_events(conn, events)

    row_count = conn.execute("SELECT COUNT(*) AS count FROM raw_earthquakes").fetchone()["count"]
    stored_row = conn.execute(
        "SELECT event_id, magnitude, place FROM raw_earthquakes WHERE event_id = ?",
        ("event-1",),
    ).fetchone()

    assert row_count == 2
    assert stored_row["event_id"] == "event-1"
    assert stored_row["magnitude"] == 1.5
    assert stored_row["place"] == "Place 1"

    conn.close()


def test_refresh_daily_aggregates_replaces_previous_rows(tmp_path: Path):
    db_path = tmp_path / "test_earthquakes.db"
    conn = get_connection(db_path)
    init_db(conn)

    first_aggregates = [
        {
            "event_date_utc": "2026-04-18",
            "magnitude_bucket": "0-2",
            "event_count": 5,
        },
        {
            "event_date_utc": "2026-04-18",
            "magnitude_bucket": "2-4",
            "event_count": 3,
        },
    ]

    second_aggregates = [
        {
            "event_date_utc": "2026-04-19",
            "magnitude_bucket": "4-6",
            "event_count": 2,
        }
    ]

    refresh_daily_aggregates(conn, first_aggregates)
    refresh_daily_aggregates(conn, second_aggregates)

    rows = conn.execute(
        """
        SELECT event_date_utc, magnitude_bucket, event_count
        FROM daily_earthquake_counts
        ORDER BY event_date_utc, magnitude_bucket
        """
    ).fetchall()

    assert len(rows) == 1
    assert rows[0]["event_date_utc"] == "2026-04-19"
    assert rows[0]["magnitude_bucket"] == "4-6"
    assert rows[0]["event_count"] == 2

    conn.close()