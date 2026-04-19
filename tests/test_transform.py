from app.transform import (
    build_daily_aggregates,
    get_magnitude_bucket,
    timestamp_ms_to_utc_date,
)


def test_get_magnitude_bucket_boundaries():
    assert get_magnitude_bucket(1.9) == "0-2"
    assert get_magnitude_bucket(2.0) == "2-4"
    assert get_magnitude_bucket(3.99) == "2-4"
    assert get_magnitude_bucket(4.0) == "4-6"
    assert get_magnitude_bucket(5.99) == "4-6"
    assert get_magnitude_bucket(6.0) == "6+"


def test_timestamp_ms_to_utc_date():
    apr_18_2026_midnight_utc_ms = 1776470400000
    assert timestamp_ms_to_utc_date(apr_18_2026_midnight_utc_ms) == "2026-04-18"


def test_build_daily_aggregates():
    apr_18_2026_ms = 1776470400000
    apr_19_2026_ms = 1776556800000

    events = [
        {"magnitude": 1.5, "event_time_ms": apr_18_2026_ms},
        {"magnitude": 2.1, "event_time_ms": apr_18_2026_ms},
        {"magnitude": 4.3, "event_time_ms": apr_18_2026_ms},
        {"magnitude": 6.2, "event_time_ms": apr_18_2026_ms},
        {"magnitude": 2.7, "event_time_ms": apr_19_2026_ms},
        {"magnitude": None, "event_time_ms": apr_19_2026_ms},
        {"magnitude": 1.1, "event_time_ms": None},
    ]

    result, skipped_event_count = build_daily_aggregates(events)

    assert result == [
        {"event_date_utc": "2026-04-18", "magnitude_bucket": "0-2", "event_count": 1},
        {"event_date_utc": "2026-04-18", "magnitude_bucket": "2-4", "event_count": 1},
        {"event_date_utc": "2026-04-18", "magnitude_bucket": "4-6", "event_count": 1},
        {"event_date_utc": "2026-04-18", "magnitude_bucket": "6+", "event_count": 1},
        {"event_date_utc": "2026-04-19", "magnitude_bucket": "2-4", "event_count": 1},
    ]
    assert skipped_event_count == 2