from collections import defaultdict
from datetime import UTC, datetime


def get_magnitude_bucket(magnitude: float) -> str:
    """Map an earthquake magnitude to the required reporting bucket."""
    if magnitude < 2:
        return "0-2"
    if magnitude < 4:
        return "2-4"
    if magnitude < 6:
        return "4-6"
    return "6+"


def timestamp_ms_to_utc_date(timestamp_ms: int) -> str:
    """Convert a millisecond timestamp to a UTC date string."""
    event_datetime = datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC)
    return event_datetime.date().isoformat()


def build_daily_aggregates(events: list[dict]) -> list[dict]:
    """Build daily earthquake counts grouped by magnitude bucket."""
    counts = defaultdict(int)

    for event in events:
        magnitude = event.get("magnitude")
        event_time_ms = event.get("event_time_ms")

        # Aggregation requires both magnitude and event time.
        if magnitude is None or event_time_ms is None:
            continue

        event_date = timestamp_ms_to_utc_date(event_time_ms)
        bucket = get_magnitude_bucket(magnitude)
        counts[(event_date, bucket)] += 1

    aggregates = []

    # Sort output so test results and DB inserts stay predictable.
    for (event_date, bucket), count in sorted(counts.items()):
        aggregates.append(
            {
                "event_date_utc": event_date,
                "magnitude_bucket": bucket,
                "event_count": count,
            }
        )

    return aggregates