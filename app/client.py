import logging
from datetime import UTC, datetime, timedelta

import requests

from app.config import (
    LOOKBACK_DAYS,
    OUTPUT_FORMAT,
    PAGE_LIMIT,
    REQUEST_TIMEOUT,
    USGS_BASE_URL,
)

logger = logging.getLogger(__name__)


def get_date_range() -> tuple[str, str]:
    """Build the UTC date window for the last configured number of days."""
    end_time = datetime.now(UTC)
    start_time = end_time - timedelta(days=LOOKBACK_DAYS)

    start_time_str = start_time.replace(microsecond=0).isoformat()
    end_time_str = end_time.replace(microsecond=0).isoformat()

    return start_time_str, end_time_str


def build_query_params(start_time: str, end_time: str, limit: int, offset: int) -> dict:
    """Build one page of query parameters for the USGS event API."""
    return {
        "format": OUTPUT_FORMAT,
        "starttime": start_time,
        "endtime": end_time,
        "limit": limit,
        "offset": offset,
        "orderby": "time",
    }


def isoformat_utc(timestamp_ms: int | None) -> str | None:
    """Convert a millisecond timestamp to an ISO 8601 UTC datetime string."""
    if timestamp_ms is None:
        return None

    return datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC).replace(microsecond=0).isoformat()


def normalize_feature(feature: dict) -> dict:
    """Normalize a USGS GeoJSON feature into the application's raw event shape."""
    properties = feature.get("properties", {})
    coordinates = feature.get("geometry", {}).get("coordinates", [])

    longitude = coordinates[0] if len(coordinates) > 0 else None
    latitude = coordinates[1] if len(coordinates) > 1 else None
    depth_km = coordinates[2] if len(coordinates) > 2 else None

    # Keep only the fields needed for aggregation, debugging, and practical querying.
    # The full original payload is still preserved in raw_json.
    return {
        "event_id": feature.get("id"),
        "event_time_ms": properties.get("time"),
        "event_time_utc": isoformat_utc(properties.get("time")),
        "updated_time_utc": isoformat_utc(properties.get("updated")),
        "latitude": latitude,
        "longitude": longitude,
        "depth_km": depth_km,
        "magnitude": properties.get("mag"),
        "place": properties.get("place"),
        "mag_type": properties.get("magType"),
        "tsunami": properties.get("tsunami"),
        "event_type": properties.get("type"),
        "status": properties.get("status"),
        "raw_json": feature,
    }


def fetch_earthquake_events(page_limit: int = PAGE_LIMIT) -> list[dict]:
    """Fetch earthquake events from the USGS API with paginated requests."""
    # page_limit is configurable so pagination can be tested with small mocked pages
    # without changing the production default.
    start_time, end_time = get_date_range()

    logger.info("Fetching earthquakes from %s to %s", start_time, end_time)

    all_events = []
    offset = 1
    page_number = 1

    while True:
        params = build_query_params(
            start_time=start_time,
            end_time=end_time,
            limit=page_limit,
            offset=offset,
        )

        logger.info(
            "Requesting page %s with offset=%s limit=%s",
            page_number,
            offset,
            page_limit,
        )

        response = requests.get(
            USGS_BASE_URL,
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()

        payload = response.json()
        features = payload.get("features", [])

        logger.info("Received %s events on page %s", len(features), page_number)

        if not features:
            break

        all_events.extend(normalize_feature(feature) for feature in features)

        # A short page indicates that the result set is exhausted.
        if len(features) < page_limit:
            break

        offset += page_limit
        page_number += 1

    logger.info("Fetched %s total earthquake events", len(all_events))

    return all_events