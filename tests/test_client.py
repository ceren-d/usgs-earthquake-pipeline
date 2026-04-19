from app.client import fetch_earthquake_events


def test_fetch_earthquake_events_paginates(requests_mock):
    apr_18_event_1_ms = 1776470400000
    apr_18_event_2_ms = 1776471400000
    apr_19_event_1_ms = 1776556800000

    page_1 = {
        "features": [
            {
                "id": "event-1",
                "properties": {
                    "time": apr_18_event_1_ms,
                    "updated": apr_18_event_1_ms,
                    "mag": 1.5,
                    "place": "Place 1",
                    "magType": "ml",
                    "tsunami": 0,
                    "type": "earthquake",
                    "status": "reviewed",
                },
                "geometry": {
                    "coordinates": [-122.1, 37.1, 10.0],
                },
            },
            {
                "id": "event-2",
                "properties": {
                    "time": apr_18_event_2_ms,
                    "updated": apr_18_event_2_ms,
                    "mag": 2.8,
                    "place": "Place 2",
                    "magType": "mb",
                    "tsunami": 1,
                    "type": "earthquake",
                    "status": "automatic",
                },
                "geometry": {
                    "coordinates": [-123.2, 38.2, 12.0],
                },
            },
        ]
    }

    page_2 = {
        "features": [
            {
                "id": "event-3",
                "properties": {
                    "time": apr_19_event_1_ms,
                    "updated": apr_19_event_1_ms,
                    "mag": 4.2,
                    "place": "Place 3",
                    "magType": "mw",
                    "tsunami": 0,
                    "type": "earthquake",
                    "status": "reviewed",
                },
                "geometry": {
                    "coordinates": [-124.3, 39.3, 15.0],
                },
            }
        ]
    }

    requests_mock.get(
        "https://earthquake.usgs.gov/fdsnws/event/1/query",
        [
            {"json": page_1, "status_code": 200},
            {"json": page_2, "status_code": 200},
        ],
    )

    events = fetch_earthquake_events(page_limit=2)

    assert len(events) == 3

    assert events[0]["event_id"] == "event-1"
    assert events[0]["magnitude"] == 1.5
    assert events[0]["latitude"] == 37.1
    assert events[0]["longitude"] == -122.1
    assert events[0]["depth_km"] == 10.0
    assert events[0]["place"] == "Place 1"
    assert events[0]["event_type"] == "earthquake"

    assert events[2]["event_id"] == "event-3"
    assert events[2]["magnitude"] == 4.2
    assert events[2]["latitude"] == 39.3
    assert events[2]["longitude"] == -124.3
    assert events[2]["depth_km"] == 15.0