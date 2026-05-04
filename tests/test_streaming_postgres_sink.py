from datetime import datetime, timezone

from transit_reliability.streaming.postgres_sink import parse_timestamp


def test_parse_timestamp_normalizes_to_utc() -> None:
    parsed = parse_timestamp("2026-05-04T00:15:30+00:00")

    assert parsed == datetime(2026, 5, 4, 0, 15, 30, tzinfo=timezone.utc)

