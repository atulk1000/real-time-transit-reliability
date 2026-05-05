from datetime import datetime, timedelta, timezone

from transit_reliability.transforms.refresh import (
    event_hash,
    freshness_status,
    infer_origin,
    validate_train_position,
)


def test_event_hash_is_stable_and_distinguishes_nulls() -> None:
    assert event_hash("batch-1", "train-1", None) == event_hash("batch-1", "train-1", None)
    assert event_hash("batch-1", "train-1", None) != event_hash("batch-1", "train-1", "")
    assert event_hash("batch-1", "train-1", None) != event_hash("batch-1", "train-2", None)


def test_validate_train_position_accepts_required_fields() -> None:
    is_valid, error = validate_train_position({"TrainId": "123", "CircuitId": 456})

    assert is_valid is True
    assert error is None


def test_validate_train_position_rejects_missing_train_id() -> None:
    is_valid, error = validate_train_position({"CircuitId": 456})

    assert is_valid is False
    assert error == "missing TrainId"


def test_validate_train_position_rejects_missing_circuit_id() -> None:
    is_valid, error = validate_train_position({"TrainId": "123"})

    assert is_valid is False
    assert error == "missing CircuitId"


def test_infer_origin_uses_opposite_terminal_when_destination_is_terminal() -> None:
    line = {"start_station_code": "A01", "end_station_code": "A15"}

    assert infer_origin(line, "A15") == "A01"
    assert infer_origin(line, "A01") == "A15"


def test_infer_origin_returns_none_for_non_terminal_or_missing_line() -> None:
    line = {"start_station_code": "A01", "end_station_code": "A15"}

    assert infer_origin(line, "A07") is None
    assert infer_origin(None, "A15") is None


def test_freshness_status_marks_recent_records_fresh() -> None:
    recent = datetime.now(timezone.utc) - timedelta(seconds=10)

    assert freshness_status(recent, stale_after_seconds=45) == "fresh"


def test_freshness_status_marks_old_records_stale() -> None:
    old = datetime.now(timezone.utc) - timedelta(seconds=90)

    assert freshness_status(old, stale_after_seconds=45) == "stale"
