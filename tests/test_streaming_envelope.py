from datetime import datetime, timezone

from transit_reliability.streaming.envelope import snapshot_message, train_position_messages


def test_train_position_messages_explode_snapshot_to_one_message_per_train() -> None:
    messages = train_position_messages(
        endpoint="/TrainPositions/TrainPositions",
        params={"contentType": "json"},
        fetched_at=datetime(2026, 5, 4, tzinfo=timezone.utc),
        ingestion_batch_id="batch-1",
        response_status=200,
        payload={
            "TrainPositions": [
                {"TrainId": "336", "TrainNumber": "407", "LineCode": "BL"},
                {"TrainId": "317", "TrainNumber": "409", "LineCode": "BL"},
            ]
        },
    )

    assert len(messages) == 2
    assert messages[0].topic == "wmata.train_positions.raw"
    assert messages[0].key == "336"
    assert messages[0].value["payload_kind"] == "record"
    assert messages[0].value["record_index"] == 0
    assert messages[0].value["record"]["TrainNumber"] == "407"
    assert messages[0].value["record_count"] == 2

    assert messages[1].key == "317"
    assert messages[1].value["record_index"] == 1


def test_snapshot_message_preserves_reference_response_body() -> None:
    message = snapshot_message(
        source_name="lines",
        endpoint="/Rail.svc/json/jLines",
        params={},
        fetched_at=datetime(2026, 5, 4, tzinfo=timezone.utc),
        ingestion_batch_id="batch-1",
        response_status=200,
        payload={"Lines": [{"LineCode": "RD", "DisplayName": "Red"}]},
    )

    assert message.topic == "wmata.lines.raw"
    assert message.key == "lines:batch-1"
    assert message.value["payload_kind"] == "snapshot"
    assert message.value["response_body"]["Lines"][0]["LineCode"] == "RD"
    assert message.value["record_count"] == 1
