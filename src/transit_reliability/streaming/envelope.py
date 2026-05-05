from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from transit_reliability.ingestion.worker import record_count, stable_hash
from transit_reliability.streaming.topics import snapshot_key, topic_for_source, train_position_key


@dataclass(frozen=True)
class KafkaMessage:
    topic: str
    key: str
    value: dict[str, Any]


def base_envelope(
    *,
    source_name: str,
    endpoint: str,
    params: dict[str, Any],
    fetched_at: datetime,
    ingestion_batch_id: str,
    response_status: int,
    payload: dict[str, Any],
) -> dict[str, Any]:
    return {
        "message_version": 1,
        "source_name": source_name,
        "endpoint": endpoint,
        "request_params": params,
        "fetched_at": fetched_at.isoformat(),
        "ingestion_batch_id": ingestion_batch_id,
        "response_status": response_status,
        "record_count": record_count(source_name, payload),
        "content_hash": stable_hash(payload),
    }


def train_position_messages(
    *,
    endpoint: str,
    params: dict[str, Any],
    fetched_at: datetime,
    ingestion_batch_id: str,
    response_status: int,
    payload: dict[str, Any],
    topic_config: dict[str, Any] | None = None,
) -> list[KafkaMessage]:
    records = payload.get("TrainPositions") or []
    envelope = base_envelope(
        source_name="train_positions",
        endpoint=endpoint,
        params=params,
        fetched_at=fetched_at,
        ingestion_batch_id=ingestion_batch_id,
        response_status=response_status,
        payload=payload,
    )
    topic = topic_for_source("train_positions", topic_config)
    return [
        KafkaMessage(
            topic=topic,
            key=train_position_key(record, ingestion_batch_id),
            value={
                **envelope,
                "payload_kind": "record",
                "record_index": index,
                "record": record,
            },
        )
        for index, record in enumerate(records)
    ]


def snapshot_message(
    *,
    source_name: str,
    endpoint: str,
    params: dict[str, Any],
    fetched_at: datetime,
    ingestion_batch_id: str,
    response_status: int,
    payload: dict[str, Any],
    topic_config: dict[str, Any] | None = None,
) -> KafkaMessage:
    envelope = base_envelope(
        source_name=source_name,
        endpoint=endpoint,
        params=params,
        fetched_at=fetched_at,
        ingestion_batch_id=ingestion_batch_id,
        response_status=response_status,
        payload=payload,
    )
    return KafkaMessage(
        topic=topic_for_source(source_name, topic_config),
        key=snapshot_key(source_name, ingestion_batch_id),
        value={
            **envelope,
            "payload_kind": "snapshot",
            "response_body": payload,
        },
    )
