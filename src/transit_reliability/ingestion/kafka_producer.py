from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from dotenv import load_dotenv

from transit_reliability.config import load_config
from transit_reliability.ingestion.worker import env_bool, source_api_key
from transit_reliability.streaming.envelope import KafkaMessage, snapshot_message, train_position_messages
from transit_reliability.streaming.topics import load_kafka_config
from transit_reliability.wmata_client import WmataClient


load_dotenv()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish WMATA API data to Kafka/Redpanda.")
    parser.add_argument("--once", action="store_true", help="Run one train-position poll and exit.")
    parser.add_argument(
        "--refresh-reference",
        action="store_true",
        help="Publish lines, stations, and standard routes before train positions.",
    )
    return parser.parse_args()


def kafka_bootstrap_servers(kafka_config: dict[str, Any]) -> str:
    return os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        kafka_config["kafka"]["local_bootstrap_servers"],
    )


def build_producer(bootstrap_servers: str):
    from kafka import KafkaProducer

    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode("utf-8"),
        linger_ms=50,
        retries=5,
    )


def publish_messages(producer, messages: list[KafkaMessage]) -> None:
    for message in messages:
        producer.send(message.topic, key=message.key, value=message.value)
    producer.flush()


def poll_source_messages(
    *,
    client: WmataClient,
    app_config: dict[str, Any],
    topic_config: dict[str, Any],
    source_name: str,
    source: dict[str, Any],
) -> list[KafkaMessage]:
    fetched_at = datetime.now(timezone.utc)
    ingestion_batch_id = str(uuid4())
    response = client.get(
        source["endpoint"],
        source.get("params") or {},
        api_key=source_api_key(app_config, source),
    )
    payload = response.json()
    response.raise_for_status()

    if source_name == "train_positions":
        return train_position_messages(
            endpoint=source["endpoint"],
            params=source.get("params") or {},
            fetched_at=fetched_at,
            ingestion_batch_id=ingestion_batch_id,
            response_status=response.status_code,
            payload=payload,
            topic_config=topic_config,
        )

    return [
        snapshot_message(
            source_name=source_name,
            endpoint=source["endpoint"],
            params=source.get("params") or {},
            fetched_at=fetched_at,
            ingestion_batch_id=ingestion_batch_id,
            response_status=response.status_code,
            payload=payload,
            topic_config=topic_config,
        )
    ]


def main() -> None:
    args = parse_args()
    app_config = load_config()
    topic_config = load_kafka_config()
    client = WmataClient(
        base_url=app_config["api"]["base_url"],
        key_header_name=app_config["api"]["key_header_name"],
        verify_ssl=env_bool(app_config["api"].get("verify_ssl_env_var"), default=True),
    )
    producer = build_producer(kafka_bootstrap_servers(topic_config))

    if args.refresh_reference:
        for source_name in ("lines", "stations", "standard_routes"):
            publish_messages(
                producer,
                poll_source_messages(
                    client=client,
                    app_config=app_config,
                    topic_config=topic_config,
                    source_name=source_name,
                    source=app_config["sources"][source_name],
                ),
            )

    interval = app_config["polling"]["train_positions_seconds"]
    while True:
        publish_messages(
            producer,
            poll_source_messages(
                client=client,
                app_config=app_config,
                topic_config=topic_config,
                source_name="train_positions",
                source=app_config["sources"]["train_positions"],
            ),
        )
        if args.once:
            break
        time.sleep(interval)


if __name__ == "__main__":
    main()
