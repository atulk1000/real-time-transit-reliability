from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from transit_reliability.config import project_root


def load_kafka_config(path: str | Path | None = None) -> dict[str, Any]:
    config_path = Path(path) if path else project_root() / "configs" / "kafka.yml"
    with config_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def topic_for_source(source_name: str, config: dict[str, Any] | None = None) -> str:
    kafka_config = config or load_kafka_config()
    return kafka_config["topics"][source_name]["name"]


def topic_partitions(source_name: str, config: dict[str, Any] | None = None) -> int:
    kafka_config = config or load_kafka_config()
    return int(kafka_config["topics"][source_name]["partitions"])


def train_position_key(record: dict[str, Any], ingestion_batch_id: str) -> str:
    train_id = record.get("TrainId")
    return str(train_id) if train_id else ingestion_batch_id


def snapshot_key(source_name: str, ingestion_batch_id: str) -> str:
    return f"{source_name}:{ingestion_batch_id}"
