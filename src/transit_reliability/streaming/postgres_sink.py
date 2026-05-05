from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from psycopg import Connection
from psycopg.types.json import Jsonb

from transit_reliability.transforms.refresh import (
    event_hash,
    refresh_all,
    refresh_current_train_board,
    refresh_feed_health,
    refresh_history,
    validate_train_position,
)

RAW_TABLES = {
    "train_positions": "bronze_wmata_train_positions",
    "standard_routes": "bronze_wmata_standard_routes",
    "stations": "bronze_wmata_stations",
    "lines": "bronze_wmata_lines",
}

SOURCE_RECORD_KEYS = {
    "train_positions": "TrainPositions",
    "standard_routes": "StandardRoutes",
    "stations": "Stations",
    "lines": "Lines",
}


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def stable_hash(payload: Any) -> str:
    body = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def record_count(source_name: str, payload: dict[str, Any]) -> int | None:
    key = SOURCE_RECORD_KEYS.get(source_name)
    records = payload.get(key) if key else None
    return len(records) if isinstance(records, list) else None


def insert_reference_bronze(conn: Connection, envelope: dict[str, Any]) -> int:
    source_name = envelope["source_name"]
    payload = envelope["response_body"]
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {RAW_TABLES[source_name]} (
                fetched_at, response_status, request_url, request_params,
                response_body, record_count, content_hash, ingestion_batch_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                parse_timestamp(envelope["fetched_at"]),
                envelope["response_status"],
                envelope["endpoint"],
                Jsonb(envelope.get("request_params") or {}),
                Jsonb(payload),
                record_count(source_name, payload),
                stable_hash(payload),
                envelope["ingestion_batch_id"],
            ),
        )
        return cur.fetchone()["id"]


def insert_train_position_bronze(conn: Connection, envelope: dict[str, Any]) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO bronze_wmata_train_positions (
                fetched_at, response_status, request_url, request_params,
                response_body, record_count, content_hash, ingestion_batch_id
            )
            VALUES (%s, %s, %s, %s, %s, 1, %s, %s)
            RETURNING id
            """,
            (
                parse_timestamp(envelope["fetched_at"]),
                envelope["response_status"],
                envelope["endpoint"],
                Jsonb(envelope.get("request_params") or {}),
                Jsonb(envelope),
                envelope["content_hash"],
                envelope["ingestion_batch_id"],
            ),
        )
        return cur.fetchone()["id"]


def insert_train_position_silver(
    conn: Connection, source_raw_id: int, envelope: dict[str, Any]
) -> None:
    record = envelope.get("record") or {}
    is_valid, validation_error = validate_train_position(record)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO silver_train_position_events (
                source_raw_id, ingestion_batch_id, fetched_at, event_hash,
                train_id, train_number, line_code, direction_num, circuit_id,
                destination_station_code, seconds_at_location, service_type,
                car_count, is_valid, validation_error
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_hash) DO NOTHING
            """,
            (
                source_raw_id,
                envelope["ingestion_batch_id"],
                parse_timestamp(envelope["fetched_at"]),
                event_hash(
                    envelope["ingestion_batch_id"],
                    record.get("TrainId"),
                    record.get("TrainNumber"),
                    record.get("LineCode"),
                    record.get("DirectionNum"),
                    record.get("CircuitId"),
                    record.get("DestinationStationCode"),
                    record.get("SecondsAtLocation"),
                ),
                record.get("TrainId"),
                record.get("TrainNumber"),
                record.get("LineCode"),
                record.get("DirectionNum"),
                record.get("CircuitId"),
                record.get("DestinationStationCode"),
                record.get("SecondsAtLocation"),
                record.get("ServiceType"),
                record.get("CarCount"),
                is_valid,
                validation_error,
            ),
        )


def process_train_position_envelopes(
    conn: Connection,
    envelopes: list[dict[str, Any]],
    config: dict[str, Any],
) -> None:
    if not envelopes:
        return
    for envelope in envelopes:
        raw_id = insert_train_position_bronze(conn, envelope)
        insert_train_position_silver(conn, raw_id, envelope)

    latest_seen = max(parse_timestamp(envelope["fetched_at"]) for envelope in envelopes)
    refresh_current_train_board(conn, config)
    refresh_feed_health(conn)
    refresh_history(conn, latest_seen)


def process_reference_envelopes(
    conn: Connection,
    envelopes: list[dict[str, Any]],
    config: dict[str, Any],
) -> None:
    for envelope in envelopes:
        insert_reference_bronze(conn, envelope)

    refresh_all(conn, config)
