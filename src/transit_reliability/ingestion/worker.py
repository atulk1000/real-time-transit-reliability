from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from psycopg import Connection
from psycopg.types.json import Jsonb

from transit_reliability.config import load_config
from transit_reliability.db import get_connection
from transit_reliability.transforms.refresh import refresh_all
from transit_reliability.wmata_client import WmataClient

SOURCE_RECORD_KEYS = {
    "train_positions": "TrainPositions",
    "standard_routes": "StandardRoutes",
    "stations": "Stations",
    "lines": "Lines",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run WMATA MVP ingestion worker.")
    parser.add_argument("--once", action="store_true", help="Run one train-position poll and exit.")
    parser.add_argument(
        "--refresh-reference",
        action="store_true",
        help="Refresh lines, stations, and standard routes before polling train positions.",
    )
    return parser.parse_args()


def stable_hash(payload: Any) -> str:
    body = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def record_count(source_name: str, payload: dict[str, Any]) -> int | None:
    key = SOURCE_RECORD_KEYS.get(source_name)
    records = payload.get(key) if key else None
    return len(records) if isinstance(records, list) else None


def insert_bronze(
    conn: Connection,
    *,
    raw_table: str,
    source_name: str,
    endpoint: str,
    params: dict[str, Any],
    status_code: int,
    payload: dict[str, Any],
    fetched_at: datetime,
    ingestion_batch_id: str,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {raw_table} (
                fetched_at, response_status, request_url, request_params,
                response_body, record_count, content_hash, ingestion_batch_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                fetched_at,
                status_code,
                endpoint,
                Jsonb(params),
                Jsonb(payload),
                record_count(source_name, payload),
                stable_hash(payload),
                ingestion_batch_id,
            ),
        )
        raw_id = cur.fetchone()["id"]
    return raw_id


def source_api_key(config: dict[str, Any], source: dict[str, Any]) -> str | None:
    key_env_var = source.get("key_env_var") or config["api"].get("fallback_key_env_var")
    api_key = os.getenv(key_env_var or "")
    if api_key:
        return api_key
    fallback_env_var = config["api"].get("fallback_key_env_var")
    return os.getenv(fallback_env_var or "")


def env_bool(name: str | None, default: bool = True) -> bool:
    if not name:
        return default
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "no", "off"}


def poll_source(
    conn: Connection,
    client: WmataClient,
    config: dict[str, Any],
    source_name: str,
    source: dict[str, Any],
) -> int:
    fetched_at = datetime.now(timezone.utc)
    ingestion_batch_id = str(uuid4())
    response = client.get(
        source["endpoint"],
        source.get("params") or {},
        api_key=source_api_key(config, source),
    )
    payload = response.json()
    raw_id = insert_bronze(
        conn,
        raw_table=source["raw_table"],
        source_name=source_name,
        endpoint=source["endpoint"],
        params=source.get("params") or {},
        status_code=response.status_code,
        payload=payload,
        fetched_at=fetched_at,
        ingestion_batch_id=ingestion_batch_id,
    )
    response.raise_for_status()
    return raw_id


def main() -> None:
    args = parse_args()
    config = load_config()
    client = WmataClient(
        base_url=config["api"]["base_url"],
        key_header_name=config["api"]["key_header_name"],
        verify_ssl=env_bool(config["api"].get("verify_ssl_env_var"), default=True),
    )

    with get_connection() as conn:
        if args.refresh_reference:
            for source_name in ("lines", "stations", "standard_routes"):
                poll_source(conn, client, config, source_name, config["sources"][source_name])
            refresh_all(conn, config)
            conn.commit()

        interval = config["polling"]["train_positions_seconds"]
        while True:
            poll_source(
                conn, client, config, "train_positions", config["sources"]["train_positions"]
            )
            refresh_all(conn, config)
            conn.commit()
            if args.once:
                break
            time.sleep(interval)


if __name__ == "__main__":
    main()
