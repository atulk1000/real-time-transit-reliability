import os
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from transit_reliability.transforms.refresh import (
    refresh_all,
    refresh_current_train_board,
    route_position,
)


TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")


pytestmark = pytest.mark.skipif(
    not TEST_DATABASE_URL,
    reason="Set TEST_DATABASE_URL to run database integration tests.",
)


@pytest.fixture()
def conn():
    assert TEST_DATABASE_URL
    with psycopg.connect(TEST_DATABASE_URL, row_factory=dict_row) as connection:
        schema_sql = Path("sql/schema.sql").read_text(encoding="utf-8")
        connection.execute(schema_sql)
        for table in [
            "gold_feed_health_history",
            "gold_line_activity_history",
            "gold_feed_health",
            "gold_current_train_board",
            "silver_train_position_events",
            "silver_route_circuits",
            "silver_line_reference",
            "silver_station_reference",
            "bronze_wmata_train_positions",
            "bronze_wmata_standard_routes",
            "bronze_wmata_stations",
            "bronze_wmata_lines",
        ]:
            connection.execute(f"TRUNCATE {table} RESTART IDENTITY CASCADE")
        yield connection
        connection.rollback()


def insert_bronze(connection, table: str, response_body: dict, record_count: int) -> int:
    row = connection.execute(
        f"""
        INSERT INTO {table} (
            response_status, request_url, request_params, response_body,
            record_count, content_hash, ingestion_batch_id
        )
        VALUES (200, '/test', '{{}}'::jsonb, %s, %s, 'hash', %s)
        RETURNING id
        """,
        (Jsonb(response_body), record_count, str(uuid4())),
    ).fetchone()
    return row["id"]


def test_route_position_derives_between_and_next_station(conn) -> None:
    insert_bronze(
        conn,
        "bronze_wmata_stations",
        {
            "Stations": [
                {"Code": "A01", "Name": "Alpha"},
                {"Code": "A02", "Name": "Beta"},
                {"Code": "A03", "Name": "Gamma"},
            ]
        },
        3,
    )
    insert_bronze(
        conn,
        "bronze_wmata_standard_routes",
        {
            "StandardRoutes": [
                {
                    "LineCode": "RD",
                    "TrackNum": 1,
                    "TrackCircuits": [
                        {"SeqNum": 1, "CircuitId": 101, "StationCode": "A01"},
                        {"SeqNum": 2, "CircuitId": 102, "StationCode": None},
                        {"SeqNum": 3, "CircuitId": 103, "StationCode": "A02"},
                        {"SeqNum": 4, "CircuitId": 104, "StationCode": "A03"},
                    ],
                }
            ]
        },
        1,
    )
    refresh_all(conn, {"polling": {"stale_after_seconds": 45}})

    position = route_position(conn, "RD", 1, 102)

    assert position["current_location"] == "Between Alpha and Beta"
    assert position["current_station_code"] == "A01"
    assert position["next_station_code"] == "A02"
    assert position["next_station_name"] == "Beta"


def test_route_position_derives_at_station_and_next_station(conn) -> None:
    insert_bronze(
        conn,
        "bronze_wmata_stations",
        {
            "Stations": [
                {"Code": "A01", "Name": "Alpha"},
                {"Code": "A02", "Name": "Beta"},
            ]
        },
        2,
    )
    insert_bronze(
        conn,
        "bronze_wmata_standard_routes",
        {
            "StandardRoutes": [
                {
                    "LineCode": "RD",
                    "TrackNum": 1,
                    "TrackCircuits": [
                        {"SeqNum": 1, "CircuitId": 101, "StationCode": "A01"},
                        {"SeqNum": 2, "CircuitId": 102, "StationCode": "A02"},
                    ],
                }
            ]
        },
        1,
    )
    refresh_all(conn, {"polling": {"stale_after_seconds": 45}})

    position = route_position(conn, "RD", 1, 101)

    assert position["current_location"] == "At Alpha"
    assert position["current_station_code"] == "A01"
    assert position["next_station_code"] == "A02"


def test_refresh_current_train_board_applies_display_defaults_and_enrichment(conn) -> None:
    now = datetime.now(timezone.utc)
    station_raw_id = insert_bronze(conn, "bronze_wmata_stations", {"Stations": []}, 0)
    line_raw_id = insert_bronze(conn, "bronze_wmata_lines", {"Lines": []}, 0)
    routes_raw_id = insert_bronze(conn, "bronze_wmata_standard_routes", {"StandardRoutes": []}, 0)
    train_raw_id = insert_bronze(conn, "bronze_wmata_train_positions", {"TrainPositions": []}, 0)
    conn.execute(
        """
        INSERT INTO silver_station_reference (station_code, station_name, source_raw_id)
        VALUES
            ('A01', 'Alpha', %s),
            ('A02', 'Beta', %s)
        """,
        (station_raw_id, station_raw_id),
    )
    conn.execute(
        """
        INSERT INTO silver_line_reference (
            line_code, display_name, start_station_code, end_station_code, source_raw_id
        )
        VALUES ('RD', 'Red', 'A01', 'A02', %s)
        """,
        (line_raw_id,),
    )
    conn.execute(
        """
        INSERT INTO silver_route_circuits (
            line_code, track_num, seq_num, circuit_id, station_code, source_raw_id
        )
        VALUES
            ('RD', 1, 1, 101, 'A01', %s),
            ('RD', 1, 2, 102, 'A02', %s)
        """,
        (routes_raw_id, routes_raw_id),
    )
    conn.execute(
        """
        INSERT INTO silver_train_position_events (
            source_raw_id, ingestion_batch_id, fetched_at, event_hash, train_id,
            train_number, line_code, direction_num, circuit_id,
            destination_station_code, seconds_at_location, service_type,
            car_count, is_valid
        )
        VALUES
            (%s, %s, %s, 'assigned', 'T1', '101', 'RD', 1, 101, 'A02', 5, 'Normal', 8, true),
            (%s, %s, %s, 'unassigned', 'T2', '102', NULL, 2, 999, NULL, 7, 'NoPassengers', 6, true)
        """,
        (train_raw_id, str(uuid4()), now, train_raw_id, str(uuid4()), now),
    )

    refresh_current_train_board(conn, {"polling": {"stale_after_seconds": 45}})
    rows = conn.execute(
        "SELECT * FROM gold_current_train_board ORDER BY train_id"
    ).fetchall()

    assert rows[0]["line_code"] == "RD"
    assert rows[0]["line_name"] == "Red"
    assert rows[0]["current_location"] == "At Alpha"
    assert rows[0]["next_station_name"] == "Beta"
    assert rows[0]["origin_station_name"] == "Alpha"
    assert rows[0]["destination_station_name"] == "Beta"
    assert rows[0]["direction_label"] == "Northbound / Eastbound"

    assert rows[1]["line_code"] == "UNASSIGNED"
    assert rows[1]["line_name"] == "Unassigned / Non-Revenue"
    assert rows[1]["direction_label"] == "Southbound / Westbound"
