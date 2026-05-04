from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from typing import Any

from psycopg import Connection

from transit_reliability.display import direction_label


def latest_bronze(conn: Connection, table: str) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {table} ORDER BY fetched_at DESC LIMIT 1")
        return cur.fetchone()


def event_hash(*parts: Any) -> str:
    body = json.dumps(parts, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def validate_train_position(record: dict[str, Any]) -> tuple[bool, str | None]:
    if not record.get("TrainId"):
        return False, "missing TrainId"
    if record.get("CircuitId") is None:
        return False, "missing CircuitId"
    return True, None


def refresh_station_reference(conn: Connection) -> None:
    raw = latest_bronze(conn, "bronze_wmata_stations")
    if not raw:
        return
    stations = raw["response_body"].get("Stations") or []
    with conn.cursor() as cur:
        for station in stations:
            cur.execute(
                """
                INSERT INTO silver_station_reference (
                    station_code, station_name, lat, lon, line_code_1, line_code_2,
                    line_code_3, line_code_4, station_together_1, station_together_2,
                    source_raw_id, loaded_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (station_code) DO UPDATE SET
                    station_name = EXCLUDED.station_name,
                    lat = EXCLUDED.lat,
                    lon = EXCLUDED.lon,
                    line_code_1 = EXCLUDED.line_code_1,
                    line_code_2 = EXCLUDED.line_code_2,
                    line_code_3 = EXCLUDED.line_code_3,
                    line_code_4 = EXCLUDED.line_code_4,
                    station_together_1 = EXCLUDED.station_together_1,
                    station_together_2 = EXCLUDED.station_together_2,
                    source_raw_id = EXCLUDED.source_raw_id,
                    loaded_at = now()
                """,
                (
                    station.get("Code"),
                    station.get("Name"),
                    station.get("Lat"),
                    station.get("Lon"),
                    station.get("LineCode1"),
                    station.get("LineCode2"),
                    station.get("LineCode3"),
                    station.get("LineCode4"),
                    station.get("StationTogether1"),
                    station.get("StationTogether2"),
                    raw["id"],
                ),
            )


def refresh_line_reference(conn: Connection) -> None:
    raw = latest_bronze(conn, "bronze_wmata_lines")
    if not raw:
        return
    lines = raw["response_body"].get("Lines") or []
    with conn.cursor() as cur:
        for line in lines:
            cur.execute(
                """
                INSERT INTO silver_line_reference (
                    line_code, display_name, start_station_code, end_station_code,
                    internal_destination_1, internal_destination_2, source_raw_id, loaded_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (line_code) DO UPDATE SET
                    display_name = EXCLUDED.display_name,
                    start_station_code = EXCLUDED.start_station_code,
                    end_station_code = EXCLUDED.end_station_code,
                    internal_destination_1 = EXCLUDED.internal_destination_1,
                    internal_destination_2 = EXCLUDED.internal_destination_2,
                    source_raw_id = EXCLUDED.source_raw_id,
                    loaded_at = now()
                """,
                (
                    line.get("LineCode"),
                    line.get("DisplayName"),
                    line.get("StartStationCode"),
                    line.get("EndStationCode"),
                    line.get("InternalDestination1"),
                    line.get("InternalDestination2"),
                    raw["id"],
                ),
            )


def refresh_route_circuits(conn: Connection) -> None:
    raw = latest_bronze(conn, "bronze_wmata_standard_routes")
    if not raw:
        return
    routes = raw["response_body"].get("StandardRoutes") or []
    with conn.cursor() as cur:
        cur.execute("DELETE FROM silver_route_circuits")
        for route in routes:
            line_code = route.get("LineCode")
            track_num = route.get("TrackNum")
            for circuit in route.get("TrackCircuits") or []:
                cur.execute(
                    """
                    INSERT INTO silver_route_circuits (
                        line_code, track_num, seq_num, circuit_id, station_code, source_raw_id, loaded_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, now())
                    ON CONFLICT (line_code, track_num, seq_num, circuit_id) DO UPDATE SET
                        station_code = EXCLUDED.station_code,
                        source_raw_id = EXCLUDED.source_raw_id,
                        loaded_at = now()
                    """,
                    (
                        line_code,
                        track_num,
                        circuit.get("SeqNum"),
                        circuit.get("CircuitId"),
                        circuit.get("StationCode"),
                        raw["id"],
                    ),
                )


def refresh_train_position_events(conn: Connection) -> None:
    raw = latest_bronze(conn, "bronze_wmata_train_positions")
    if not raw:
        return
    records = raw["response_body"].get("TrainPositions") or []
    with conn.cursor() as cur:
        for record in records:
            is_valid, validation_error = validate_train_position(record)
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
                    raw["id"],
                    raw["ingestion_batch_id"],
                    raw["fetched_at"],
                    event_hash(
                        raw["ingestion_batch_id"],
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


def station_name(conn: Connection, station_code: str | None) -> str | None:
    if not station_code:
        return None
    with conn.cursor() as cur:
        cur.execute(
            "SELECT station_name FROM silver_station_reference WHERE station_code = %s",
            (station_code,),
        )
        row = cur.fetchone()
    return row["station_name"] if row else None


def line_row(conn: Connection, line_code: str | None) -> dict[str, Any] | None:
    if not line_code:
        return None
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM silver_line_reference WHERE line_code = %s", (line_code,))
        return cur.fetchone()


def route_position(conn: Connection, line_code: str | None, direction_num: int | None, circuit_id: int | None) -> dict[str, Any]:
    if not line_code or direction_num is None or circuit_id is None:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT * FROM silver_route_circuits
            WHERE line_code = %s AND track_num = %s AND circuit_id = %s
            ORDER BY seq_num
            LIMIT 1
            """,
            (line_code, direction_num, circuit_id),
        )
        current = cur.fetchone()
        if not current:
            cur.execute(
                """
                SELECT * FROM silver_route_circuits
                WHERE line_code = %s AND circuit_id = %s
                ORDER BY track_num, seq_num
                LIMIT 1
                """,
                (line_code, circuit_id),
            )
            current = cur.fetchone()
        if not current:
            return {}

        cur.execute(
            """
            SELECT station_code FROM silver_route_circuits
            WHERE line_code = %s AND track_num = %s AND seq_num <= %s AND station_code IS NOT NULL
            ORDER BY seq_num DESC
            LIMIT 1
            """,
            (current["line_code"], current["track_num"], current["seq_num"]),
        )
        previous_station = cur.fetchone()
        cur.execute(
            """
            SELECT station_code FROM silver_route_circuits
            WHERE line_code = %s AND track_num = %s AND seq_num > %s AND station_code IS NOT NULL
            ORDER BY seq_num
            LIMIT 1
            """,
            (current["line_code"], current["track_num"], current["seq_num"]),
        )
        next_station = cur.fetchone()

    current_station_code = current["station_code"] or (previous_station or {}).get("station_code")
    next_station_code = (next_station or {}).get("station_code")
    current_station_name = station_name(conn, current_station_code)
    next_station_name = station_name(conn, next_station_code)

    if current["station_code"] and current_station_name:
        current_location = f"At {current_station_name}"
    elif current_station_name and next_station_name:
        current_location = f"Between {current_station_name} and {next_station_name}"
    elif next_station_name:
        current_location = f"Approaching {next_station_name}"
    else:
        current_location = "Location unavailable"

    return {
        "current_location": current_location,
        "current_station_code": current_station_code,
        "current_station_name": current_station_name,
        "next_station_code": next_station_code,
        "next_station_name": next_station_name,
    }


def freshness_status(last_seen_at: datetime, stale_after_seconds: int) -> str:
    age = (datetime.now(timezone.utc) - last_seen_at).total_seconds()
    return "stale" if age > stale_after_seconds else "fresh"


def infer_origin(line: dict[str, Any] | None, destination_station_code: str | None) -> str | None:
    if not line:
        return None
    start = line.get("start_station_code")
    end = line.get("end_station_code")
    if destination_station_code == start:
        return end
    if destination_station_code == end:
        return start
    return None


def refresh_current_train_board(conn: Connection, config: dict[str, Any]) -> None:
    stale_after_seconds = config["polling"]["stale_after_seconds"]
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (train_id) *
            FROM silver_train_position_events
            WHERE is_valid = true AND train_id IS NOT NULL
            ORDER BY train_id, fetched_at DESC
            """
        )
        latest_events = cur.fetchall()
        cur.execute("TRUNCATE gold_current_train_board")

    with conn.cursor() as cur:
        for event in latest_events:
            line = line_row(conn, event["line_code"])
            line_code_display = event["line_code"] or "UNASSIGNED"
            line_name_display = (line or {}).get("display_name") or "Unassigned / Non-Revenue"
            destination_name = station_name(conn, event["destination_station_code"])
            origin_station_code = infer_origin(line, event["destination_station_code"])
            origin_station_name = station_name(conn, origin_station_code)
            location = route_position(
                conn,
                event["line_code"],
                event["direction_num"],
                event["circuit_id"],
            )
            cur.execute(
                """
                INSERT INTO gold_current_train_board (
                    train_id, train_number, line_code, line_name, current_location,
                    current_station_code, current_station_name, next_station_code,
                    next_station_name, origin_station_code, origin_station_name,
                    destination_station_code, destination_station_name, direction_num,
                    direction_label, car_count, service_status, seconds_at_location,
                    last_seen_at, refreshed_at, freshness_status
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), %s)
                """,
                (
                    event["train_id"],
                    event["train_number"],
                    line_code_display,
                    line_name_display,
                    location.get("current_location", "Location unavailable"),
                    location.get("current_station_code"),
                    location.get("current_station_name"),
                    location.get("next_station_code"),
                    location.get("next_station_name"),
                    origin_station_code,
                    origin_station_name,
                    event["destination_station_code"],
                    destination_name,
                    event["direction_num"],
                    direction_label(event["direction_num"]),
                    event["car_count"],
                    event["service_type"],
                    event["seconds_at_location"],
                    event["fetched_at"],
                    freshness_status(event["fetched_at"], stale_after_seconds),
                ),
            )


def refresh_feed_health(conn: Connection) -> None:
    sources = {
        "wmata_train_positions": "bronze_wmata_train_positions",
        "wmata_standard_routes": "bronze_wmata_standard_routes",
        "wmata_stations": "bronze_wmata_stations",
        "wmata_lines": "bronze_wmata_lines",
    }
    with conn.cursor() as cur:
        for source_name, table in sources.items():
            latest = latest_bronze(conn, table)
            if not latest:
                continue
            cur.execute(
                """
                INSERT INTO gold_feed_health (
                    source_name, last_successful_fetch_at, latest_record_count,
                    latest_response_status, latest_content_hash, refreshed_at
                )
                VALUES (%s, %s, %s, %s, %s, now())
                ON CONFLICT (source_name) DO UPDATE SET
                    last_successful_fetch_at = EXCLUDED.last_successful_fetch_at,
                    latest_record_count = EXCLUDED.latest_record_count,
                    latest_response_status = EXCLUDED.latest_response_status,
                    latest_content_hash = EXCLUDED.latest_content_hash,
                    refreshed_at = now()
                """,
                (
                    source_name,
                    latest["fetched_at"] if latest["response_status"] < 400 else None,
                    latest["record_count"],
                    latest["response_status"],
                    latest["content_hash"],
                ),
            )


def refresh_line_activity_history(conn: Connection, window_start: datetime, window_end: datetime) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO gold_line_activity_history (
                window_start, window_end, line_code, line_name, active_trains,
                stale_trains, location_unavailable_trains, normal_service_trains,
                no_passenger_trains, refreshed_at
            )
            SELECT
                %s AS window_start,
                %s AS window_end,
                line_code,
                MAX(line_name) AS line_name,
                COUNT(*) AS active_trains,
                COUNT(*) FILTER (WHERE freshness_status = 'stale') AS stale_trains,
                COUNT(*) FILTER (WHERE current_location = 'Location unavailable') AS location_unavailable_trains,
                COUNT(*) FILTER (WHERE service_status = 'Normal') AS normal_service_trains,
                COUNT(*) FILTER (WHERE service_status = 'NoPassengers') AS no_passenger_trains,
                now() AS refreshed_at
            FROM gold_current_train_board
            GROUP BY line_code
            ON CONFLICT (window_start, line_code) DO UPDATE SET
                window_end = EXCLUDED.window_end,
                line_name = EXCLUDED.line_name,
                active_trains = EXCLUDED.active_trains,
                stale_trains = EXCLUDED.stale_trains,
                location_unavailable_trains = EXCLUDED.location_unavailable_trains,
                normal_service_trains = EXCLUDED.normal_service_trains,
                no_passenger_trains = EXCLUDED.no_passenger_trains,
                refreshed_at = now()
            """,
            (window_start, window_end),
        )


def refresh_feed_health_history(conn: Connection, observed_at: datetime) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO gold_feed_health_history (
                source_name, observed_at, latest_record_count,
                latest_response_status, latest_content_hash, refreshed_at
            )
            SELECT
                source_name,
                %s AS observed_at,
                latest_record_count,
                latest_response_status,
                latest_content_hash,
                now() AS refreshed_at
            FROM gold_feed_health
            ON CONFLICT (source_name, observed_at) DO UPDATE SET
                latest_record_count = EXCLUDED.latest_record_count,
                latest_response_status = EXCLUDED.latest_response_status,
                latest_content_hash = EXCLUDED.latest_content_hash,
                refreshed_at = now()
            """,
            (observed_at,),
        )


def refresh_history(conn: Connection, observed_at: datetime | None = None) -> None:
    event_time = observed_at or datetime.now(timezone.utc)
    window_start = event_time.replace(second=0, microsecond=0)
    window_end = window_start + timedelta(minutes=1)
    refresh_line_activity_history(conn, window_start, window_end)
    refresh_feed_health_history(conn, event_time)


def refresh_all(conn: Connection, config: dict[str, Any]) -> None:
    refresh_line_reference(conn)
    refresh_station_reference(conn)
    refresh_route_circuits(conn)
    refresh_train_position_events(conn)
    refresh_current_train_board(conn, config)
    refresh_feed_health(conn)
