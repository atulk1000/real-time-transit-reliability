from fastapi import FastAPI

from transit_reliability.db import get_connection

app = FastAPI(title="Transit Reliability API")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/trains/current")
def current_trains() -> list[dict]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM gold_current_train_board
                ORDER BY line_code, direction_num, train_number, train_id
                """)
            return cur.fetchall()


@app.get("/routes/reliability")
def route_reliability() -> list[dict]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    line_code,
                    COALESCE(line_name, line_code) AS line_name,
                    COUNT(*) AS active_trains,
                    COUNT(*) FILTER (WHERE freshness_status = 'stale') AS stale_trains,
                    MAX(last_seen_at) AS last_seen_at
                FROM gold_current_train_board
                GROUP BY line_code, line_name
                ORDER BY line_code
                """)
            return cur.fetchall()


@app.get("/feed-health")
def feed_health() -> list[dict]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM gold_feed_health ORDER BY source_name")
            return cur.fetchall()


@app.get("/history/line-activity")
def line_activity_history() -> list[dict]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM gold_line_activity_history
                ORDER BY window_start DESC, line_code
                LIMIT 1000
                """)
            return cur.fetchall()


@app.get("/history/feed-health")
def feed_health_history() -> list[dict]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM gold_feed_health_history
                ORDER BY observed_at DESC, source_name
                LIMIT 1000
                """)
            return cur.fetchall()
