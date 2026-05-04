import pandas as pd
import plotly.express as px
import streamlit as st

from transit_reliability.db import get_connection
from transit_reliability.display import direction_label


def query(sql: str) -> pd.DataFrame:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    return pd.DataFrame(rows)


st.set_page_config(page_title="Train Reliability", layout="wide")
st.title("WMATA Metro Train Board")

snapshot = query("SELECT * FROM gold_current_train_board ORDER BY line_code, direction_num, train_number, train_id")
feed_health = query("SELECT * FROM gold_feed_health ORDER BY source_name")
line_history = query(
    """
    SELECT *
    FROM gold_line_activity_history
    ORDER BY window_start
    """
)
feed_history = query(
    """
    SELECT *
    FROM gold_feed_health_history
    ORDER BY observed_at
    """
)

if snapshot.empty:
    st.info("No train board data yet. Run the ingestion worker with --refresh-reference --once.")
    st.stop()

active = snapshot["train_id"].nunique()
stale = int((snapshot["freshness_status"] == "stale").sum())
stale_pct = stale / active if active else 0
service_lines = snapshot[snapshot["line_code"] != "UNASSIGNED"]
lines_active = service_lines["line_code"].nunique()
unassigned = int((snapshot["line_code"] == "UNASSIGNED").sum())

current_tab, history_tab, feed_tab = st.tabs(["Current Train Board", "Historical Metrics", "Feed Health"])

with current_tab:
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Active Metro trains", active)
    col2.metric("Stale trains", stale)
    col3.metric("Stale share", f"{stale_pct:.1%}")
    col4.metric("Service lines active", lines_active)
    col5.metric("Unassigned trains", unassigned)

    st.subheader("Line Summary")
    line_summary = (
        snapshot.groupby(["line_code", "line_name"], dropna=False)
        .agg(
            active_trains=("train_id", "nunique"),
            stale_trains=("freshness_status", lambda value: (value == "stale").sum()),
            last_seen_at=("last_seen_at", "max"),
        )
        .reset_index()
    )
    line_summary["stale_pct"] = line_summary["stale_trains"] / line_summary["active_trains"]
    line_summary["stale_pct"] = line_summary["stale_pct"].map(lambda value: f"{value:.1%}")
    st.dataframe(
        line_summary.sort_values(["line_code"]),
        use_container_width=True,
    )

    st.subheader("Train Board")
    line_codes = ["All"] + sorted(snapshot["line_code"].dropna().unique().tolist())
    selected_line = st.selectbox("Line", line_codes)
    board = snapshot if selected_line == "All" else snapshot[snapshot["line_code"] == selected_line]
    board = board.copy()
    board["direction_label"] = board["direction_num"].map(direction_label).fillna(board["direction_label"])

    columns = [
        "line_code",
        "train_number",
        "train_id",
        "current_location",
        "next_station_name",
        "origin_station_name",
        "destination_station_name",
        "direction_label",
        "car_count",
        "service_status",
        "seconds_at_location",
        "freshness_status",
        "last_seen_at",
    ]
    st.dataframe(board[columns], use_container_width=True, hide_index=True)

with history_tab:
    st.subheader("Historical Line Activity")
    if line_history.empty:
        st.info("No historical metrics yet. Run the V2 Spark train-position stream to populate history.")
    else:
        line_history["window_start"] = pd.to_datetime(line_history["window_start"])
        st.plotly_chart(
            px.line(
                line_history,
                x="window_start",
                y="active_trains",
                color="line_code",
                markers=True,
            ),
            use_container_width=True,
        )
        st.plotly_chart(
            px.line(
                line_history,
                x="window_start",
                y="location_unavailable_trains",
                color="line_code",
                markers=True,
            ),
            use_container_width=True,
        )
        st.dataframe(line_history.sort_values(["window_start", "line_code"], ascending=[False, True]), use_container_width=True, hide_index=True)

with feed_tab:
    st.subheader("Current Feed Health")
    st.dataframe(feed_health, use_container_width=True, hide_index=True)
    st.subheader("Feed Health History")
    if feed_history.empty:
        st.info("No feed-health history yet.")
    else:
        st.dataframe(feed_history.sort_values(["observed_at", "source_name"], ascending=[False, True]), use_container_width=True, hide_index=True)
