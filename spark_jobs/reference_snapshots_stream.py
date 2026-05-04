from __future__ import annotations

import json
import os

from pyspark.sql import SparkSession

from transit_reliability.config import load_config
from transit_reliability.db import get_connection
from transit_reliability.streaming.postgres_sink import process_reference_envelopes


def main() -> None:
    bootstrap_servers = os.getenv("SPARK_KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
    checkpoint_location = os.getenv(
        "SPARK_REFERENCE_CHECKPOINT",
        "/tmp/transit-reliability/checkpoints/reference_snapshots",
    )

    spark = SparkSession.builder.appName("wmata-reference-snapshots-stream").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    messages = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", "wmata.lines.raw,wmata.stations.raw,wmata.standard_routes.raw")
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value AS STRING) AS value")
    )

    config = load_config()

    def foreach_batch(batch_df, batch_id: int) -> None:
        envelopes = [json.loads(row["value"]) for row in batch_df.collect()]
        with get_connection() as conn:
            process_reference_envelopes(conn, envelopes, config)
            conn.commit()

    query = (
        messages.writeStream.foreachBatch(foreach_batch)
        .option("checkpointLocation", checkpoint_location)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
