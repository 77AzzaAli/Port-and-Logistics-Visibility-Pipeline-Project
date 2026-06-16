# =========================================================
# FLOW KPI STREAM (CLEAN VERSION)
# =========================================================
from pyspark.sql.functions import lit
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    window,
    col,
    avg,
    count,
    when,
    round
)

# =========================================================
# FLOW KPI BUILDER (NO STREAM START HERE)
# =========================================================
def start_flow_kpi_stream(transformed_df: DataFrame):

    flow_kpi_df = (
        transformed_df

        # WATERMARK (required for windowed aggregation)
        .withWatermark("event_time", "1 minute")

        # WINDOW + GROUPING
        .groupBy(
            window(col("event_time"), "1 minute"),
            col("event_type")
        )

        # KPI AGGREGATIONS
        .agg(

            count("*").alias("containers_processed"),

            round(
                avg(col("delay_hours")),
                2
            ).alias("avg_delay_hours"),

            round(
                avg(col("stage_efficiency_ratio")),
                2
            ).alias("avg_efficiency"),

            round(
                (
                    count(when(col("status") == "SUCCESS", True)) /
                    count("*")
                ) * 100,
                2
            ).alias("success_rate_pct")
        )

        # FLATTEN WINDOW
        .select(

            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("event_type"),
            col("containers_processed"),
            col("avg_delay_hours"),
            col("avg_efficiency"),
            col("success_rate_pct")
        )
    )

    # STREAM OUTPUT (SAFE)
    query = (
        flow_kpi_df.writeStream

        .format("console")
        .outputMode("update")

        .option("truncate", False)
        .option("numRows", 50)

        .option("checkpointLocation", "/tmp/checkpoints/flow_kpis")

        .trigger(processingTime="10 seconds")
        .start()
    )

    return flow_kpi_df