# =========================================================
# BOTTLENECK KPI STREAM (SAFE VERSION)
# =========================================================

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    when,
    lit,
    round,
    avg,
    count
)

# =========================================================
# BOTTLENECK STREAM BUILDER
# =========================================================
def start_bottleneck_kpi_stream(transformed_df: DataFrame):

    df = transformed_df

    # =====================================================
    # 1. ENSURE LOCAL BOTTLENECK FLAG (DO NOT TRUST PIPELINE ONLY)
    # =====================================================
    df = df.withColumn(
        "bottleneck_flag_local",
        (col("delay_hours") > 24) | (col("operation_state") == "UNDER_REVIEW")
    )

    # =====================================================
    # 2. BOTTLENECK SCORE (CORE METRIC)
    # =====================================================
    df = df.withColumn(
        "bottleneck_score",
        round(
            (col("delay_hours") * 2.5) +
            (col("system_pressure") * 1.5) +
            when(col("bottleneck_flag_local"), 30).otherwise(0),
            2
        )
    )

    # =====================================================
    # 3. ZONE PRESSURE INDEX (LIGHTWEIGHT LOCAL VIEW)
    # =====================================================
    df = df.withColumn(
        "zone_pressure_index",
        round(col("system_pressure") * col("delay_hours"), 2)
    )

    # =====================================================
    # 4. KPI CATEGORY (CRITICAL FOR PIPELINE SEPARATION)
    # =====================================================
    df = df.withColumn(
        "kpi_category",
        lit("BOTTLENECK_INTELLIGENCE")
    )

    # =====================================================
    # 5. FINAL SELECT (NO OVERLAP WITH FLOW KPI)
    # =====================================================
    bottleneck_df = df.select(
        "sequence_id",
        "container_id",
        "zone",

        "delay_hours",
        "system_pressure",

        "bottleneck_flag_local",
        "bottleneck_score",
        "zone_pressure_index",

        "kpi_category"
    )

    # =====================================================
    # 6. STREAM OUTPUT (SAFE CONSOLE)
    # =====================================================
    query = (
        bottleneck_df.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", False)
        .option("numRows", 50)

        # IMPORTANT: separate checkpoint
        .option("checkpointLocation", "/tmp/checkpoints/bottleneck_kpis")

        .trigger(processingTime="10 seconds")
        .start()
    )

    return bottleneck_df