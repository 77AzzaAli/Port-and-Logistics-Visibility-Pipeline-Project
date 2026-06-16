# =========================================================
# CUSTOMS KPI STREAM (SAFE VERSION)
# =========================================================

from pyspark.sql import DataFrame

from pyspark.sql.functions import (
    col,
    when,
    lit,
    round
)

# =========================================================
# CUSTOMS KPI STREAM
# =========================================================
def start_customs_kpi_stream(
    transformed_df: DataFrame
):

    customs_df = (

        transformed_df

        # =================================================
        # CUSTOMS EVENTS ONLY
        # =================================================
        .filter(
            col("event_type").startswith("CUSTOMS")
        )

        # =================================================
        # CUSTOMS DELAY RATE
        # =================================================
        .withColumn(
            "customs_delay_flag",
            when(
                col("delay_hours") > 0,
                True
            ).otherwise(False)
        )

        # =================================================
        # INSPECTION PRESSURE
        # =================================================
        .withColumn(
            "inspection_pressure",
            round(
                col("system_pressure") * 1.25,
                2
            )
        )

        # =================================================
        # MANUAL REVIEW
        # =================================================
        .withColumn(
            "manual_review_flag",
            when(
                (col("status") == "HOLD") |
                (
                    col("failure_reason")
                    == "WAITING_MANUAL_REVIEW"
                ),
                True
            ).otherwise(False)
        )

        # =================================================
        # CUSTOMS RISK DISTRIBUTION
        # =================================================
        .withColumn(
            "customs_risk_tier",
            when(
                col("risk_score") >= 80,
                "HIGH"
            )
            .when(
                col("risk_score") >= 40,
                "MEDIUM"
            )
            .otherwise("LOW")
        )

        # =================================================
        # CLEARANCE EFFICIENCY SCORE
        # =================================================
        .withColumn(
            "clearance_efficiency_score",
            round(
                col("stage_efficiency_ratio") * 100,
                2
            )
        )

        # =================================================
        # HIGH RISK CARGO DETECTION
        # =================================================
        .withColumn(
            "high_risk_cargo",
            when(
                (
                    col("delay_hours") > 24
                )
                |
                (
                    col("manual_review_flag")
                )
                |
                (
                    col("customs_risk") == "HIGH"
                ),
                True
            ).otherwise(False)
        )

        # =================================================
        # KPI CATEGORY
        # =================================================
        .withColumn(
            "kpi_category",
            lit("CUSTOMS_INTELLIGENCE")
        )

        # =================================================
        # FINAL OUTPUT
        # =================================================
        .select(
            "sequence_id",
            "container_id",
            "zone",
            "event_type",

            "delay_hours",

            "customs_delay_flag",

            "inspection_pressure",

            "manual_review_flag",

            "customs_risk_tier",

            "clearance_efficiency_score",

            "high_risk_cargo",

            "kpi_category"
        )
    )

    query = (

        customs_df.writeStream

        .format("console")

        .outputMode("append")

        .option("truncate", False)

        .option(
            "checkpointLocation",
            "/tmp/checkpoints/customs_kpis"
        )

        .trigger(
            processingTime="10 seconds"
        )

        .start()
    )

    return customs_df