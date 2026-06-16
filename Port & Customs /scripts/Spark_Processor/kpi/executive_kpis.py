# =========================================================
# EXECUTIVE KPI STREAM
# =========================================================

from pyspark.sql import DataFrame

from pyspark.sql.functions import (
    col,
    when,
    lit,
    round,
    greatest,
    least
)

# =========================================================
# EXECUTIVE KPI STREAM
# =========================================================
def start_executive_kpi_stream(
    transformed_df: DataFrame
):

    executive_df = (

        transformed_df

        # =================================================
        # EXECUTIVE RISK INDEX
        # =================================================
        .withColumn(
            "executive_risk_index",
            round(
                (
                    col("delay_hours") * 2
                )
                +
                (
                    col("system_pressure") * 0.5
                )
                +
                (
                    col("risk_score") * 0.3
                ),
                2
            )
        )

        # =================================================
        # PORT HEALTH SCORE
        # =================================================
        .withColumn(
            "port_health_score_raw",
            round(
                100
                -
                (
                    col("delay_hours") * 1.5
                )
                -
                (
                    col("system_pressure") * 0.3
                ),
                2
            )
        )

        .withColumn(
            "port_health_score",
            greatest(
                lit(0),
                least(
                    col("port_health_score_raw"),
                    lit(100)
                )
            )
        )

        # =================================================
        # OPERATIONAL STABILITY INDEX
        # =================================================
        .withColumn(
            "stability_index",
            round(
                (
                    col("stage_efficiency_ratio") * 100
                )
                -
                col("delay_hours"),
                2
            )
        )

        # =================================================
        # EXECUTIVE ALERT LEVEL
        # =================================================
        .withColumn(
            "executive_alert_level",

            when(
                (
                    col("delay_hours") > 36
                )
                |
                (
                    col("system_pressure") > 85
                )
                |
                (
                    col("risk_score") > 100
                ),
                "CRITICAL"
            )

            .when(
                col("delay_hours") > 24,
                "HIGH"
            )

            .when(
                col("delay_hours") > 8,
                "MEDIUM"
            )

            .otherwise(
                "LOW"
            )
        )

        # =================================================
        # STRATEGIC PRIORITY QUEUE
        # =================================================
        .withColumn(
            "priority_queue",

            when(
                col("risk_score") > 100,
                "P1"
            )

            .when(
                col("customs_risk") == "HIGH",
                "P2"
            )

            .when(
                col("system_pressure") > 70,
                "P3"
            )

            .otherwise(
                "P4"
            )
        )

        # =================================================
        # EXECUTIVE SUMMARY
        # =================================================
        .withColumn(
            "executive_summary",

            when(
                col("executive_risk_index") >= 120,
                "CRITICAL_OPERATION"
            )

            .when(
                col("executive_risk_index") >= 80,
                "AT_RISK"
            )

            .when(
                col("executive_risk_index") >= 40,
                "WATCHLIST"
            )

            .otherwise(
                "HEALTHY_OPERATION"
            )
        )

        # =================================================
        # KPI CATEGORY
        # =================================================
        .withColumn(
            "kpi_category",
            lit("EXECUTIVE_PLATFORM")
        )

        # =================================================
        # FINAL OUTPUT
        # =================================================
        .select(
            "sequence_id",
            "container_id",
            "zone",

            "executive_risk_index",

            "port_health_score",

            "stability_index",

            "executive_alert_level",

            "priority_queue",

            "executive_summary",

            "kpi_category"
        )
    )

    query = (

        executive_df.writeStream

        .format("console")

        .outputMode("append")

        .option(
            "truncate",
            False
        )

        .option(
            "checkpointLocation",
            "/tmp/checkpoints/executive_kpis"
        )

        .trigger(
            processingTime="10 seconds"
        )

        .start()
    )

    return executive_df