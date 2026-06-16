# =========================================================
# CONGESTION KPI STREAM
# =========================================================

from pyspark.sql import DataFrame

from pyspark.sql.functions import (
    col,
    when,
    lit,
    round
)

# =========================================================
# CONGESTION KPI STREAM
# =========================================================
def start_congestion_kpi_stream(
    transformed_df: DataFrame
):

    congestion_df = (

        transformed_df

        # =================================================
        # ZONE PRESSURE
        # =================================================
        .withColumn(
            "zone_pressure_score",
            round(
                col("system_pressure") * 1.5,
                2
            )
        )

        # =================================================
        # DELAY CONGESTION
        # =================================================
        .withColumn(
            "delay_congestion_score",
            round(
                col("delay_hours") * 2,
                2
            )
        )

        # =================================================
        # BOTTLENECK IMPACT
        # =================================================
        .withColumn(
            "zone_bottleneck_rate",
            when(
                col("is_bottleneck"),
                100
            ).otherwise(0)
        )

        # =================================================
        # CONGESTION TREND
        # =================================================
        .withColumn(
            "congestion_trend",
            when(
                col("system_pressure") < 30,
                "LOW"
            )
            .when(
                col("system_pressure") < 60,
                "STABLE"
            )
            .when(
                col("system_pressure") < 85,
                "RISING"
            )
            .otherwise(
                "SEVERE"
            )
        )

        # =================================================
        # OPERATIONAL HEAT SCORE
        # =================================================
        .withColumn(
            "operational_heat_score",
            round(
                (col("delay_hours") * 2)
                +
                (col("system_pressure") * 1.5)
                +
                when(
                    col("is_bottleneck"),
                    lit(50)
                ).otherwise(lit(0)),
                2
            )
        )

        # =================================================
        # KPI CATEGORY
        # =================================================
        .withColumn(
            "kpi_category",
            lit(
                "CONGESTION_INTELLIGENCE"
            )
        )

        # =================================================
        # OUTPUT
        # =================================================
        .select(
            "sequence_id",
            "container_id",
            "zone",

            "delay_hours",

            "system_pressure",

            "zone_pressure_score",

            "delay_congestion_score",

            "zone_bottleneck_rate",

            "congestion_trend",

            "operational_heat_score",

            "kpi_category"
        )
    )

    query = (

        congestion_df.writeStream

        .format("console")

        .outputMode("append")

        .option(
            "truncate",
            False
        )

        .option(
            "checkpointLocation",
            "/tmp/checkpoints/congestion_kpis"
        )

        .trigger(
            processingTime="10 seconds"
        )

        .start()
    )

    return congestion_df