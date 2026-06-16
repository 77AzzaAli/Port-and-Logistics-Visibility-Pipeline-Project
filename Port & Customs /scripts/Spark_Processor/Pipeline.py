# =========================================================
# PORT & CUSTOMS STREAMING PIPELINE (PRODUCTION SAFE FIXED)
# =========================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    when,
    round,
    lit
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    BooleanType
)

# =========================================================
# KPI IMPORTS
# =========================================================
from kpi.flow_kpis import start_flow_kpi_stream
from kpi.bottleneck_kpis import start_bottleneck_kpi_stream
from kpi.customs_kpis import start_customs_kpi_stream
from kpi.congestion_kpis import start_congestion_kpi_stream
from kpi.executive_kpis import start_executive_kpi_stream

from storage.postgres_sink import create_postgres_sink
# =========================================================
# CONFIG
# =========================================================
KAFKA_BROKER = "kafka:9092"
TOPIC = "container_events"

# =========================================================
# SPARK SESSION
# =========================================================
spark = (
    SparkSession.builder
    .appName("PortStreamingConsumer")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print("🚀 PORT STREAMING PIPELINE STARTED")
print("=" * 70)

# =========================================================
# KAFKA SOURCE
# =========================================================
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

print("📡 Kafka Connected")

# =========================================================
# RAW LAYER
# =========================================================
raw_df = kafka_df.selectExpr(
    "CAST(key AS STRING) AS kafka_key",
    "CAST(value AS STRING) AS json_data",
    "timestamp AS kafka_timestamp"
)

# =========================================================
# SCHEMA
# =========================================================
schema = StructType([
    StructField("sequence_id", IntegerType(), False),
    StructField("container_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("event_category", StringType(), False),
    StructField("event_time", StringType(), False),
    StructField("container_type", StringType(), False),
    StructField("shipping_line", StringType(), False),
    StructField("zone", StringType(), False),
    StructField("weight", DoubleType(), False),
    StructField("port_name", StringType(), False),
    StructField("planned_duration_hours", IntegerType(), False),
    StructField("actual_duration_hours", IntegerType(), False),
    StructField("delay_hours", IntegerType(), False),
    StructField("system_pressure", DoubleType(), False),
    StructField("status", StringType(), False),
    StructField("failure_reason", StringType(), False),
    StructField("is_delayed", BooleanType(), False)
])

print("🧾 Schema Loaded")

# =========================================================
# JSON PARSING
# =========================================================
parsed_df = (
    raw_df
    .select(
        from_json(col("json_data"), schema).alias("data")
    )
    .select("data.*")
)

print("✅ JSON Parsing Completed")

# =========================================================
# FEATURE ENGINEERING
# =========================================================
transformed_df = (
    parsed_df

    # TIMESTAMP
    .withColumn("event_time", to_timestamp("event_time"))

    # OPERATION STATE
    .withColumn(
        "operation_state",
        when(col("status") == "SUCCESS", "NORMAL_FLOW")
        .when(col("status") == "DELAYED", "PORT_DELAY")
        .when(col("status") == "FAILED", "OPERATION_FAILED")
        .when(col("status") == "HOLD", "UNDER_REVIEW")
        .otherwise("UNKNOWN")
    )

    # DELAY SEVERITY
    .withColumn(
        "delay_severity",
        when(col("delay_hours") == 0, "ON_TIME")
        .when(col("delay_hours") <= 6, "LOW_DELAY")
        .when(col("delay_hours") <= 24, "MEDIUM_DELAY")
        .otherwise("CRITICAL_DELAY")
    )

    # PRESSURE LEVEL
    .withColumn(
        "pressure_level",
        when(col("system_pressure") < 30, "LOW")
        .when(col("system_pressure") < 70, "MEDIUM")
        .otherwise("HIGH")
    )

    # STAGE EFFICIENCY
    .withColumn(
        "stage_efficiency_ratio",
        round(
            col("planned_duration_hours") / col("actual_duration_hours"),
            2
        )
    )

    # BOTTLENECK FLAG
    .withColumn(
        "is_bottleneck",
        (col("delay_hours") > 24) | (col("status") == "HOLD")
    )

    # RISK SCORE
    .withColumn(
        "risk_score",
        round(
            (col("delay_hours") * 2) +
            (col("system_pressure") * 0.5) +
            when(col("status") == "FAILED", lit(50)).otherwise(lit(0)),
            2
        )
    )

    # CUSTOMS RISK
    .withColumn(
    "customs_risk",
    when(
        col("failure_reason").isin(
            "WAITING_MANUAL_REVIEW",
            "DOCUMENT_VERIFICATION",
            "INSPECTION_BACKLOG",
            "CUSTOMS_REJECTED"
        ),
        "HIGH"
    ).otherwise("NORMAL")
)

    # PROGRESS
    .withColumn(
        "progress_pct",
        when(col("event_type") == "VESSEL_ARRIVED", 10)
        .when(col("event_type") == "CONTAINER_DISCHARGED", 25)
        .when(col("event_type") == "YARD_ENTER", 40)
        .when(col("event_type") == "CUSTOMS_SUBMITTED", 55)
        .when(col("event_type") == "CUSTOMS_INSPECTED", 70)
        .when(col("event_type") == "CUSTOMS_CLEAR", 85)
        .when(col("event_type") == "GATE_OUT", 95)
        .when(col("event_type") == "DELIVERED", 100)
        .otherwise(0)
    )
)

print("⚙️ Feature Engineering Completed")

# =========================================================
# FLOW KPI STREAM
# =========================================================
flow_df = start_flow_kpi_stream(
    transformed_df
)
# print("📊 Flow KPI Stream Started")

# =========================================================
# BOTTLENECK KPI STREAM
# =========================================================
bottleneck_df = start_bottleneck_kpi_stream(
    transformed_df
)
# print("🚨 Bottleneck KPI Stream Started")

# =========================================================
# CUSTOMS KPI STREAM
# =========================================================
customs_df = start_customs_kpi_stream(
    transformed_df
)

# print("🛃 Customs KPI Stream Started")

# =========================================================
# CONGESTION KPI STREAM
# =========================================================

congestion_df = start_congestion_kpi_stream(
    transformed_df
)

# print("🔥 Congestion KPI Stream Started")

# =========================================================
# EXECUTIVE KPI STREAM
# =========================================================

executive_df = start_executive_kpi_stream(
    transformed_df
)

# print("🏢 Executive KPI Stream Started")

# =========================================================
# POSTGRESQL SINKS
# =========================================================

flow_postgres = create_postgres_sink(
    flow_df,
    "flow_kpis",
    "/tmp/checkpoints/postgres_flow"
)

print("🐘 Flow PostgreSQL Sink Started")


bottleneck_postgres = create_postgres_sink(
    bottleneck_df,
    "bottleneck_kpis",
    "/tmp/checkpoints/postgres_bottleneck"
)

print("🐘 Bottleneck PostgreSQL Sink Started")


customs_postgres = create_postgres_sink(
    customs_df,
    "customs_kpis",
    "/tmp/checkpoints/postgres_customs"
)

print("🐘 Customs PostgreSQL Sink Started")


congestion_postgres = create_postgres_sink(
    congestion_df,
    "congestion_kpis",
    "/tmp/checkpoints/postgres_congestion"
)

print("🐘 Congestion PostgreSQL Sink Started")


executive_postgres = create_postgres_sink(
    executive_df,
    "executive_kpis",
    "/tmp/checkpoints/postgres_executive"
)

print("🐘 Executive PostgreSQL Sink Started")


# =========================================================
# MAIN DEBUG STREAM
# =========================================================
feature_query = (
    transformed_df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .option("numRows", 50)
    .option("checkpointLocation", "/tmp/checkpoints/port_main")
    .trigger(processingTime="5 seconds")
    .start()
)

print("🔥 Streaming Started — Waiting for Events...")

# =========================================================
# WAIT FOR TERMINATION
# =========================================================
spark.streams.awaitAnyTermination()