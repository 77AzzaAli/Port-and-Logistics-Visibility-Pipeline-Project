# =========================================================
# POSTGRES SINK
# =========================================================

from pyspark.sql import DataFrame

# =========================================================
# POSTGRES CONFIG
# =========================================================

POSTGRES_URL = (
    "jdbc:postgresql://airflow-postgres:5432/port_intelligence"
)

POSTGRES_PROPERTIES = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

def write_to_postgres(df, epoch_id, table_name):

    unwanted_columns = [
        "kpi_category",
        "window_start",
        "window_end"
    ]

    existing = [
        c for c in df.columns
        if c not in unwanted_columns
    ]

    (
        df.select(*existing)
          .write
          .mode("append")
          .jdbc(
              url=POSTGRES_URL,
              table=table_name,
              properties=POSTGRES_PROPERTIES
          )
    )

# =========================================================
# CREATE STREAM WRITER
# =========================================================
def create_postgres_sink(
    dataframe: DataFrame,
    table_name: str,
    checkpoint_path: str
):

    query = (

        dataframe.writeStream

        .foreachBatch(
            lambda df, epoch_id:
            write_to_postgres(
                df,
                epoch_id,
                table_name
            )
        )

        .outputMode("append")

        .option(
            "checkpointLocation",
            checkpoint_path
        )

        .start()
    )

    return query