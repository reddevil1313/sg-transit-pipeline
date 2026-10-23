import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

TOPIC = "bus.arrivals.raw"


def to_bronze(kafka_df: DataFrame) -> DataFrame:
    value = F.col("value").cast("string")
    polled_at = F.coalesce(
        F.get_json_object(value, "$.polled_at").cast("timestamp"),
        F.col("timestamp"),
    )
    return kafka_df.select(
        value.alias("value"),
        F.col("key").cast("string").alias("stop_code"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        polled_at.alias("polled_at"),
    ).withColumns({
        "date": F.to_date("polled_at"),
        "hour": F.hour("polled_at"),
    })


def main():
    hdfs = os.environ["HDFS_URL"]
    spark = (
        SparkSession.builder.appName("bronze-ingest")
        .config("spark.sql.session.timeZone", "Asia/Singapore")
        .getOrCreate()
    )

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.environ["KAFKA_BOOTSTRAP_SERVERS"])
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    query = (
        to_bronze(raw)
        .coalesce(1)
        .writeStream.format("parquet")
        .option("path", f"{hdfs}/lake/bronze/arrivals")
        .option("checkpointLocation", f"{hdfs}/checkpoints/bronze")
        .partitionBy("date", "hour")
        .trigger(processingTime="5 minutes")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
